import Foundation
import CloudKit
import os.log
import PersistedPropertyWrapper
import ReadingList_Foundation
import CoreData
import CocoaLumberjackSwift
import Reachability

@available(iOS 13.0, *)
final class SyncCoordinator { //swiftlint:disable:this type_body_length

    private let persistentStoreCoordinator: NSPersistentStoreCoordinator
    private let orderedTypesToSync: [CKRecordRepresentable.Type]

    init(persistentStoreCoordinator: NSPersistentStoreCoordinator, orderedTypesToSync: [CKRecordRepresentable.Type]) {
        self.persistentStoreCoordinator = persistentStoreCoordinator
        self.orderedTypesToSync = orderedTypesToSync
    }

    /// Local Core Data transactions which have not yet been confirmed to have been pushed  to CloudKit. The push may have been initiated, but
    /// no response yet received.
    private var localTransactionsBuffer = [NSPersistentHistoryTransaction]()

    private let cloudOperationQueue = ConcurrentCKQueue()
    private lazy var cloudKitInitialiser = CloudKitInitialiser(cloudOperationQueue: cloudOperationQueue)

    private let reachability: Reachability? = {
        do {
            return try Reachability()
        } catch {
            DDLogError("Reachability could not be initialized: \(error.localizedDescription)")
            return nil
        }
    }()

    private lazy var syncContext: NSManagedObjectContext = {
        let context = NSManagedObjectContext(concurrencyType: .privateQueueConcurrencyType)
        context.persistentStoreCoordinator = persistentStoreCoordinator
        context.name = "SyncEngineContext"
        try! context.setQueryGenerationFrom(.current)
        // Ensure that other changes made to the store trump the changes made in this context, so that UI changes don't get overwritten
        // by sync chnages.
        context.mergePolicy = NSMergePolicy.mergeByPropertyObjectTrump
        return context
    }()

    #if DEBUG
    private var debugSimulatorSyncPollTimer: Timer?
    #endif
    
    private var persistentStoreRemoteChangeObserver: Any?

    func start() {
        DDLogInfo("SyncCoordinator starting")
        self.syncContext.perform {
            self.cloudKitInitialiser.prepareCloudEnvironment { [weak self] in
                guard let self = self else { return }
                DDLogInfo("Cloud environment prepared")
                self.syncContext.perform {

                    // Initialise our in-memory transaction retrieval timestamp from the persisted cloudkit commited timestamp
                    self.localTransactionBufferTimestamp = self.lastLocaTransactionCommittedToCloudKitTimestamp

                    // Read the local transactions, and observe future changes so we continue to do this ongoing
                    self.readLocalTransactionsToBuffer()
                    self.persistentStoreRemoteChangeObserver = NotificationCenter.default.addObserver(
                        forName: .NSPersistentStoreRemoteChange,
                        object: self.persistentStoreCoordinator,
                        queue: nil,
                        using: self.handleLocalChange(notification:)
                    )

                    // Monitoring the network reachabiity will allow us to automatically re-do work when network connectivity resumes
                    self.monitorNetworkReachability()

                    // Do some syncing!
                    self.uploadLocalChanges()

                    #if DEBUG && targetEnvironment(simulator)
                    self.debugSimulatorSyncPollTimer = Timer.scheduledTimer(timeInterval: 5, target: self, selector: #selector(self.respondToRemoteChangeNotification), userInfo: nil, repeats: true)
                    #endif
                }
            }
        }
    }

    public func stop() {
        #warning("stop() not implemented yet")
    }

    @objc public func respondToRemoteChangeNotification() {
        syncContext.perform {
            self.fetchRemoteChanges()
        }
    }

    // MARK: - Network Monitoring
    private func monitorNetworkReachability() {
        guard let reachability = reachability else { return }
        do {
            try reachability.startNotifier()
            NotificationCenter.default.addObserver(self, selector: #selector(networkConnectivityDidChange), name: .reachabilityChanged, object: nil)
        } catch {
            DDLogError("Error starting reachability notifier: \(error.localizedDescription)")
        }
    }

    @objc private func networkConnectivityDidChange() {
        syncContext.perform {
            guard let reachability = self.reachability else { preconditionFailure("Reachability was nil in a networkChange callback") }
            DDLogDebug("Network connectivity changed to \(reachability.connection.description)")
            if reachability.connection == .unavailable {
                self.cloudOperationQueue.suspend()
            } else {
                self.cloudOperationQueue.resume()
                self.fetchRemoteChanges()
            }
        }
    }

    // MARK: - Upload

    @Persisted("SyncEngine_LocalChangeTimestamp")
    private var lastLocaTransactionCommittedToCloudKitTimestamp: Date?

    private var localTransactionBufferTimestamp: Date?

    private lazy var historyFetcher = PersistentHistoryFetcher(context: syncContext, excludeHistoryFromContextWithName: syncContext.name!)

    private func handleLocalChange(notification: Notification) {
        self.syncContext.perform {
            DDLogInfo("Handling local change")
            self.syncContext.mergeChanges(fromContextDidSave: notification)
            self.readLocalTransactionsToBuffer()
            self.uploadLocalChanges()
        }
    }

    private func readLocalTransactionsToBuffer() {
        guard let fetchFromWhen = localTransactionBufferTimestamp else {
            DDLogInfo("No local transaction timestamp stored; cannot extract changes yet")
            return
        }

        let transactions = historyFetcher.fetch(fromDate: fetchFromWhen)
        guard let lastTransactionTimestamp = transactions.last?.timestamp else { return }
        self.localTransactionBufferTimestamp = lastTransactionTimestamp
        self.localTransactionsBuffer.append(contentsOf: transactions)
        DDLogInfo("Appended \(transactions.count) transaction(s) from \(fetchFromWhen) until \(lastTransactionTimestamp) to upload buffer")
    }

    private func getAllObjectCkRecords() -> [CKRecord] {
        var ckRecords: [CKRecord] = []
        for entity in orderedTypesToSync.map({ $0.entity() }) {
            let request: NSFetchRequest<NSFetchRequestResult> = NSFetchRequest()
            request.entity = entity
            request.returnsObjectsAsFaults = false
            request.includesPropertyValues = true
            request.fetchBatchSize = 100
            let objects = try! syncContext.fetch(request) as! [CKRecordRepresentable]
            ckRecords.append(contentsOf: objects.map { $0.buildCKRecord() })
        }
        
        // TODO What was this for? Anything?
        //syncContext.saveIfChanged()
        return ckRecords
    }

    private func uploadLocalChanges() {
        if lastLocaTransactionCommittedToCloudKitTimestamp == nil {
            DDLogInfo("No local transactions previously commit to CloudKit, uploading all objects")
            let now = Date()
            localTransactionBufferTimestamp = now
            let allRecords = getAllObjectCkRecords()
            uploadChanges(records: allRecords, deletions: []) {
                self.lastLocaTransactionCommittedToCloudKitTimestamp = now
            }
            return
        }

        // TODO REMOVE FIRST?
        guard let transactionToUpload = localTransactionsBuffer.first else {
            DDLogInfo("No local transactions to upload")
            return
        }

        func onTransactionUploadCompletion() {
            let removedFirstTransaction = self.localTransactionsBuffer.removeFirst()
            if transactionToUpload != removedFirstTransaction {
                DDLogError("Concurrency error; first transaction in buffer is not the same as the processed transaction")
                fatalError("Concurrency error; first transaction in buffer is not the same as the processed transaction")
            }

            self.lastLocaTransactionCommittedToCloudKitTimestamp = removedFirstTransaction.timestamp
            DDLogInfo("Updated last-pushed local timestamp to \(removedFirstTransaction.timestamp)")

            self.uploadLocalChanges()
        }

        guard let changes = transactionToUpload.changes else {
            onTransactionUploadCompletion()
            return
        }

        DDLogDebug("Processing local transaction consisting of changes:\n" + changes.map {
            var base = "\($0.changeType.description) \($0.changedObjectID.uriRepresentation().path)"
            if $0.changeType == .update {
                base += " [\($0.updatedProperties?.map(\.name).joined(separator: ", ") ?? "")]"
            }
            return base
        }.joined(separator: "\n"))

        // We want to extract the objects corresponding to the changes to that we can determine the entity types,
        // and then order them according to the orderedTypesToSync property (this will help keep CKReferences intact),
        // before generating our CKRecords.
        let changesAndObjects = changes.filter { $0.changeType != .delete }
            .compactMap { change -> (change: NSPersistentHistoryChange, managedObject: CKRecordRepresentable)? in
                guard let managedObject = self.syncContext.object(with: change.changedObjectID) as? CKRecordRepresentable else { return nil }
                return (change, managedObject)
            }
        let changesByEntityType = Dictionary(grouping: changesAndObjects, by: { $0.managedObject.entity })

        let ckRecords = self.orderedTypesToSync.compactMap { changesByEntityType[$0.entity()] }
            .flatMap { $0 }
            .compactMap { change, managedObject -> CKRecord? in
                let ckKeysToUpload: [String]?
                if change.changeType == .update {
                    guard let coreDataKeys = change.updatedProperties?.map(\.name) else { return nil }
                    let ckRecordKeys = coreDataKeys.compactMap { managedObject.ckRecordKey(forLocalPropertyKey: $0) }
                    if ckRecordKeys.isEmpty { return nil }
                    ckKeysToUpload = ckRecordKeys
                } else {
                    ckKeysToUpload = nil
                }

                return managedObject.buildCKRecord(ckRecordKeys: ckKeysToUpload)
            }

        let deletionIDs = changes.filter { $0.changeType == .delete }
            .compactMap { (change: NSPersistentHistoryChange) -> CKRecord.ID? in
                guard let remoteIdentifier = change.tombstone?[SyncConstants.remoteIdentifierKeyPath] as? String else { return nil }
                return CKRecord.ID(recordName: remoteIdentifier, zoneID: SyncConstants.zoneID)
            }

        // Buiding the CKRecord can in some cases cause updates to the managed object; save if this is the case
        self.syncContext.saveIfChanged()
        self.uploadChanges(records: ckRecords, deletions: deletionIDs, completion: onTransactionUploadCompletion)
    }

    private func uploadChanges(records: [CKRecord], deletions: [CKRecord.ID], priority: Operation.QueuePriority = .normal, completion: @escaping () -> Void) {
        if records.isEmpty && deletions.isEmpty {
            completion()
            return
        }

        DDLogInfo("Uploading \(records.count) record(s) and \(deletions.count) deletion(s):\n"
            + records.map { "Upload \($0.recordType.description) \($0.recordID.recordName) [\($0.changedKeys().joined(separator: ", "))]" }.joined(separator: "\n") + "\n"
            + deletions.map { "Delete \($0.recordName)" }.joined(separator: "\n")
        )

        let operation = CKModifyRecordsOperation(recordsToSave: records, recordIDsToDelete: deletions)
        operation.perRecordCompletionBlock = { [weak self] record, error in
            guard let self = self else { return }
            self.syncContext.perform {
                if error != nil {
                    DDLogVerbose("CKRecord \(record.recordID.recordName) upload errored")
                } else {
                    DDLogVerbose("CKRecord \(record.recordID.recordName) upload completed")
                }
            }
        }
        operation.modifyRecordsCompletionBlock = { [weak self] serverRecords, _, error in
            guard let self = self else { return }
            self.syncContext.perform {
                if let error = error {
                    DDLogError("Error during record upload")
                    self.handleUploadError(error, records: records, ids: deletions, completion: completion)
                } else {
                    DDLogInfo("Completed upload")
                    guard let serverRecords = serverRecords else {
                        DDLogError("Unexpected nil `serverRecords` in response from CKModifyRecordsOperation operation")
                        self.handleUnexpectedResponse()
                        return
                    }
                    self.updateLocalModelsAfterUpload(with: serverRecords)
                    completion()
                }
            }
        }

        operation.savePolicy = .ifServerRecordUnchanged
        operation.queuePriority = priority
        cloudOperationQueue.addOperation(operation)
    }

    func handleUnexpectedResponse() {
        DDLogError("Unimplemented function handleUnexpectedResponse")
        fatalError("Should stop syncing, at least for a bit")
    }

    private func handleUploadError(_ error: Error, records: [CKRecord], ids: [CKRecord.ID], completion: @escaping () -> Void) {
        guard let ckError = error as? CKError else {
            handleUnexpectedResponse()
            return
        }

        DDLogInfo("Upload error occurred with CKError code \(ckError.code.name)")
        if ckError.code == .limitExceeded {
            DDLogError("CloudKit batch limit exceeded, sending records in chunks")
            fatalError("Not implemented: batch uploads. Here we should divide the records in chunks and upload in batches instead of trying everything at once.")
        } else if ckError.code == .partialFailure {
            guard let errorsByItemId = ckError.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: Error] else {
                DDLogError("Missing CKPartialErrorsByItemIDKey data")
                self.handleUnexpectedResponse()
                return
            }

            var refetchIDs = [CKRecord.ID]()
            for record in records {
                guard let uploadError = errorsByItemId[record.recordID] as? CKError else {
                    DDLogError("Missing CKError for record \(record.recordID.recordName)")
                    handleUnexpectedResponse()
                    return
                }
                if uploadError.code == .serverRecordChanged {
                    DDLogInfo("CKRecord \(record.recordID.recordName) upload failed as server record has changed")
                    refetchIDs.append(record.recordID)
                } else if uploadError.code == .batchRequestFailed {
                    DDLogVerbose("CKRecord \(record.recordID.recordName) part of failed upload batch")
                    continue
                } else {
                    DDLogError("Unhandled error \(uploadError.code.name) for CKRecord \(record.recordID.recordName)")
                }
            }

            fetchRecords(refetchIDs) {
                self.uploadLocalChanges()
            }
        } else {
            if cloudOperationQueue.suspendCloudInterop(dueTo: error) {
                self.uploadChanges(records: records, deletions: ids, completion: completion)
            } else {
                DDLogError("Error is not recoverable: \(String(describing: error))")
                handleUnexpectedResponse()
            }
        }
    }

    private func updateLocalModelsAfterUpload(with records: [CKRecord]) {
        guard !records.isEmpty else { return }
        for record in records {
            saveRecordDataLocally(record, option: .storeSystemFieldsOnly)
        }
        syncContext.saveAndLogIfErrored()
        DDLogInfo("Completed updating \(records.count) local model(s) after upload")
    }

    // MARK: - Remote change tracking

    @Persisted(archivedDataKey: "SyncEngine_SeverChangeToken")
    private var remoteChangeToken: CKServerChangeToken?

    private func fetchRemoteChanges() {
        let operation = CKFetchRecordZoneChangesOperation()
        let config = CKFetchRecordZoneChangesOperation.ZoneConfiguration(
            previousServerChangeToken: remoteChangeToken,
            resultsLimit: nil,
            desiredKeys: nil
        )
        operation.configurationsByRecordZoneID = [SyncConstants.zoneID: config]
        operation.recordZoneIDs = [SyncConstants.zoneID]
        operation.fetchAllChanges = true

        operation.recordZoneChangeTokensUpdatedBlock = { [weak self] _, changeToken, _ in
            guard let self = self else { return }
            self.syncContext.perform {
                DDLogInfo("Server change token updated")
                self.syncContext.saveAndLogIfErrored()
                self.remoteChangeToken = changeToken
            }
        }

        operation.recordZoneFetchCompletionBlock = { [weak self] _, token, _, _, error in
            guard let self = self else { return }
            self.syncContext.perform {
                if let error = error as? CKError {
                    DDLogError("Failed to fetch record zone changes: \(String(describing: error))")

                    if error.code == .changeTokenExpired {
                        DDLogError("Change token expired, resetting token and trying again")
                        self.remoteChangeToken = nil
                        self.fetchRemoteChanges()
                    } else {
                        if self.cloudOperationQueue.suspendCloudInterop(dueTo: error) {
                            self.fetchRemoteChanges()
                        }
                    }
                } else {
                    DDLogDebug("Remote record fetch completed; commiting new change token")
                    self.remoteChangeToken = token
                    self.syncContext.saveIfChanged()
                }
            }
        }

        operation.recordChangedBlock = { [weak self] record -> Void in
            guard let self = self else { return }
            self.syncContext.perform {
                DDLogVerbose("Handing remote record change")
                self.saveRecordDataLocally(record, option: .createIfNotFound)
            }
        }

        operation.recordWithIDWasDeletedBlock = { [weak self] recordID, recordType in
            guard let self = self else { return }
            self.syncContext.perform {
                let localObject = self.localEntity(forIdentifier: CKRecordIdentity(ID: recordID, type: recordType))
                localObject?.delete()
            }
        }

        operation.fetchRecordZoneChangesCompletionBlock = { [weak self] error in
            guard let self = self else { return }
            DDLogInfo("Remote change fetch completed")
            self.syncContext.perform {
                if let error = error {
                    DDLogError("Failed to fetch record zone changes: \(String(describing: error))")

                    if self.cloudOperationQueue.suspendCloudInterop(dueTo: error) {
                        self.fetchRemoteChanges()
                    }
                } else {
                    self.syncContext.saveIfChanged()
                }
            }
        }

        operation.queuePriority = .high
        if remoteChangeToken != nil {
            DDLogInfo("Enqueuing operation to fetch remote changes using CKServerChangeToken")
        } else {
            DDLogInfo("Enqueuing operation to fetch all remote changes")
        }
        cloudOperationQueue.addOperation(operation)
    }

    private func fetchRecords(_ recordIDs: [CKRecord.ID], completion: @escaping () -> Void) {
        let operation = CKFetchRecordsOperation(recordIDs: recordIDs)
        operation.fetchRecordsCompletionBlock = { [weak self] records, error in
            guard let self = self else { return }
            self.syncContext.perform {
                if let error = error {
                    DDLogError("Failed to fetch records: \(String(describing: error))")
                    if self.cloudOperationQueue.suspendCloudInterop(dueTo: error) {
                        self.fetchRecords(recordIDs, completion: completion)
                    } else {
                        DDLogError("WHAT TO DO HERE?")
                    }
                    return
                }

                guard let records = records else {
                    self.handleUnexpectedResponse()
                    return
                }
                self.commitServerChangesToDatabase(with: Array(records.values), deletedRecordIDs: [])
                completion()
            }
        }

        operation.queuePriority = .high
        DDLogInfo("Fetching remote records with IDs \(recordIDs.map { $0.recordName }.joined(separator: ", "))")
        cloudOperationQueue.addOperation(operation)
    }

    private func commitServerChangesToDatabase(with changedRecords: [CKRecord], deletedRecordIDs: [CKRecordIdentity]) {
        guard !changedRecords.isEmpty || !deletedRecordIDs.isEmpty else {
            DDLogInfo("Finished record zone changes fetch with no changes")
            return
        }

        DDLogInfo("Will commit \(changedRecords.count) changed record(s) and \(deletedRecordIDs.count) deleted record(s) to the database")
        for record in changedRecords {
            self.saveRecordDataLocally(record, option: .createIfNotFound)
        }
        for deletedID in deletedRecordIDs {
            self.localEntity(forIdentifier: deletedID)?.delete()
        }
        self.syncContext.saveAndLogIfErrored()
        DDLogInfo("Completed updating local model(s) after download")
    }

    private func localEntity(forIdentifier remoteIdentifier: CKRecordIdentity) -> NSManagedObject? {
        guard let entityType = orderedTypesToSync.first(where: { remoteIdentifier.type == $0.ckRecordType }) else {
            DDLogError("Unexpected record type supplied: \(remoteIdentifier.type)")
            return nil
        }
        return lookupLocalObject(ofType: entityType, withIdentifier: remoteIdentifier.ID.recordName)
    }

    enum DownloadOption {
        case storeSystemFieldsOnly
        case createIfNotFound
    }

    private func saveRecordDataLocally(_ ckRecord: CKRecord, option: DownloadOption?) {
        if let localObject = lookupLocalObject(for: ckRecord) {
            if localObject.isDeleted {
                DDLogInfo("Local \(ckRecord.recordType) was deleted; skipping local update")
                return
            }

            DDLogInfo("Updating \(ckRecord.recordType) from CKRecord \(ckRecord.recordID.recordName)")
            if option == .storeSystemFieldsOnly {
                localObject.setSystemAndIdentifierFields(from: ckRecord)
                DDLogInfo("Updated system fields for CKRecord \(ckRecord.recordID.recordName) on object \(localObject.objectID.uriRepresentation().path)")
            } else {
                let keysPendingUpdate = localTransactionsBuffer.compactMap { $0.changes }
                    .flatMap { $0 }
                    .filter { $0.changeType == .update && $0.changedObjectID == localObject.objectID }
                    .compactMap { $0.updatedProperties }
                    .flatMap { $0 }
                    .map { $0.name }
                    .distinct()

                localObject.update(from: ckRecord, excluding: keysPendingUpdate)
                DDLogInfo("Updated metadata for CKRecord \(ckRecord.recordID.recordName) on object \(localObject.objectID.uriRepresentation().path)")
            }
        } else if option == .createIfNotFound {
            DDLogInfo("Creating new \(ckRecord.recordType) with record name \(ckRecord.recordID.recordName)")

            guard let type = orderedTypesToSync.first(where: { $0.ckRecordType == ckRecord.recordType }) else {
                DDLogError("No type corresponding to \(ckRecord.recordType) found")
                return
            }
            let newObject = type.create(from: ckRecord, in: syncContext)
        }
    }

    func lookupLocalObject(for remoteRecord: CKRecord) -> CKRecordRepresentable? {
        guard let type = orderedTypesToSync.first(where: { $0.ckRecordType == remoteRecord.recordType }) else { return nil }

        let recordName = remoteRecord.recordID.recordName
        if let localItem = lookupLocalObject(ofType: type, withIdentifier: recordName) {
            DDLogDebug("Found local \(type.ckRecordType) with remote identifier \(recordName)")
            return localItem
        }

        let localIdLookup = type.fetchRequest()
        localIdLookup.fetchLimit = 1
        localIdLookup.predicate = NSCompoundPredicate(
            andPredicateWithSubpredicates: [
                NSPredicate(format: "%K == NULL", SyncConstants.remoteIdentifierKeyPath),
                type.matchCandidateItemForRemoteRecord(remoteRecord)
            ]
        )

        if let localItem = (try! syncContext.fetch(localIdLookup)).first as? CKRecordRepresentable {
            DDLogDebug("Found candidate local \(type.ckRecordType) for remote record \(recordName) using metadata")
            return localItem
        }

        DDLogDebug("No local \(type.ckRecordType) found for remote record \(recordName)")
        return nil
    }

    func lookupLocalObject(ofType type: CKRecordRepresentable.Type, withIdentifier recordName: String) -> CKRecordRepresentable? {
        let fetchRequest = type.fetchRequest()
        fetchRequest.predicate = type.remoteIdentifierPredicate(recordName)
        fetchRequest.fetchLimit = 1
        return (try! syncContext.fetch(fetchRequest)).first as? CKRecordRepresentable
    }
}

extension NSPersistentHistoryChangeType {
    var description: String {
        switch self {
        case .insert: return "Insert"
        case .update: return "Update"
        case .delete: return "Delete"
        @unknown default: return "Unknown"
        }
    }
}
