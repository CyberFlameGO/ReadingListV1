import Foundation
import CloudKit
import PersistedPropertyWrapper
import ReadingList_Foundation
import CoreData
import Logging
import Combine
import Reachability

@available(iOS 13.0, *)
final class SyncCoordinator { //swiftlint:disable:this type_body_length

    private let persistentStoreCoordinator: NSPersistentStoreCoordinator
    private let orderedTypesToSync: [CKRecordRepresentable.Type]

    init(persistentStoreCoordinator: NSPersistentStoreCoordinator, orderedTypesToSync: [CKRecordRepresentable.Type]) {
        self.persistentStoreCoordinator = persistentStoreCoordinator
        self.orderedTypesToSync = orderedTypesToSync
        self.bufferBookmark = latestConfirmedUploadedTransactionTimestamp
    }

    private var localTransactionsPendingPushCompletion = [NSPersistentHistoryTransaction]()

    private let cloudOperationQueue = ConcurrentCKQueue()
    private lazy var cloudKitInitialiser = CloudKitInitialiser(cloudOperationQueue: cloudOperationQueue)

    private lazy var reachability: Reachability? = {
        do {
            return try Reachability()
        } catch {
            logger.error("Reachability could not be initialized: \(error.localizedDescription)")
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
    private var cancellables = Set<AnyCancellable>()

    func start() {
        logger.info("SyncCoordinator starting")
        self.syncContext.perform {
            self.cloudKitInitialiser.prepareCloudEnvironment { [weak self] in
                guard let self = self else { return }
                logger.info("Cloud environment prepared")
                self.syncContext.perform {

                    // Read the local transactions, and observe future changes so we continue to do this ongoing
                    self.enqueueUploadOperations()
                    self.persistentStoreRemoteChangeObserver = NotificationCenter.default.addObserver(
                        forName: .NSPersistentStoreRemoteChange,
                        object: self.persistentStoreCoordinator,
                        queue: nil,
                        using: self.handleLocalChange(notification:)
                    )
                    
                    NotificationCenter.default.publisher(for: .CKAccountChanged).sink { _ in
                        logger.info("CKAccountChanged; stopping SyncCoordinator")
                        self.stop()
                    }.store(in: &self.cancellables)

                    // Monitoring the network reachabiity will allow us to automatically re-do work when network connectivity resumes
                    self.monitorNetworkReachability()

                    #if DEBUG && targetEnvironment(simulator)
                    self.debugSimulatorSyncPollTimer = Timer.scheduledTimer(timeInterval: 5, target: self, selector: #selector(self.respondToRemoteChangeNotification), userInfo: nil, repeats: true)
                    #endif
                }
            }
        }
    }

    func stop() {
        #warning("stop() not implemented yet")
    }

    func forceFullResync() {
        cloudOperationQueue.cancelAll()
        cloudOperationQueue.addOperation(BlockOperation {
            self.syncContext.perform {
                let syncHelper = ManagedObjectContextSyncHelper(managedObjectContext: self.syncContext, entityTypes: self.orderedTypesToSync.map { $0.entity() })
                syncHelper.eraseSyncMetadata()

                self.remoteChangeToken = nil
                self.enqueueFetchRemoteChanges()
                self.enqueueUploadOfAllObjects()
            }
        })
    }

    @objc public func respondToRemoteChangeNotification() {
        syncContext.perform {
            self.enqueueFetchRemoteChanges()
        }
    }

    // MARK: - Network Monitoring
    private func monitorNetworkReachability() {
        guard let reachability = reachability else { return }
        do {
            try reachability.startNotifier()
            NotificationCenter.default.addObserver(self, selector: #selector(networkConnectivityDidChange), name: .reachabilityChanged, object: nil)
        } catch {
            logger.error("Error starting reachability notifier: \(error.localizedDescription)")
        }
    }

    @objc private func networkConnectivityDidChange() {
        syncContext.perform {
            guard let reachability = self.reachability else { preconditionFailure("Reachability was nil in a networkChange callback") }
            logger.debug("Network connectivity changed to \(reachability.connection.description)")
            if reachability.connection == .unavailable {
                self.cloudOperationQueue.suspend()
            } else {
                self.cloudOperationQueue.resume()
                self.enqueueFetchRemoteChanges()
            }
        }
    }

    // MARK: - Upload

    @Persisted("SyncEngine_LocalChangeTimestamp")
    private var latestConfirmedUploadedTransactionTimestamp: Date?

    private var bufferBookmark: Date?

    private lazy var historyFetcher = PersistentHistoryFetcher(context: syncContext, excludeHistoryFromContextWithName: syncContext.name!)

    private func handleLocalChange(notification: Notification) {
        logger.debug("Detected local change")
        self.syncContext.perform {
            logger.debug("Merging changes into syncContext")
            self.syncContext.mergeChanges(fromContextDidSave: notification)
            if let bufferBookmark = self.bufferBookmark {
                let transactions = self.historyFetcher.fetch(fromDate: bufferBookmark)
                self.localTransactionsPendingPushCompletion.append(contentsOf: transactions)
                if let lastTransaction = transactions.last {
                    self.bufferBookmark = lastTransaction.timestamp
                    self.enqueueUploadOperations()
                }
            }
            // DO AT STARTUP ONLY?
//            else {
//                self.enqueueUploadOfAllObjects()
//            }
        }
    }

    private func enqueueUploadOperations() {
        let uploadOperation = uploadRecordsOperation(completion: { _ in })

        let updateTimestampOperation = BlockOperation {
            let transaction = self.localTransactionsPendingPushCompletion.removeFirst()
            logger.info("Updating confirmed pushed timestamp to \(transaction.timestamp)")
            self.latestConfirmedUploadedTransactionTimestamp = transaction.timestamp
        }
        updateTimestampOperation.addDependency(uploadOperation)

        let buildCKRecordsOperation = BlockOperation {
            logger.info("Building CKRecords for upload")
            self.syncContext.performAndWait {
                guard let transaction = self.localTransactionsPendingPushCompletion.first else {
                    logger.info("No transactions are pending upload; cancelling upload operation.")
                    uploadOperation.cancel()
                    updateTimestampOperation.cancel()
                    return
                }
                if let changes = transaction.changes {
                    self.buildCkRecords(for: changes, uploadOperation: uploadOperation)
                } else {
                    logger.info("Transaction had no changes, cancelling upload and moving timestamp to \(transaction.timestamp)")
                    self.localTransactionsPendingPushCompletion.removeFirst()
                    uploadOperation.cancel()
                    updateTimestampOperation.cancel()
                    self.latestConfirmedUploadedTransactionTimestamp = transaction.timestamp
                    self.enqueueUploadOperations()
                }
            }
        }
        uploadOperation.addDependency(buildCKRecordsOperation)

        cloudOperationQueue.addOperations([buildCKRecordsOperation, uploadOperation, updateTimestampOperation])
        logger.info("Upload operations added to operation queue")
    }

    private func enqueueUploadOfAllObjects() {
        let uploadOperation = uploadRecordsOperation(completion: { _ in })
        var timestamp: Date?
        let fetchRecordsOperation = BlockOperation {
            logger.info("Fetching all records to upload")
            timestamp = Date()
            let allRecords = self.getAllObjectCkRecords()
            uploadOperation.recordsToSave = allRecords
        }
        uploadOperation.addDependency(fetchRecordsOperation)

        let updateBookmarkOperation = BlockOperation {
            guard let timestamp = timestamp else { fatalError("Unexpected nil timestamp") }
            logger.info("Updating upload bookmark to \(timestamp)")
            self.latestConfirmedUploadedTransactionTimestamp = timestamp
        }
        updateBookmarkOperation.addDependency(uploadOperation)

        cloudOperationQueue.addOperations([fetchRecordsOperation, uploadOperation, updateBookmarkOperation])
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

        // building CKRecord sometimes creates remote name; save this
        syncContext.saveIfChanged()
        return ckRecords
    }

//    private func enqueueUploadOperation(for transaction: NSPersistentHistoryTransaction) {
//        guard let changes = transaction.changes else {
//            logger.info("No changes present in transaction")
//            // Update the timestamp as an operation to ensure it is processed in order
//            cloudOperationQueue.addOperation(BlockOperation {
//                self.latestConfirmedUploadedTransactionTimestamp = transaction.timestamp
//                logger.info("Updated last-pushed local timestamp to \(transaction.timestamp)")
//            })
//            return
//        }
//
//        let uploadOperation = uploadRecordsOperation { success in
//            if success {
//                self.latestConfirmedUploadedTransactionTimestamp = transaction.timestamp
//                //self.localTransactionsPendingPushCompletion.removeValue(forKey: transaction.token)
//                logger.info("Updated last-pushed local timestamp to \(transaction.timestamp)")
//            }
//        }
//
//        let ckRecordGenerationOperation = BlockOperation {
//            self.buildCkRecords(for: changes, uploadOperation: uploadOperation)
//        }
//        uploadOperation.addDependency(ckRecordGenerationOperation)
//
//        cloudOperationQueue.addOperations([ckRecordGenerationOperation, uploadOperation])
//
//        self.enqueueUploadOperation(records: ckRecords, deletions: deletionIDs) { success in
//            if success {
//                self.latestConfirmedUploadedTransactionTimestamp = transaction.timestamp
//                self.localTransactionsPendingPushCompletion.removeValue(forKey: transaction.token)
//                logger.info("Updated last-pushed local timestamp to \(transaction.timestamp)")
//            }
//        }
//    }

    private func buildCkRecords(for changes: [NSPersistentHistoryChange], uploadOperation: CKModifyRecordsOperation) {
        func changeDescription() -> String {
            return changes.map {
                var base = "\($0.changeType.description) \($0.changedObjectID.uriRepresentation().path)"
                if $0.changeType == .update {
                    base += " [\($0.updatedProperties?.map(\.name).joined(separator: ", ") ?? "")]"
                }
                return base
            }.joined(separator: "\n")
        }
        logger.debug("Building CKRecords for local transaction consisting of changes:\n\(changeDescription())")

        self.syncContext.performAndWait {
            // We want to extract the objects corresponding to the changes to that we can determine the entity types,
            // and then order them according to the orderedTypesToSync property (this will help keep CKReferences intact),
            // before generating our CKRecords.
            let changesAndObjects = changes.filter { $0.changeType != .delete }
                .compactMap { change -> (change: NSPersistentHistoryChange, managedObject: CKRecordRepresentable)? in
                    guard let managedObject = self.syncContext.object(with: change.changedObjectID) as? CKRecordRepresentable else { return nil }
                    return (change, managedObject)
                }
            let changesByEntityType = Dictionary(grouping: changesAndObjects, by: { $0.managedObject.entity })

            uploadOperation.recordsToSave = self.orderedTypesToSync.compactMap { changesByEntityType[$0.entity()] }
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

            uploadOperation.recordIDsToDelete = changes.filter { $0.changeType == .delete }
                .compactMap { (change: NSPersistentHistoryChange) -> CKRecord.ID? in
                    guard let remoteIdentifier = change.tombstone?[SyncConstants.remoteIdentifierKeyPath] as? String else { return nil }
                    return CKRecord.ID(recordName: remoteIdentifier, zoneID: SyncConstants.zoneID)
                }

            // Buiding the CKRecord can in some cases cause updates to the managed object; save if this is the case
            self.syncContext.saveIfChanged()
        }
    }

    private func uploadRecordsOperation(completion: @escaping (Bool) -> Void) -> CKModifyRecordsOperation {
        let operation = CKModifyRecordsOperation()
        operation.modifyRecordsCompletionBlock = { [weak self] serverRecords, _, error in
            guard let self = self else { return }
            self.syncContext.performAndWait {
                if let error = error {
//                    logger.error("Error during record upload. Cancelling all pending operations.")
//                    self.cloudOperationQueue.cancelAll()
                    self.handleUploadError(error, records: operation.recordsToSave ?? [], ids: operation.recordIDsToDelete ?? [], completion: completion)
                } else {
                    logger.info("Completed upload. Updating local models with server record data.")
                    guard let serverRecords = serverRecords else {
                        logger.error("Unexpected nil `serverRecords` in response from CKModifyRecordsOperation operation")
                        self.handleUnexpectedResponse()
                        return
                    }
                    self.updateLocalModelsAfterUpload(with: serverRecords)
                    completion(true)
                }
            }
        }

        operation.savePolicy = .ifServerRecordUnchanged
        return operation
    }

    // TODO This should get the records as part of a block operation, so we don't generate stale CKRecords.
    private func enqueueUploadOperation(records: [CKRecord], deletions: [CKRecord.ID], priority: Operation.QueuePriority = .normal, completion: @escaping (Bool) -> Void) {
        if records.isEmpty && deletions.isEmpty {
            cloudOperationQueue.addOperation(BlockOperation {
                logger.info("No-op upload operation.")
                completion(true)
            })
            return
        }

        func uploadDescription() -> String {
            return records.map { "Upload \($0.recordType.description) \($0.recordID.recordName) [\($0.changedKeys().joined(separator: ", "))]" }.joined(separator: "\n") + "\n"
                + deletions.map { "Delete \($0.recordName)" }.joined(separator: "\n")
        }
        logger.info("Uploading \(records.count) record(s) and \(deletions.count) deletion(s):\n\(uploadDescription())")

        let operation = uploadRecordsOperation(completion: completion)
        operation.recordsToSave = records
        operation.recordIDsToDelete = deletions
        operation.queuePriority = priority
        cloudOperationQueue.addOperation(operation)
    }

    func handleUnexpectedResponse() {
        logger.error("Unimplemented function handleUnexpectedResponse")
        fatalError("Should stop syncing, at least for a bit")
    }

    private func handleUploadError(_ error: Error, records: [CKRecord], ids: [CKRecord.ID], completion: @escaping (Bool) -> Void) {
        guard let ckError = error as? CKError else {
            handleUnexpectedResponse()
            return
        }

        logger.info("Upload error occurred with CKError code \(ckError.code.name)")
        if ckError.code == .limitExceeded {
            logger.error("CloudKit batch limit exceeded, sending records in chunks")
            fatalError("Not implemented: batch uploads. Here we should divide the records in chunks and upload in batches instead of trying everything at once.")
        } else if ckError.code == .operationCancelled {
            logger.error("The operation was cancelled")
        } else if ckError.code == .partialFailure {
            guard let errorsByItemId = ckError.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: Error] else {
                logger.error("Missing CKPartialErrorsByItemIDKey data")
                self.handleUnexpectedResponse()
                return
            }

            var refetchIDs = [CKRecord.ID]()
            var reuploadObjects = [CKRecordRepresentable]()
            for record in records {
                guard let uploadError = errorsByItemId[record.recordID] as? CKError else {
                    logger.error("Missing CKError for record \(record.recordID.recordName)")
                    handleUnexpectedResponse()
                    return
                }
                if uploadError.code == .serverRecordChanged {
                    logger.info("CKRecord \(record.recordID.recordName) upload failed as server record has changed")
                    refetchIDs.append(record.recordID)
                } else if uploadError.code == .batchRequestFailed {
                    logger.trace("CKRecord \(record.recordID.recordName) part of failed upload batch")
                    continue
                } else if uploadError.code == .unknownItem {
                    logger.error("UnknownItem error returned for upload of CKRecord \(record.recordID.recordName); clearing system fields")
                    guard let localObject = lookupLocalObject(for: record) else {
                        logger.error("Could not find local object for CKRecord.")
                        return
                    }
                    localObject.setSystemFields(nil)
                    reuploadObjects.append(localObject)
                } else {
                    logger.error("Unhandled error \(uploadError.code.name) for CKRecord \(record.recordID.recordName)")
                }
            }

            // TODO
            // We should try to make the sync coordinator act more like a state engine at all times
            // i.e. less passing specific bits here and there and more processing based on observed current state
            // E.g. rather than enqueueing up some objects to upload here, why not just say we need to re-run the whole operation

            syncContext.saveIfChanged()
            fetchRecords(refetchIDs)
            enqueueUploadOperations()
            //enqueueUploadOperation(records: records, deletions: ids, completion: completion)
        } else {
            if cloudOperationQueue.suspendCloudInterop(dueTo: error) {
                self.enqueueUploadOperation(records: records, deletions: ids, completion: completion)
            } else {
                logger.error("Error is not recoverable: \(String(describing: error))")
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
        logger.info("Completed updating \(records.count) local model(s) after upload")
    }

    // MARK: - Remote change tracking

    @Persisted(archivedDataKey: "SyncEngine_SeverChangeToken")
    private var remoteChangeToken: CKServerChangeToken?

    private func enqueueFetchRemoteChanges() {
        let operation = CKFetchRecordZoneChangesOperation()
        let config = CKFetchRecordZoneChangesOperation.ZoneConfiguration(
            previousServerChangeToken: remoteChangeToken,
            resultsLimit: nil,
            desiredKeys: nil
        )
        operation.configurationsByRecordZoneID = [SyncConstants.zoneID: config]
        operation.recordZoneIDs = [SyncConstants.zoneID]
        operation.fetchAllChanges = true

        var recordsByType = [CKRecord.RecordType: [CKRecord]]()
        operation.recordChangedBlock = { record -> Void in
            logger.trace("recordChangedBlock for CKRecord with ID \(record.recordID.recordName)")
            recordsByType.append(record, to: record.recordType)
        }

        var deletionIDs = [CKRecord.RecordType: [CKRecord.ID]]()
        operation.recordWithIDWasDeletedBlock = { recordID, recordType in
            logger.trace("recordWithIDWasDeletedBlock for CKRecord.ID \(recordID.recordName)")
            deletionIDs.append(recordID, to: recordType)
        }

        func importAndSaveChanges() {
            for recordType in self.orderedTypesToSync {
                if let recordsToUpdate = recordsByType[recordType.ckRecordType] {
                    for record in recordsToUpdate {
                        logger.trace("Saving data for CKRecord with ID \(record.recordID.recordName)")
                        self.saveRecordDataLocally(record, option: .createIfNotFound)
                    }
                }
                if let idsToDelete = deletionIDs[recordType.ckRecordType] {
                    for idToDelete in idsToDelete {
                        logger.trace("Deleting object (if exists) CKRecord.ID \(idToDelete.recordName)")
                        let localObject = self.localEntity(forIdentifier: CKRecordIdentity(ID: idToDelete, type: recordType.ckRecordType))
                        localObject?.delete()
                    }
                }
            }

            logger.info("Saving syncContext after record change import")
            self.syncContext.saveIfChanged()

            recordsByType.removeAll()
            deletionIDs.removeAll()
        }

        operation.recordZoneChangeTokensUpdatedBlock = { [weak self] _, changeToken, _ in
            guard let self = self else { return }
            self.syncContext.performAndWait {
                logger.info("Server change token updated")
                importAndSaveChanges()
                self.remoteChangeToken = changeToken
            }
        }

        operation.recordZoneFetchCompletionBlock = { [weak self] _, token, _, _, error in
            guard let self = self else { return }
            self.syncContext.performAndWait {
                if let error = error as? CKError {
                    logger.error("Failed to fetch record zone changes: \(String(describing: error))")

                    if error.code == .changeTokenExpired {
                        logger.warning("Change token expired, resetting token and trying again")
                        self.remoteChangeToken = nil
                        self.enqueueFetchRemoteChanges()
                    } else {
                        if self.cloudOperationQueue.suspendCloudInterop(dueTo: error) {
                            self.enqueueFetchRemoteChanges()
                        }
                    }
                } else {
                    logger.info("Remote record fetch completed; commiting new change token")
                    importAndSaveChanges()
                    self.remoteChangeToken = token
                }
            }
        }

        operation.fetchRecordZoneChangesCompletionBlock = { [weak self] error in
            guard let self = self else { return }
            logger.info("Remote change fetch completed")
            self.syncContext.performAndWait {
                if let error = error {
                    logger.error("Failed to fetch record zone changes: \(String(describing: error))")

                    if self.cloudOperationQueue.suspendCloudInterop(dueTo: error) {
                        self.enqueueFetchRemoteChanges()
                    }
                } else {
                    importAndSaveChanges()
                }
            }
        }

        operation.queuePriority = .high
        if remoteChangeToken != nil {
            logger.info("Enqueuing operation to fetch remote changes using CKServerChangeToken")
        } else {
            logger.info("Enqueuing operation to fetch all remote changes")
        }
        cloudOperationQueue.addOperation(operation)
    }

    private func fetchRecords(_ recordIDs: [CKRecord.ID]) {
        let operation = CKFetchRecordsOperation(recordIDs: recordIDs)
        operation.fetchRecordsCompletionBlock = { [weak self] records, error in
            guard let self = self else { return }
            self.syncContext.performAndWait {
                if let error = error {
                    logger.error("Failed to fetch records: \(String(describing: error))")
                    if self.cloudOperationQueue.suspendCloudInterop(dueTo: error) {
                        self.fetchRecords(recordIDs)
                    } else {
                        logger.error("WHAT TO DO HERE?")
                    }
                    return
                }

                guard let records = records else {
                    self.handleUnexpectedResponse()
                    return
                }
                self.commitServerChangesToDatabase(with: Array(records.values), deletedRecordIDs: [])
            }
        }

        logger.info("Fetching remote records with IDs \(recordIDs.map { $0.recordName }.joined(separator: ", "))")
        cloudOperationQueue.addOperation(operation)
    }

    private func commitServerChangesToDatabase(with changedRecords: [CKRecord], deletedRecordIDs: [CKRecordIdentity]) {
        guard !changedRecords.isEmpty || !deletedRecordIDs.isEmpty else {
            logger.info("Finished record zone changes fetch with no changes")
            return
        }

        logger.info("Will commit \(changedRecords.count) changed record(s) and \(deletedRecordIDs.count) deleted record(s) to the database")
        for record in changedRecords {
            self.saveRecordDataLocally(record, option: .createIfNotFound)
        }
        for deletedID in deletedRecordIDs {
            self.localEntity(forIdentifier: deletedID)?.delete()
        }
        self.syncContext.saveAndLogIfErrored()
        logger.info("Completed updating local model(s) after download")
    }

    private func localEntity(forIdentifier remoteIdentifier: CKRecordIdentity) -> NSManagedObject? {
        guard let entityType = orderedTypesToSync.first(where: { remoteIdentifier.type == $0.ckRecordType }) else {
            logger.error("Unexpected record type supplied: \(remoteIdentifier.type)")
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
                logger.info("Local \(ckRecord.recordType) was deleted; skipping local update")
                return
            }

            logger.info("Updating \(ckRecord.recordType) from CKRecord \(ckRecord.recordID.recordName)")
            if option == .storeSystemFieldsOnly {
                localObject.setSystemAndIdentifierFields(from: ckRecord)
                logger.info("Updated system fields for CKRecord \(ckRecord.recordID.recordName) on object \(localObject.objectID.uriRepresentation().path)")
            } else {
                let keysPendingUpdate = localTransactionsPendingPushCompletion
                    .compactMap { $0.changes }
                    .flatMap { $0 }
                    .filter { $0.changeType == .update && $0.changedObjectID == localObject.objectID }
                    .compactMap { $0.updatedProperties }
                    .flatMap { $0 }
                    .map { $0.name }
                    .distinct()

                localObject.update(from: ckRecord, excluding: keysPendingUpdate)
                logger.info("Updated metadata for CKRecord \(ckRecord.recordID.recordName) on object \(localObject.objectID.uriRepresentation().path)")
            }
        } else if option == .createIfNotFound {
            logger.info("Creating new \(ckRecord.recordType) with record name \(ckRecord.recordID.recordName)")

            guard let type = orderedTypesToSync.first(where: { $0.ckRecordType == ckRecord.recordType }) else {
                logger.error("No type corresponding to \(ckRecord.recordType) found")
                return
            }
            let newObject = type.create(from: ckRecord, in: syncContext)
        }
    }

    func lookupLocalObject(for remoteRecord: CKRecord) -> CKRecordRepresentable? {
        guard let type = orderedTypesToSync.first(where: { $0.ckRecordType == remoteRecord.recordType }) else { return nil }

        let recordName = remoteRecord.recordID.recordName
        if let localItem = lookupLocalObject(ofType: type, withIdentifier: recordName) {
            logger.debug("Found local \(type.ckRecordType) with remote identifier \(recordName)")
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
            logger.debug("Found candidate local \(type.ckRecordType) for remote record \(recordName) using metadata")
            return localItem
        }

        logger.debug("No local \(type.ckRecordType) found for remote record \(recordName)")
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
