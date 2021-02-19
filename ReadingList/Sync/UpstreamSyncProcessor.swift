import Foundation
import CoreData
import CloudKit
import Combine
import PersistedPropertyWrapper

class UpstreamSyncProcessor {
    weak var coordinator: SyncCoordinator?
    let cloudOperationQueue: ConcurrentCKQueue
    let syncContext: NSManagedObjectContext
    let orderedTypesToSync: [CKRecordRepresentable.Type]
    var localTransactionsPendingPushCompletion = [NSPersistentHistoryTransaction]()

    init(syncContext: NSManagedObjectContext, cloudOperationQueue: ConcurrentCKQueue, types: [CKRecordRepresentable.Type]) {
        self.syncContext = syncContext
        self.cloudOperationQueue = cloudOperationQueue
        self.orderedTypesToSync = types
    }

    @Persisted("SyncEngine_LocalChangeTimestamp")
    private(set) var latestConfirmedUploadedTransaction: Date?

    private var bufferBookmark: Date?
    private lazy var historyFetcher = PersistentHistoryFetcher(context: syncContext, excludeHistoryFromContextWithName: syncContext.name!)
    private var cancellables = Set<AnyCancellable>()

    func start(storeCoordinator: NSPersistentStoreCoordinator) {
        NotificationCenter.default.publisher(for: .NSPersistentStoreRemoteChange, object: storeCoordinator)
            .sink(receiveValue: handleLocalChangeNotification)
            .store(in: &cancellables)
        self.bufferBookmark = latestConfirmedUploadedTransaction
        self.enqueueUploadOperations()
    }

    func enqueueUploadOperations() {
        if let bufferBookmark = self.bufferBookmark {
            addNewTransactionsToBuffer(since: bufferBookmark)
            if !localTransactionsPendingPushCompletion.isEmpty {
                enqueueUploadOperationsForPendingTransactions()
            }
        } else {
            enqueueUploadOfAllObjects()
        }
    }

    private func handleLocalChangeNotification(_ notification: Notification) {
        logger.debug("Detected local change")
        cloudOperationQueue.addBlock {
            self.syncContext.perform {
                logger.debug("Merging changes into syncContext")
                self.syncContext.mergeChanges(fromContextDidSave: notification)
                self.enqueueUploadOperations()
            }
        }
    }

    private func addNewTransactionsToBuffer(since bookmark: Date) {
        let transactions = historyFetcher.fetch(fromDate: bookmark)
        localTransactionsPendingPushCompletion.append(contentsOf: transactions)
        if let lastTransaction = transactions.last {
            bufferBookmark = lastTransaction.timestamp
        }
    }

    private func enqueueUploadOperationsForPendingTransactions() {
        let uploadOperation = uploadRecordsOperation()

        let updateBufferBookmarkOperation = BlockOperation {
            let transaction = self.localTransactionsPendingPushCompletion.removeFirst()
            logger.info("Updating confirmed pushed timestamp to \(transaction.timestamp)")
            self.latestConfirmedUploadedTransaction = transaction.timestamp
            self.historyFetcher.deleteHistory(beforeToken: transaction.token)
            self.enqueueUploadOperations()
        }
        updateBufferBookmarkOperation.addDependency(uploadOperation)

        let buildCKRecordsOperation = BlockOperation {
            logger.info("Building CKRecords for upload")
            self.syncContext.performAndWait {
                guard let transaction = self.localTransactionsPendingPushCompletion.first else {
                    logger.info("No transactions are pending upload; cancelling upload operation.")
                    uploadOperation.cancel()
                    updateBufferBookmarkOperation.cancel()
                    return
                }
                if let changes = transaction.changes {
                    self.attachCKRecords(for: changes, to: uploadOperation)
                }

                if uploadOperation.recordsToSave?.isEmpty != false && uploadOperation.recordIDsToDelete?.isEmpty != false {
                    logger.info("Transaction had no changes, cancelling upload and moving timestamp to \(transaction.timestamp)")
                    self.localTransactionsPendingPushCompletion.removeFirst()
                    uploadOperation.cancel()
                    updateBufferBookmarkOperation.cancel()
                    self.latestConfirmedUploadedTransaction = transaction.timestamp
                    self.enqueueUploadOperations()
                }
            }
        }
        uploadOperation.addDependency(buildCKRecordsOperation)

        cloudOperationQueue.addOperations([buildCKRecordsOperation, uploadOperation, updateBufferBookmarkOperation])
        logger.info("Upload operations added to operation queue")
    }

    private func enqueueUploadOfAllObjects() {
        let uploadOperation = uploadRecordsOperation()
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
            self.latestConfirmedUploadedTransaction = timestamp
            self.bufferBookmark = timestamp

            // In case there have been any local changes since we started uploading all records:
            self.enqueueUploadOperations()
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

    private func attachCKRecords(for changes: [NSPersistentHistoryChange], to uploadOpertion: CKModifyRecordsOperation) {
        logger.debug("Building CKRecords for local transaction consisting of changes:\n\(changes.description())")

        self.syncContext.performAndWait {
            // We want to extract the objects corresponding to the changes to that we can determine the entity types,
            // and then order them according to the orderedTypesToSync property (this will help keep CKReferences intact),
            // before generating our CKRecords.
            let changesAndObjects = changes.filter { $0.changeType != .delete }
                .compactMap { change -> (change: NSPersistentHistoryChange, managedObject: CKRecordRepresentable)? in
                    guard let managedObject = self.syncContext.object(with: change.changedObjectID) as? CKRecordRepresentable else { return nil }
                    return (change, managedObject)
                }
            let changesByEntityType = Dictionary(grouping: changesAndObjects) { $0.managedObject.entity }

            uploadOpertion.recordsToSave = self.orderedTypesToSync.compactMap { changesByEntityType[$0.entity()] }
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

            uploadOpertion.recordIDsToDelete = changes.filter { $0.changeType == .delete }
                .compactMap { (change: NSPersistentHistoryChange) -> CKRecord.ID? in
                    guard let remoteIdentifier = change.tombstone?[SyncConstants.remoteIdentifierKeyPath] as? String else { return nil }
                    return CKRecord.ID(recordName: remoteIdentifier, zoneID: SyncConstants.zoneID)
                }

            // Buiding the CKRecord can in some cases cause updates to the managed object; save if this is the case
            self.syncContext.saveIfChanged()
        }
    }

    private func uploadRecordsOperation() -> CKModifyRecordsOperation {
        let operation = CKModifyRecordsOperation()
        operation.modifyRecordsCompletionBlock = { [weak self] serverRecords, _, error in
            guard let self = self else { return }
            self.syncContext.performAndWait {
                if let error = error {
                    self.handleUploadError(error, records: operation.recordsToSave ?? [], ids: operation.recordIDsToDelete ?? [])
                } else {
                    logger.info("Completed upload. Updating local models with server record data.")
                    guard let serverRecords = serverRecords else {
                        logger.error("Unexpected nil `serverRecords` in response from CKModifyRecordsOperation operation")
                        guard let coordinator = self.coordinator else { fatalError("Missing coordinator") }
                        coordinator.handleUnexpectedResponse()
                        return
                    }
                    self.updateLocalModelsAfterUpload(with: serverRecords)
                }
            }
        }

        operation.savePolicy = .ifServerRecordUnchanged
        return operation
    }

    private func handleUploadError(_ error: Error, records: [CKRecord], ids: [CKRecord.ID]) {
        guard let ckError = error as? CKError else {
            guard let coordinator = self.coordinator else { fatalError("Missing coordinator") }
            coordinator.handleUnexpectedResponse()
            return
        }

        if records.isEmpty && ids.isEmpty && ckError.code == .operationCancelled {
            // No need to log this trivial cancellation
            return
        }

        logger.error("Upload error occurred with CKError code \(ckError.code.name)")
        if ckError.code == .limitExceeded {
            logger.error("CloudKit batch limit exceeded, sending records in chunks")
            fatalError("Not implemented: batch uploads. Here we should divide the records in chunks and upload in batches instead of trying everything at once.")
        } else if ckError.code == .operationCancelled {
            return
        } else if ckError.code == .partialFailure {
            handlePartialUploadFailure(ckError, records: records, ids: ids)
        } else if ckError.code == .userDeletedZone {
            guard let coordinator = coordinator else { fatalError("Missing sync coordinator") }
            logger.info("Disabling sync due to deleted record zone")
            coordinator.disableSync()
        } else if ckError.code == .notAuthenticated {
            guard let coordinator = coordinator else { fatalError("Missing sync coordinator") }
            logger.info("Disabling sync due to user not being authenticated")
            coordinator.disableSync()
        } else if let retryDelay = ckError.retryAfterSeconds {
            logger.info("Instructed to delay for \(retryDelay) seconds: suspending operation queue")
            cloudOperationQueue.suspend()
            // Enqueue an upload operation ready for when we unsuspend things
            enqueueUploadOperations()
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + retryDelay) {
                logger.info("Resuming operation queue")
                self.cloudOperationQueue.resume()
            }
        } else {
            logger.critical("Unhandled error response")
            guard let coordinator = self.coordinator else { fatalError("Missing coordinator") }
            coordinator.handleUnexpectedResponse()
        }
    }

    private func handlePartialUploadFailure(_ ckError: CKError, records: [CKRecord], ids: [CKRecord.ID]) {
        guard let errorsByItemId = ckError.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: Error] else {
            logger.error("Missing CKPartialErrorsByItemIDKey data")
            guard let coordinator = self.coordinator else { fatalError("Missing coordinator") }
            coordinator.handleUnexpectedResponse()
            return
        }

        var refetchIDs = [CKRecord.ID]()
        for record in records {
            guard let uploadError = errorsByItemId[record.recordID] as? CKError else {
                logger.error("Missing CKError for record \(record.recordID.recordName)")
                guard let coordinator = self.coordinator else { fatalError("Missing coordinator") }
                coordinator.handleUnexpectedResponse()
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
                guard let localObject = LocalDataMatcher(syncContext: syncContext, types: orderedTypesToSync).lookupLocalObject(for: record) else {
                    logger.error("Could not find local object for CKRecord.")
                    return
                }
                localObject.setSystemFields(nil)
            } else {
                logger.error("Unhandled error \(uploadError.code.name) for CKRecord \(record.recordID.recordName)")
            }
        }

        syncContext.saveIfChanged()
        if !refetchIDs.isEmpty {
            guard let coordinator = coordinator else { fatalError("Missing coordinator") }
            logger.info("Requesting fetch for \(refetchIDs.count) records")
            coordinator.requestFetch(for: refetchIDs)
        }
        enqueueUploadOperations()
    }

    private func updateLocalModelsAfterUpload(with records: [CKRecord]) {
        guard !records.isEmpty else { return }
        let dataLookup = LocalDataMatcher(syncContext: syncContext, types: orderedTypesToSync)
        for record in records {
            dataLookup.lookupLocalObject(for: record)?.setSystemAndIdentifierFields(from: record)
        }
        syncContext.saveAndLogIfErrored()
        logger.info("Completed updating \(records.count) local model(s) after upload")
    }
}
