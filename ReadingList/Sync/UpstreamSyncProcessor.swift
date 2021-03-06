import Foundation
import CoreData
import CloudKit
import Combine
import PersistedPropertyWrapper

class UpstreamSyncProcessor {
    weak var coordinator: SyncCoordinator!
    let container: NSPersistentContainer
    let cloudOperationQueue: ConcurrentCKQueue
    let syncContext: NSManagedObjectContext
    let orderedTypesToSync: [CKRecordRepresentable.Type]
    var localTransactionsPendingPushCompletion = [NSPersistentHistoryTransaction]()
    private let historyFetcher: PersistentHistoryFetcher

    init(container: NSPersistentContainer, syncContext: NSManagedObjectContext, cloudOperationQueue: ConcurrentCKQueue, types: [CKRecordRepresentable.Type]) {
        self.container = container
        self.syncContext = syncContext
        self.cloudOperationQueue = cloudOperationQueue
        self.orderedTypesToSync = types
        self.historyFetcher = PersistentHistoryFetcher(context: container.newBackgroundContext(), excludeHistoryFromContextWithName: syncContext.name!)
    }

    @Persisted("SyncEngine_LocalChangeTimestamp")
    private(set) var latestConfirmedUploadedTransaction: Date?

    private var bufferBookmark: Date?
    private var cancellables = Set<AnyCancellable>()

    func start() {
        self.syncContext.refreshAllObjects()
        NotificationCenter.default.publisher(for: .NSPersistentStoreRemoteChange, object: container.persistentStoreCoordinator)
            .sink(receiveValue: handleLocalChangeNotification)
            .store(in: &cancellables)
        self.bufferBookmark = latestConfirmedUploadedTransaction
        self.enqueueUploadOperations()
    }

    func stop() {
        self.cancellables.forEach { $0.cancel() }
        self.cancellables.removeAll()
    }

    func reset() {
        latestConfirmedUploadedTransaction = nil
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
        guard let historyToken = notification.userInfo?[NSPersistentHistoryTokenKey] as? NSPersistentHistoryToken else {
            logger.critical("Could not find Persistent History Token from remote change notification")
            self.coordinator.stopSyncDueToError(.unexpectedResponse("Change notification had no NSPersistentHistoryToken"))
            return
        }
        logger.debug("Detected local change \(historyToken)")
        enqueueUploadOperations()
    }

    private func addNewTransactionsToBuffer(since bookmark: Date) {
        // Although history fetcher tries to exclude syncContext transactions, sometimes the fetch request for history items
        // cannot be obtained (for unknown reasons) and so a predicate cannot be appended. So perform the filtering here too.
        var transactions = historyFetcher.fetch(fromDate: bookmark)
        transactions.removeAll { $0.contextName == syncContext.name }
        if transactions.isEmpty {
            logger.info("No transactions found")
        }

        localTransactionsPendingPushCompletion.append(contentsOf: transactions)
        if let lastTransaction = transactions.last {
            bufferBookmark = lastTransaction.timestamp
        }
    }

    private func enqueueUploadOperationsForPendingTransactions() {
        let updateBufferBookmarkOperation = BlockOperation {
            let transaction = self.localTransactionsPendingPushCompletion.removeFirst()
            logger.info("Updating confirmed pushed timestamp to \(transaction.timestamp)")
            self.latestConfirmedUploadedTransaction = transaction.timestamp
            self.historyFetcher.deleteHistory(beforeToken: transaction.token)
            self.enqueueUploadOperations()
        }

        let uploadOperation = uploadRecordsOperation {
            updateBufferBookmarkOperation.cancel()
        }

        let buildCKRecordsOperation = BlockOperation {
            logger.info("Building CKRecords for upload")
            self.syncContext.performAndWait {
                guard let transaction = self.localTransactionsPendingPushCompletion.first else {
                    logger.info("No transactions are pending upload; cancelling upload operation.")
                    uploadOperation.cancel()
                    updateBufferBookmarkOperation.cancel()
                    return
                }
                guard let transactionNotificationUserInfo = transaction.objectIDNotification().userInfo else {
                    self.coordinator.stopSyncDueToError(.unexpectedResponse("Merge notification UserInfo was nil"))
                    return
                }
                NSManagedObjectContext.mergeChanges(fromRemoteContextSave: transactionNotificationUserInfo, into: [self.syncContext])
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
        updateBufferBookmarkOperation.addDependency(uploadOperation)

        cloudOperationQueue.addOperations([buildCKRecordsOperation, uploadOperation, updateBufferBookmarkOperation])
        logger.info("Upload operations added to operation queue")
    }

    private func enqueueUploadOfAllObjects() {
        var timestamp: Date?

        let updateBookmarkOperation = BlockOperation {
            guard let timestamp = timestamp else { fatalError("Unexpected nil timestamp") }
            logger.info("Updating upload bookmark to \(timestamp)")
            self.latestConfirmedUploadedTransaction = timestamp
            self.bufferBookmark = timestamp

            // In case there have been any local changes since we started uploading all records:
            self.enqueueUploadOperations()
        }

        let uploadOperation = uploadRecordsOperation {
            updateBookmarkOperation.cancel()
        }

        let fetchRecordsOperation = BlockOperation {
            logger.info("Fetching all records to upload")
            timestamp = Date()
            let allRecords = self.getAllObjectCkRecords() // TODO Perhaps just un-uploaded objects?
            uploadOperation.recordsToSave = allRecords
        }

        uploadOperation.addDependency(fetchRecordsOperation)
        updateBookmarkOperation.addDependency(uploadOperation)
        cloudOperationQueue.addOperations([fetchRecordsOperation, uploadOperation, updateBookmarkOperation])
    }

    private func getAllObjectCkRecords() -> [CKRecord] {
        var ckRecords: [CKRecord] = []
        for entity in orderedTypesToSync {
            let request = entity.fetchRequest(in: syncContext)
            request.returnsObjectsAsFaults = false
            request.includesPropertyValues = true
            request.fetchBatchSize = 100
            let objects = try! syncContext.fetch(request) as! [CKRecordRepresentable]
            ckRecords.append(contentsOf: objects.map { $0.buildCKRecord() })
        }

        return ckRecords
    }

    private func attachCKRecords(for changes: [NSPersistentHistoryChange], to uploadOpertion: CKModifyRecordsOperation) {
        logger.debug("Building CKRecords for local transaction consisting of changes:\n\(changes.description())")

        // We want to extract the objects corresponding to the changes to that we can determine the entity types,
        // and then order them according to the orderedTypesToSync property (this will help keep CKReferences intact),
        // before generating our CKRecords.
        let changesAndObjects = changes.filter { $0.changeType != .delete }
            .compactMap { change -> (change: NSPersistentHistoryChange, managedObject: CKRecordRepresentable)? in
                guard let managedObject = try? self.syncContext.existingObject(with: change.changedObjectID) as? CKRecordRepresentable else {
                    return nil
                }
                return (change, managedObject)
            }
        let changesByEntityType = Dictionary(grouping: changesAndObjects) { $0.managedObject.entity }

        uploadOpertion.recordsToSave = self.orderedTypesToSync.compactMap { changesByEntityType[$0.entity(in: syncContext)] }
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

        if !uploadOpertion.recordsToSave!.isEmpty {
            logger.trace("Attached \(uploadOpertion.recordsToSave!.count) records to save:\n\(uploadOpertion.recordsToSave!.map { $0.description }.joined(separator: "\n"))")
        }
        if !uploadOpertion.recordIDsToDelete!.isEmpty {
            logger.trace("Attached \(uploadOpertion.recordIDsToDelete!.count) records to delete:\n\(uploadOpertion.recordIDsToDelete!.map { $0.recordName }.joined(separator: "\n"))")
        }
    }

    private func uploadRecordsOperation(didFail: @escaping () -> Void) -> CKModifyRecordsOperation {
        let operation = CKModifyRecordsOperation()
        operation.modifyRecordsCompletionBlock = { [weak self] serverRecords, _, error in
            guard let self = self else { return }
            self.syncContext.performAndWait {
                if let error = error {
                    self.handleUploadError(error, records: operation.recordsToSave ?? [], ids: operation.recordIDsToDelete ?? [])
                    didFail()
                } else {
                    logger.info("Completed upload. Updating local models with server record data.")
                    guard let serverRecords = serverRecords else {
                        logger.error("Unexpected nil `serverRecords` in response from CKModifyRecordsOperation operation")
                        self.coordinator.stopSyncDueToError(.unexpectedResponse("Unexpected nil `serverRecords` in response from CKModifyRecordsOperation operation"))
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
            self.coordinator.stopSyncDueToError(.unexpectedErrorType(error))
            return
        }

        if records.isEmpty && ids.isEmpty && ckError.code == .operationCancelled {
            // No need to log this trivial cancellation
            return
        }

        logger.error("Upload error occurred with CKError code \(ckError.code.name)")
        if ckError.code == .limitExceeded {
            // TODO: Implement!
            logger.error("CloudKit batch limit exceeded, sending records in chunks")
            self.coordinator.stopSyncDueToError(.unexpectedResponse("CloudKit batch limit exceeded"))
        } else if ckError.code == .operationCancelled {
            return
        } else if ckError.code == .partialFailure {
            handlePartialUploadFailure(ckError, records: records, ids: ids)
        } else if ckError.code == .userDeletedZone {
            logger.info("Disabling sync due to deleted record zone")
            self.coordinator.disableSync(reason: .cloudDataDeleted)
        } else if ckError.code == .notAuthenticated {
            logger.info("Disabling sync due to user not being authenticated")
            self.coordinator.stop()
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
            logger.critical("Unhandled error response \(ckError)")
            self.coordinator.stopSyncDueToError(.unhandledError(ckError))
        }
    }

    private func handlePartialUploadFailure(_ ckError: CKError, records: [CKRecord], ids: [CKRecord.ID]) {
        guard let errorsByItemId = ckError.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: Error] else {
            logger.error("Missing CKPartialErrorsByItemIDKey data")
            self.coordinator.stopSyncDueToError(.unexpectedResponse("Missing CKPartialErrorsByItemIDKey data"))
            return
        }

        var refetchIDs = [CKRecord.ID]()
        for record in records {
            guard let uploadError = errorsByItemId[record.recordID] as? CKError else {
                logger.error("Missing CKError for record \(record.recordID.recordName)")
                self.coordinator.stopSyncDueToError(.unexpectedResponse("Missing CKError for record \(record.recordID.recordName)"))
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
                // TODO We should probably re-upload this somehow. Another buffer of non-uploaded records?
                localObject.setSystemFields(nil)
            } else if uploadError.code == .invalidArguments {
                // TODO What causes this?
                logger.error("InvalidArguments error\n\(ckError)\nfor record:\n\(record)")
                coordinator.stopSyncDueToError(.unhandledError(ckError))
            } else {
                logger.error("Unhandled error\n\(ckError)\nfor record:\n\(record)")
                coordinator.stopSyncDueToError(.unhandledError(ckError))
            }
        }

        // TODO: We are not handling deletion failures at all. Do they need handling? Does any error exist which represents anything other than
        // "the item was already deleted"?

        syncContext.saveIfChanged()
        if !refetchIDs.isEmpty {
            logger.info("Requesting fetch for \(refetchIDs.count) records")
            self.coordinator.requestFetch(for: refetchIDs)
        }
        enqueueUploadOperations()
    }

    private func updateLocalModelsAfterUpload(with records: [CKRecord]) {
        guard !records.isEmpty else { return }
        let dataLookup = LocalDataMatcher(syncContext: syncContext, types: orderedTypesToSync)
        for record in records {
            dataLookup.lookupLocalObject(for: record)?.setSystemFields(from: record)
        }
        syncContext.saveAndLogIfErrored()
        logger.info("Completed updating \(records.count) local model(s) after upload")
    }
}
