import Foundation
import CoreData
import CloudKit
import Combine
import PersistedPropertyWrapper

class UpstreamSyncProcessor {
    weak var coordinator: SyncCoordinator?
    let container: NSPersistentContainer
    let cloudOperationQueue: ConcurrentCKQueue
    let syncContext: NSManagedObjectContext
    let orderedTypesToSync: [CKRecordRepresentable.Type]
    var localTransactionsPendingPushCompletion = [NSPersistentHistoryTransaction]()
    private let historyFetcher: PersistentHistoryFetcher
    private var recordUploadBatchSize = 100

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
        syncContext.refreshAllObjects()
        NotificationCenter.default.publisher(for: .NSPersistentStoreRemoteChange, object: container.persistentStoreCoordinator)
            .sink(receiveValue: handleLocalChangeNotification)
            .store(in: &cancellables)
        if let latestConfirmedUploadedTransaction = latestConfirmedUploadedTransaction {
            logger.info("Initialising bufferBookmark to latest confirmed upload time of \(latestConfirmedUploadedTransaction)")
            bufferBookmark = latestConfirmedUploadedTransaction
        } else {
            logger.info("Initialising bufferBookmark to now")
            bufferBookmark = Date()
        }
        enqueueUploadOperations()
    }

    func stop() {
        self.cancellables.forEach { $0.cancel() }
        self.cancellables.removeAll()
    }

    func reset() {
        latestConfirmedUploadedTransaction = nil
    }

    private func handleLocalChangeNotification(_ notification: Notification) {
        guard let historyToken = notification.userInfo?[NSPersistentHistoryTokenKey] as? NSPersistentHistoryToken else {
            logger.error("Could not find Persistent History Token from remote change notification")
            return
        }
        logger.debug("Detected local change \(historyToken)")
        if let bufferBookmark = self.bufferBookmark {
            addNewTransactionsToBuffer(since: bufferBookmark)
        }
        enqueueUploadOperations()
    }

    private func addNewTransactionsToBuffer(since bookmark: Date) {
        // Although history fetcher tries to exclude syncContext transactions, sometimes the fetch request for history items
        // cannot be obtained (for unknown reasons) and so a predicate cannot be appended. So perform the filtering here too.
        var transactions = historyFetcher.fetch(fromDate: bookmark)
        transactions.removeAll { $0.contextName == syncContext.name }
        if transactions.isEmpty {
            logger.info("No transactions found since \(bookmark) to add to buffer")
            return
        }

        localTransactionsPendingPushCompletion.append(contentsOf: transactions)
        if let lastTransaction = transactions.last {
            bufferBookmark = lastTransaction.timestamp
        }
    }

    private func markFirstPendingTransactionAsComplete() {
        let transaction = localTransactionsPendingPushCompletion.removeFirst()
        logger.info("Updating confirmed pushed timestamp to \(transaction.timestamp)")
        latestConfirmedUploadedTransaction = transaction.timestamp
        historyFetcher.deleteHistory(beforeToken: transaction.token)
    }

    func enqueueUploadOperations() {
        let markPendingTransactionCompleteOperation = BlockOperation { [weak self] in
            guard let self = self else { return }
            self.markFirstPendingTransactionAsComplete()
        }

        // We don't need to be atomic if we are inserting new records; this means that if we are re-syncing and all our records already
        // exist on the remote server, we handle this much more efficiently, since we will get the errors all at once, rather than just
        // the first error within a partialFailure wrapper.
        let uploadInsertsOperation = uploadRecordChangesOperation()
        uploadInsertsOperation.name = "InsertNewRecords"
        uploadInsertsOperation.isAtomic = false

        let uploadChangesOperation = uploadRecordChangesOperation {
            markPendingTransactionCompleteOperation.cancel()
        }
        uploadChangesOperation.name = "UploadChangedRecords"

        let attachCKRecordsOperation = BlockOperation { [weak self] in
            guard let self = self else { return }
            self.syncContext.performAndWait {
                // First, get the transaction we are dealing with and merge its changes into the sync context
                if let transaction = self.localTransactionsPendingPushCompletion.first {
                    if let transactionNotificationUserInfo = transaction.objectIDNotification().userInfo {
                        NSManagedObjectContext.mergeChanges(fromRemoteContextSave: transactionNotificationUserInfo, into: [self.syncContext])
                    } else {
                        self.coordinator?.stopSyncDueToError(.unexpectedResponse("Merge notification UserInfo was nil"))
                        return
                    }
                }

                // Then, attach the inserts, updates and deletes to the pre-created CKModifyRecords operations
                let objectsForInsert = self.getObjectBatchForInsert()
                if objectsForInsert.isEmpty {
                    uploadInsertsOperation.cancel()
                } else {
                    uploadInsertsOperation.recordsToSave = objectsForInsert.map { $0.buildCKRecord() }
                    self.traceOperationDetails(uploadInsertsOperation)
                }

                if let transaction = self.localTransactionsPendingPushCompletion.first {
                    if let changes = transaction.changes {
                        logger.trace("Building CKRecords for local transaction consisting of changes:\n\(changes.description())")
                        uploadChangesOperation.recordsToSave = self.buildCKRecordUpdates(for: changes)
                        uploadChangesOperation.recordIDsToDelete = self.buildCKRecordDeletionIDs(for: changes)
                        self.traceOperationDetails(uploadChangesOperation)
                    }

                    // If we did not attach any meaningful work to the ModifyRecords operations, then cancel the operation and remove it as a
                    // dependency from further work.
                    if uploadChangesOperation.isEmpty {
                        logger.info("Upload Changes operation was empty, cancelling upload")
                        markPendingTransactionCompleteOperation.cancel()
                        uploadChangesOperation.cancel()

                        // Just mark the pending transaction as complete right now, since it did not produce any actionable work.
                        // Note that for simplicity we cancel the operation defined above which was going to do work, since a
                        // CKOperation will report a failure with code `operationCancelled` which would trigger the default error
                        // handling of cancelling the next operation. We cancel the operation and call the function ourself.
                        self.markFirstPendingTransactionAsComplete()
                    }
                } else {
                    markPendingTransactionCompleteOperation.cancel()
                    uploadChangesOperation.cancel()
                }

                // If our Insert batch was at the maximum size, then enqueue a further upload pass after this one
                if self.localTransactionsPendingPushCompletion.count > 1 || objectsForInsert.count == self.recordUploadBatchSize {
                    self.enqueueUploadOperations()
                }
            }
        }

        uploadInsertsOperation.addDependency(attachCKRecordsOperation)
        uploadChangesOperation.addDependency(attachCKRecordsOperation)

        markPendingTransactionCompleteOperation.addDependency(uploadChangesOperation)

        cloudOperationQueue.addOperations([attachCKRecordsOperation, uploadInsertsOperation, uploadChangesOperation, markPendingTransactionCompleteOperation])
        logger.info("Upload operations added to operation queue")
    }

    private func buildCKRecordUpdates(for changes: [NSPersistentHistoryChange]) -> [CKRecord] {
        // We want to extract the objects corresponding to the changes to that we can determine the entity types,
        // and then order them according to the orderedTypesToSync property (this will help keep CKReferences intact),
        // before generating our CKRecords.
        let changesAndObjects = changes.filter { $0.changeType == .update }
            .compactMap { change -> (change: NSPersistentHistoryChange, managedObject: CKRecordRepresentable)? in
                guard let managedObject = try? self.syncContext.existingObject(with: change.changedObjectID) as? CKRecordRepresentable else {
                    return nil
                }
                if managedObject.ckRecordEncodedSystemFields == nil { return nil }
                return (change, managedObject)
            }
        let changesByEntityType = Dictionary(grouping: changesAndObjects) { $0.managedObject.entity }

        return self.orderedTypesToSync.compactMap { changesByEntityType[$0.entity(in: syncContext)] }
            .flatMap { $0 }
            .compactMap { change, managedObject -> CKRecord? in
                guard let coreDataKeys = change.updatedProperties?.map(\.name) else { return nil }
                let ckRecordKeys = coreDataKeys.compactMap { managedObject.ckRecordKey(forLocalPropertyKey: $0) }
                if ckRecordKeys.isEmpty { return nil }
                return managedObject.buildCKRecord(ckRecordKeys: ckRecordKeys)
            }
    }

    private func buildCKRecordDeletionIDs(for changes: [NSPersistentHistoryChange]) -> [CKRecord.ID] {
        return changes.filter { $0.changeType == .delete }
            .compactMap { (change: NSPersistentHistoryChange) -> CKRecord.ID? in
                guard let remoteIdentifier = change.tombstone?[SyncConstants.remoteIdentifierKeyPath] as? String else { return nil }
                return CKRecord.ID(recordName: remoteIdentifier, zoneID: SyncConstants.zoneID)
            }
    }

    private func traceOperationDetails(_ opertion: CKModifyRecordsOperation) {
        if let recordsToSave = opertion.recordsToSave, !recordsToSave.isEmpty {
            logger.trace("CKModifyRecordsOperation \(opertion.name ?? "(unnamed)") had \(recordsToSave.count) records to save:\n\(recordsToSave.map { $0.description }.joined(separator: "\n"))")
        }
        if let recordsToDelete = opertion.recordIDsToDelete, !recordsToDelete.isEmpty {
            logger.trace("CKModifyRecordsOperation \(opertion.name ?? "(unnamed)") had \(recordsToDelete.count) records to delete:\n\(recordsToDelete.map { $0.recordName }.joined(separator: "\n"))")
        }
    }

    private func getObjectBatchForInsert() -> [CKRecordRepresentable] {
        var objects = [CKRecordRepresentable]()
        for entity in orderedTypesToSync {
            if objects.count >= recordUploadBatchSize { break }
            let request = entity.fetchRequest(in: syncContext)
            request.returnsObjectsAsFaults = false
            request.includesPropertyValues = true
            request.predicate = NSPredicate(format: "\(SyncConstants.ckRecordEncodedSystemFieldsKey) = nil")
            request.fetchLimit = recordUploadBatchSize - objects.count
            let fetchResults = try! syncContext.fetch(request) as! [CKRecordRepresentable]
            objects.append(contentsOf: fetchResults)
        }
        return objects
    }

    private func uploadRecordChangesOperation(didFail: (() -> Void)? = nil) -> CKModifyRecordsOperation {
        let operation = CKModifyRecordsOperation()
        operation.modifyRecordsCompletionBlock = { [weak self] serverRecords, _, error in
            guard let self = self else { return }
            self.syncContext.performAndWait {
                if let error = error {
                    self.handleUploadError(error, records: operation.recordsToSave ?? [], ids: operation.recordIDsToDelete ?? [])
                    didFail?()
                } else {
                    logger.info("Completed upload. Updating local models with server record data.")
                    guard let serverRecords = serverRecords else {
                        logger.error("Unexpected nil `serverRecords` in response from CKModifyRecordsOperation operation \(operation.name ?? "(unnamed)")")
                        self.coordinator?.stopSyncDueToError(.unexpectedResponse("Unexpected nil `serverRecords` in response from CKModifyRecordsOperation operation \(operation.name ?? "(unnamed)")"))
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
            self.coordinator?.stopSyncDueToError(.unexpectedErrorType(error))
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
            self.coordinator?.stopSyncDueToError(.unexpectedResponse("CloudKit batch limit exceeded"))
        } else if ckError.code == .operationCancelled {
            return
        } else if ckError.code == .partialFailure {
            handlePartialUploadFailure(ckError, records: records, ids: ids)
        } else if ckError.code == .userDeletedZone {
            logger.info("Disabling sync due to deleted record zone")
            self.coordinator?.disableSync(reason: .cloudDataDeleted)
        } else if ckError.code == .notAuthenticated {
            logger.info("Disabling sync due to user not being authenticated")
            self.coordinator?.stop()
        } else if let retryDelay = ckError.retryAfterSeconds {
            logger.info("Instructed to delay for \(retryDelay) seconds: suspending operation queue")
            cloudOperationQueue.suspend()
            // Delay for slightly longer (10%) to try to get into iCloud's good books
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + (retryDelay * 1.1)) {
                logger.info("Resuming operation queue")
                self.cloudOperationQueue.resume()
                self.enqueueUploadOperations()
            }
        } else {
            logger.critical("Unhandled error response \(ckError)")
            self.coordinator?.stopSyncDueToError(.unhandledError(ckError))
        }
    }

    private func handlePartialUploadFailure(_ ckError: CKError, records: [CKRecord], ids: [CKRecord.ID]) {
        guard let errorsByItemId = ckError.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: Error] else {
            logger.error("Missing CKPartialErrorsByItemIDKey data")
            self.coordinator?.stopSyncDueToError(.unexpectedResponse("Missing CKPartialErrorsByItemIDKey data"))
            return
        }
        guard let coordinator = coordinator else {
            logger.error("SyncCoordinator was nil handling CKRecord upload failure")
            return
        }

        var refetchIDs = [CKRecord.ID]()
        for record in records {
            guard let uploadError = errorsByItemId[record.recordID] as? CKError else {
                logger.error("Missing CKError for record \(record.recordID.recordName)")
                self.coordinator?.stopSyncDueToError(.unexpectedResponse("Missing CKError for record \(record.recordID.recordName)"))
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
                // Answer: invalid CKRecord.
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
            coordinator.requestFetch(for: refetchIDs)
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
