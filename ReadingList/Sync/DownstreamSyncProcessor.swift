import Foundation
import CloudKit
import CoreData
import UIKit
import PersistedPropertyWrapper

class DownstreamSyncProcessor {
    weak var coordinator: SyncCoordinator?
    let orderedTypesToSync: [CKRecordRepresentable.Type]
    let syncContext: NSManagedObjectContext
    let cloudOperationQueue: ConcurrentCKQueue
    let localDataMatcher: LocalDataMatcher

    init(syncContext: NSManagedObjectContext, types: [CKRecordRepresentable.Type], cloudOperationQueue: ConcurrentCKQueue) {
        self.orderedTypesToSync = types
        self.syncContext = syncContext
        self.cloudOperationQueue = cloudOperationQueue
        self.localDataMatcher = LocalDataMatcher(syncContext: syncContext, types: types)
    }

    @Persisted(archivedDataKey: "SyncEngine_SeverChangeToken")
    private var remoteChangeToken: CKServerChangeToken?

    func resetChangeTracking() {
        remoteChangeToken = nil
    }

    func enqueueFetchRemoteChanges(completion: ((UIBackgroundFetchResult) -> Void)? = nil) {
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
            guard let recordSchemaVersion = record[SyncConstants.recordSchemaVersionKey] as? Int else {
                logger.critical("CKRecord did not have a \(SyncConstants.recordSchemaVersionKey) property")
                self.coordinator?.stopSyncDueToError(.unexpectedResponse("Record had no \(SyncConstants.recordSchemaVersionKey) property"))
                return
            }
            if recordSchemaVersion > SyncConstants.recordSchemaVersion.rawValue {
                logger.error("CKRecord schema version was \(recordSchemaVersion) but the current schema version is \(SyncConstants.recordSchemaVersion.rawValue)")
                operation.cancel()
                self.coordinator?.disableSyncDueOutOfDateLocalAppVersion()
            }
        }

        var deletionIDs = [CKRecord.RecordType: [CKRecord.ID]]()
        operation.recordWithIDWasDeletedBlock = { recordID, recordType in
            logger.trace("recordWithIDWasDeletedBlock for CKRecord.ID \(recordID.recordName)")
            deletionIDs.append(recordID, to: recordType)
        }
        var anyChanges = false

        func processChanges(newToken: CKServerChangeToken?) {
            importChanges(updatesByType: recordsByType, deletionsByType: deletionIDs)
            if !recordsByType.isEmpty || !deletionIDs.isEmpty {
                anyChanges = true
            }

            logger.info("Saving syncContext after record change import")
            self.syncContext.saveIfChanged()
            if let newToken = newToken {
                self.remoteChangeToken = newToken
            }

            recordsByType.removeAll()
            deletionIDs.removeAll()
        }

        operation.recordZoneChangeTokensUpdatedBlock = { [weak self] _, changeToken, _ in
            guard let self = self else { return }
            self.syncContext.performAndWait {
                logger.info("Server change token updated")
                processChanges(newToken: changeToken)
            }
        }

        operation.recordZoneFetchCompletionBlock = { [weak self] _, token, _, _, error in
            guard let self = self else { return }
            self.syncContext.performAndWait {
                if error != nil {
                    logger.error("Error in recordZoneFetchCompletionBlock")
                    // We shall handle the error once, in the final completion block
                } else {
                    logger.info("Remote record fetch completed; commiting new change token")
                    processChanges(newToken: token)
                }
            }
        }

        operation.fetchRecordZoneChangesCompletionBlock = { [weak self] error in
            guard let self = self else { return }
            if let error = error {
                logger.error("Remote change fetch completed with error")
                if let completion = completion {
                    logger.info("Calling UIBackgroundFetch completion handler with failure")
                    completion(.failed)
                }
                self.handleDownloadError(error) {
                    self.enqueueFetchRemoteChanges()
                }
            } else {
                self.syncContext.performAndWait {
                    logger.info("Remote change fetch completed successfully. Resolving any references")
                    CKReferenceResolver(context: self.syncContext).resolveReferences()
                    if let completion = completion {
                        logger.info("Calling UIBackgroundFetch completion handler with \(anyChanges ? "newData" : "noData")")
                        completion(anyChanges ? .newData : .noData)
                    }
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

    func fetchRecords(_ recordIDs: [CKRecord.ID]) {
        let operation = CKFetchRecordsOperation(recordIDs: recordIDs)
        operation.fetchRecordsCompletionBlock = { [weak self] records, error in
            guard let self = self else { return }
            logger.trace("CKFetchRecordsOperation completed")
            self.syncContext.performAndWait {
                if let error = error {
                    self.handleDownloadError(error) {
                        logger.info("Re-enqueing fetch of records after suspension completion")
                        self.fetchRecords(recordIDs)
                    }
                } else if let records = records {
                    self.commitServerChangesToDatabase(with: Array(records.values), deletedRecordIDs: [])
                } else {
                    self.coordinator?.stopSyncDueToError(
                        .unexpectedResponse("CKFetchRecordsOperation had neither error nor records")
                    )
                }
            }
        }
        // Jump the queue, since getting downstream changes in sooner is better for sync performance (otherwise,
        // we keep trying to upload over and over getting failures).
        operation.queuePriority = .high

        logger.info("Requesting fetch of remote records with IDs \(recordIDs.map { $0.recordName }.joined(separator: ", "))")
        cloudOperationQueue.addOperation(operation)
    }

    private func handleDownloadError(_ error: Error, onSuspensionCompletion: (() -> Void)?) {
        guard let ckError = error as? CKError else {
            self.coordinator?.stopSyncDueToError(.unexpectedErrorType(error))
            return
        }

        logger.error("Handling download CKError with code \(ckError.code.name)")
        if ckError.code == .operationCancelled || ckError.code == .networkFailure || ckError.code == .networkUnavailable {
            return
        } else if ckError.code == .changeTokenExpired {
            logger.error("Change token expired, resetting token and trying again")
            self.remoteChangeToken = nil
            self.enqueueFetchRemoteChanges()
        } else if ckError.code == .userDeletedZone {
            coordinator?.handleCloudDataDeletion()
            return
        } else if ckError.code == .partialFailure {
            guard let innerErrors = ckError.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecordZone.ID: CKError],
                  let relevantInnerError = innerErrors[SyncConstants.zoneID] else {
                logger.error("Missing inner error when fetching zone changes: \(ckError)")
                self.coordinator?.stopSyncDueToError(.unexpectedResponse("Missing inner error when fetching zone changes"))
                return
            }
            handleDownloadError(relevantInnerError, onSuspensionCompletion: nil)
        } else if let retryDelay = ckError.retryAfterSeconds {
            logger.info("Instructed to delay for \(retryDelay) seconds: suspending operation queue")
            self.cloudOperationQueue.suspend()
            // Delay for slightly longer (10%) to try to get into iCloud's good books
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + (retryDelay * 1.1)) {
                logger.info("Resuming operation queue")
                self.cloudOperationQueue.resume()
                onSuspensionCompletion?()
            }
        } else {
            logger.critical("Unhandled error response \(ckError)")
            self.coordinator?.stopSyncDueToError(.unhandledError(ckError))
        }
    }

    private func importChanges(updatesByType: [CKRecord.RecordType: [CKRecord]], deletionsByType: [CKRecord.RecordType: [CKRecord.ID]]) {
        for recordType in self.orderedTypesToSync {
            if let recordsToUpdate = updatesByType[recordType.ckRecordType] {
                for record in recordsToUpdate {
                    logger.trace("Saving data for CKRecord with ID \(record.recordID.recordName)")
                    self.saveRecordDataLocally(record)
                }
            }
            if let idsToDelete = deletionsByType[recordType.ckRecordType] {
                for idToDelete in idsToDelete {
                    logger.trace("Deleting object (if exists) CKRecord.ID \(idToDelete.recordName)")
                    let localObject = self.localEntity(forIdentifier: CKRecordIdentity(ID: idToDelete, type: recordType.ckRecordType))
                    localObject?.delete()
                }
            }
        }
    }

    private func commitServerChangesToDatabase(with changedRecords: [CKRecord], deletedRecordIDs: [CKRecordIdentity]) {
        guard !changedRecords.isEmpty || !deletedRecordIDs.isEmpty else {
            logger.info("Finished record zone changes fetch with no changes")
            return
        }

        logger.info("Will commit \(changedRecords.count) changed record(s) and \(deletedRecordIDs.count) deleted record(s) to the database")
        for record in changedRecords {
            self.saveRecordDataLocally(record)
        }
        for deletedID in deletedRecordIDs {
            self.localEntity(forIdentifier: deletedID)?.delete()
        }
        self.syncContext.saveIfChanged()
        logger.info("Completed updating local model(s) after download")
    }

    private func localEntity(forIdentifier remoteIdentifier: CKRecordIdentity) -> NSManagedObject? {
        guard let entityType = orderedTypesToSync.first(where: { remoteIdentifier.type == $0.ckRecordType }) else {
            logger.error("Unexpected record type supplied: \(remoteIdentifier.type)")
            return nil
        }
        return localDataMatcher.lookupLocalObject(ofType: entityType, withIdentifier: remoteIdentifier.ID.recordName)
    }

    private func saveRecordDataLocally(_ ckRecord: CKRecord) {
        guard let coordinator = coordinator else {
            logger.error("SyncCoordinator was nil, not saving CKRecord data")
            return
        }
        if let localObject = localDataMatcher.lookupLocalObject(for: ckRecord) {
            if localObject.ckRecordEncodedSystemFields == nil {
                logger.info("Merging \(ckRecord.recordType) \(localObject.objectID.uriRepresentation().path) from CKRecord \(ckRecord.recordID.recordName)")
                localObject.merge(with: ckRecord)
            } else {
                logger.info("Updating \(ckRecord.recordType) \(localObject.objectID.uriRepresentation().path) from CKRecord \(ckRecord.recordID.recordName)")
                let keysPendingUpdate = coordinator.transactionsPendingUpload().ckRecordKeysForChanges(involving: localObject.objectID)
                if !keysPendingUpdate.isEmpty {
                    logger.info("Excluding key from update due to pending upload: \(keysPendingUpdate.joined(separator: ", "))")
                }
                localObject.update(from: ckRecord, excluding: keysPendingUpdate)
            }
            localObject.resolveMergeConflicts()
        } else {
            logger.info("Creating new \(ckRecord.recordType) with record name \(ckRecord.recordID.recordName)")

            guard let type = orderedTypesToSync.first(where: { $0.ckRecordType == ckRecord.recordType }) else {
                logger.error("No type corresponding to \(ckRecord.recordType) found")
                return
            }
            type.createObject(from: ckRecord, in: syncContext)
        }
    }
}
