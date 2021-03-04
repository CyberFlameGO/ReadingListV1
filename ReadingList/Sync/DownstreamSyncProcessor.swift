import Foundation
import CloudKit
import CoreData
import UIKit
import PersistedPropertyWrapper

class DownstreamSyncProcessor {
    weak var coordinator: SyncCoordinator!
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
                self.coordinator.handleUnexpectedResponse()
                return
            }
            if recordSchemaVersion > SyncConstants.recordSchemaVersion.rawValue {
                logger.error("CKRecord schema version was \(recordSchemaVersion) but the current schema version is \(SyncConstants.recordSchemaVersion.rawValue)")
                operation.cancel()
                self.coordinator.disableSyncDueOutOfDateLocalAppVersion()
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
                if let error = error {
                    logger.error("Error in recordZoneFetchCompletionBlock")
                    self.handleDownloadError(error)
                } else {
                    logger.info("Remote record fetch completed; commiting new change token")
                    processChanges(newToken: token)
                }
            }
        }

        operation.fetchRecordZoneChangesCompletionBlock = { [weak self] error in
            guard let self = self else { return }
            self.syncContext.performAndWait {
                if let error = error {
                    logger.error("Remote change fetch completed with error")
                    if let completion = completion {
                        logger.info("Calling UIBackgroundFetch completion handler with failure")
                        completion(.failed)
                    }
                    self.handleDownloadError(error)
                } else {
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
            self.syncContext.performAndWait {
                if let error = error {
                    self.handleDownloadError(error)
                } else {
                    guard let records = records else {
                        self.coordinator.handleUnexpectedResponse()
                        return
                    }
                    self.commitServerChangesToDatabase(with: Array(records.values), deletedRecordIDs: [])
                }
            }
        }

        logger.info("Fetching remote records with IDs \(recordIDs.map { $0.recordName }.joined(separator: ", "))")
        cloudOperationQueue.addOperation(operation)
    }

    private func handleDownloadError(_ error: Error) {
        guard let ckError = error as? CKError else {
            self.coordinator.handleUnexpectedResponse()
            return
        }

        logger.error("Handling CKError with code \(ckError.code.name)")
        if ckError.code == .operationCancelled {
            return
        } else if ckError.code == .changeTokenExpired {
            logger.warning("Change token expired, resetting token and trying again")
            self.remoteChangeToken = nil
            self.enqueueFetchRemoteChanges()
        } else if let retryDelay = ckError.retryAfterSeconds {
            logger.info("Instructed to delay for \(retryDelay) seconds: suspending operation queue")
            self.cloudOperationQueue.suspend()
            // Enqueue a fetch operation ready for when we unsuspend things
            self.enqueueFetchRemoteChanges()
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + retryDelay) {
                logger.info("Resuming operation queue")
                self.cloudOperationQueue.resume()
            }
        } else {
            logger.critical("Unhandled error response \(ckError)")
            self.coordinator.handleUnexpectedResponse()
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
        if let localObject = localDataMatcher.lookupLocalObject(for: ckRecord) {
            if localObject.ckRecordEncodedSystemFields == nil {
                logger.info("Merging \(ckRecord.recordType) \(localObject.objectID.uriRepresentation().path) from CKRecord \(ckRecord.recordID.recordName)")
                localObject.merge(with: ckRecord)
                // TODO we need to somehow enqueue this to be repushed. Setting the transaction author perhaps?
            } else {
                logger.info("Updating \(ckRecord.recordType) \(localObject.objectID.uriRepresentation().path) from CKRecord \(ckRecord.recordID.recordName)")
                let keysPendingUpdate = self.coordinator.transactionsPendingUpload().ckRecordKeysForChanges(involving: localObject.objectID)
                if !keysPendingUpdate.isEmpty {
                    logger.info("Excluding key from update due to pending upload: \(keysPendingUpdate.joined(separator: ", "))")
                }
                localObject.update(from: ckRecord, excluding: keysPendingUpdate)
            }
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
