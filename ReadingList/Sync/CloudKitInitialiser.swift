import Foundation
import CloudKit
import PersistedPropertyWrapper
import Logging

class CloudKitInitialiser {
    private let cloudOperationQueue: ConcurrentCKQueue
    weak var coordinator: SyncCoordinator!

    init(cloudOperationQueue: ConcurrentCKQueue) {
        self.cloudOperationQueue = cloudOperationQueue
    }

    @Persisted("SyncEngine_CustomZoneCreated", defaultValue: false)
    private var createdCustomZone: Bool

    @Persisted("SyncEngine_PrivateSubscriptionKey", defaultValue: false)
    private var createdPrivateSubscription: Bool

    @Persisted("SyncEngine_UserRecordName")
    var userRecordName: String?

    static let privateSubscriptionId = "\(SyncConstants.zoneID.zoneName).subscription"

    func prepareCloudEnvironment(completion: @escaping () -> Void) {
        DispatchQueue.global(qos: .userInitiated).async {
            self.verifyUserRecordID()

            self.createCustomZoneIfNeeded()
            self.cloudOperationQueue.operationQueue.waitUntilAllOperationsAreFinished()
            guard self.createdCustomZone else { return }

            self.createPrivateSubscriptionsIfNeeded()
            self.cloudOperationQueue.operationQueue.waitUntilAllOperationsAreFinished()
            guard self.createdPrivateSubscription else { return }

            completion()
        }
    }

    func verifyUserRecordID() {
        fetchUserRecordID { recordID, error in
            if let recordID = recordID {
                if let userRecordName = self.userRecordName, recordID.recordName != userRecordName {
                    logger.error("User record was previous stored as \(userRecordName), but is now \(recordID.recordName)")
                    self.coordinator.disableSync(reason: .userAccountChanged)
                } else {
                    logger.info("User record name: \(recordID.recordName)")
                    self.userRecordName = recordID.recordName
                }
            } else if let error = error {
                logger.error("Unhandled error fetching user record ID \(error)")
                if !self.handleCloudPreparationError(error, rerunOperation: self.verifyUserRecordID) {
                    self.coordinator.stop()
                }
            } else {
                self.coordinator.stopSyncDueToError(
                    .unexpectedResponse("Fetch UserRecordID response had no success or failure arguments")
                )
            }
        }
    }

    func fetchUserRecordID(completion: @escaping (CKRecord.ID?, Error?) -> Void) {
        let operation = BlockOperation {
            let dispatchGroup = DispatchGroup()
            dispatchGroup.enter()
            // We aren't provided an Operation based way to fetch the user record ID, so wrap it in a Block Operation,
            // and use a DispatchGroup to block that block until the callback has been run.
            CKContainer.default().fetchUserRecordID {
                completion($0, $1)
                dispatchGroup.leave()
            }
            dispatchGroup.wait()
        }

        // So that user account verifications can jump the queue
        operation.queuePriority = .high
        cloudOperationQueue.addOperation(operation)
    }

    private func handleCloudPreparationError(_ error: Error, rerunOperation: () -> Void) -> Bool {
        guard let ckError = error as? CKError else {
            self.coordinator.stopSyncDueToError(.unexpectedErrorType(error))
            return true
        }

        logger.error("Operation failed with code: \(ckError.code.name)")
        if ckError.code == .userDeletedZone {
            coordinator.disableSync(reason: .cloudDataDeleted)
            return true
        } else if ckError.code == .notAuthenticated {
            coordinator.stop()
            return true
        } else if let retryAfter = ckError.retryAfterSeconds {
            self.cloudOperationQueue.suspend()
            rerunOperation()
            DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + retryAfter) {
                self.cloudOperationQueue.resume()
            }
            return true
        } else {
            return false
        }
    }

    private func createCustomZoneIfNeeded() {
        guard !createdCustomZone else {
            logger.info("Already have custom zone, skipping creation but checking if zone really exists")
            checkCustomZone()
            return
        }
        logger.info("Creating CloudKit zone \(SyncConstants.zoneID)")

        let zone = CKRecordZone(zoneID: SyncConstants.zoneID)
        let operation = CKModifyRecordZonesOperation(recordZonesToSave: [zone], recordZoneIDsToDelete: nil)

        operation.modifyRecordZonesCompletionBlock = { [weak self] _, _, error in
            guard let self = self else { return }

            if let error = error {
                if !self.handleCloudPreparationError(error, rerunOperation: self.createCustomZoneIfNeeded) {
                    self.coordinator.stop()
                }
            } else {
                logger.info("Zone created successfully")
                self.createdCustomZone = true
            }
        }

        operation.qualityOfService = .userInitiated
        cloudOperationQueue.addOperation(operation)
    }

    private func checkCustomZone() {
        let operation = CKFetchRecordZonesOperation(recordZoneIDs: [SyncConstants.zoneID])
        operation.fetchRecordZonesCompletionBlock = { [weak self] ids, error in
            guard let self = self else { return }

            if let error = error {
                if (error as? CKError)?.code == .zoneNotFound {
                    logger.info("Zone was not found; creating instead")
                    self.createdCustomZone = false
                    self.createCustomZoneIfNeeded()
                } else if !self.handleCloudPreparationError(error, rerunOperation: self.checkCustomZone) {
                    logger.error("Unhandled error when fetching custom zone, assuming it doesn't exist")
                    self.createdCustomZone = false
                    self.createCustomZoneIfNeeded()
                }
            } else if ids?.isEmpty != false {
                logger.error("Custom zone reported as existing, but it doesn't exist. Creating.")
                self.createdCustomZone = false
                self.createCustomZoneIfNeeded()
            }
        }

        operation.qualityOfService = .userInitiated
        cloudOperationQueue.addOperation(operation)
    }

    private func createPrivateSubscriptionsIfNeeded() {
        guard !createdPrivateSubscription else {
            logger.info("Already subscribed to private database changes, skipping subscription but checking if it really exists")
            checkSubscription()
            return
        }

        let subscription = CKRecordZoneSubscription(zoneID: SyncConstants.zoneID, subscriptionID: Self.privateSubscriptionId)

        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo

        let operation = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription], subscriptionIDsToDelete: nil)
        operation.qualityOfService = .userInitiated

        operation.modifySubscriptionsCompletionBlock = { [weak self] _, _, error in
            guard let self = self else { return }

            if let error = error {
                if !self.handleCloudPreparationError(error, rerunOperation: self.createPrivateSubscriptionsIfNeeded) {
                    self.coordinator.stop()
                }
            } else {
                logger.info("Private subscription created successfully")
                self.createdPrivateSubscription = true
            }
        }

        cloudOperationQueue.addOperation(operation)
    }

    private func checkSubscription() {
        let operation = CKFetchSubscriptionsOperation(subscriptionIDs: [Self.privateSubscriptionId])

        operation.fetchSubscriptionCompletionBlock = { [weak self] ids, error in
            guard let self = self else { return }

            if let error = error {
                if !self.handleCloudPreparationError(error, rerunOperation: self.checkSubscription) {
                    logger.error("Error when fetching private zone subscription, assuming it doesn't exist")
                    DispatchQueue.main.async {
                        self.createdPrivateSubscription = false
                        self.createPrivateSubscriptionsIfNeeded()
                    }
                }
            } else if ids == nil || ids!.isEmpty {
                logger.error("Private subscription reported as existing, but it doesn't exist. Creating.")

                DispatchQueue.main.async {
                    self.createdPrivateSubscription = false
                    self.createPrivateSubscriptionsIfNeeded()
                }
            }
        }

        operation.qualityOfService = .userInitiated
        cloudOperationQueue.addOperation(operation)
    }
}
