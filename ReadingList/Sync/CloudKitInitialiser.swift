import Foundation
import CloudKit
import PersistedPropertyWrapper
import Logging

class CloudKitInitialiser {
    private let cloudOperationQueue: ConcurrentCKQueue
    weak var coordinator: SyncCoordinator?

    init(cloudOperationQueue: ConcurrentCKQueue) {
        self.cloudOperationQueue = cloudOperationQueue
    }

    @Persisted("SyncEngine_CustomZoneCreated", defaultValue: false)
    private var createdCustomZone: Bool

    @Persisted("SyncEngine_PrivateSubscriptionKey", defaultValue: false)
    private var createdPrivateSubscription: Bool

    static let privateSubscriptionId = "\(SyncConstants.zoneID.zoneName).subscription"

    func prepareCloudEnvironment(completion: @escaping () -> Void) {
        DispatchQueue.global(qos: .userInitiated).async {
            self.createCustomZoneIfNeeded()
            self.cloudOperationQueue.operationQueue.waitUntilAllOperationsAreFinished()
            guard self.createdCustomZone else { return }

            self.createPrivateSubscriptionsIfNeeded()
            self.cloudOperationQueue.operationQueue.waitUntilAllOperationsAreFinished()
            guard self.createdPrivateSubscription else { return }

            completion()
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
                logger.error("Failed to create custom CloudKit zone: \(String(describing: error))")
                guard let ckError = error as? CKError else {
                    self.coordinator?.handleUnexpectedResponse()
                    return
                }
                if let retryAfter = ckError.retryAfterSeconds {
                    self.cloudOperationQueue.suspend()
                    self.createCustomZoneIfNeeded()
                    DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + retryAfter) {
                        self.cloudOperationQueue.resume()
                    }
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
                logger.error("Failed to check for custom zone existence: \(String(describing: error))")

                guard let ckError = error as? CKError else {
                    self.coordinator?.handleUnexpectedResponse()
                    return
                }

                if let retryAfter = ckError.retryAfterSeconds {
                    self.cloudOperationQueue.suspend()
                    self.checkCustomZone()
                    DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + retryAfter) {
                        self.cloudOperationQueue.resume()
                    }
                } else {
                    logger.error("Irrecoverable error when fetching custom zone, assuming it doesn't exist: \(String(describing: error))")

                    DispatchQueue.main.async {
                        self.createdCustomZone = false
                        self.createCustomZoneIfNeeded()
                    }
                }
            } else if ids == nil || ids!.isEmpty {
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
                logger.error("Failed to create private CloudKit subscription: \(String(describing: error))")
                guard let ckError = error as? CKError else {
                    self.coordinator?.handleUnexpectedResponse()
                    return
                }

                if let retryAfter = ckError.retryAfterSeconds {
                    self.cloudOperationQueue.suspend()
                    self.checkCustomZone()
                    DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + retryAfter) {
                        self.createPrivateSubscriptionsIfNeeded()
                    }
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
                logger.error("Failed to check for private zone subscription existence: \(String(describing: error))")
                guard let ckError = error as? CKError else {
                    self.coordinator?.handleUnexpectedResponse()
                    return
                }

                if let retryAfter = ckError.retryAfterSeconds {
                    self.cloudOperationQueue.suspend()
                    self.checkCustomZone()
                    DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + retryAfter) {
                        self.checkSubscription()
                    }
                } else {
                    logger.error("Irrecoverable error when fetching private zone subscription, assuming it doesn't exist: \(String(describing: error))")

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
