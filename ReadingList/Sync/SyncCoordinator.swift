import Foundation
import CloudKit
import CoreData
import Logging
import Combine
import UIKit
import Reachability

@available(iOS 13.0, *)
final class SyncCoordinator {

    private let persistentStoreCoordinator: NSPersistentStoreCoordinator
    let typesToSync: [CKRecordRepresentable.Type]
    let downstreamProcessor: DownstreamSyncProcessor
    let upstreamProcessor: UpstreamSyncProcessor

    init(persistentStoreCoordinator: NSPersistentStoreCoordinator, orderedTypesToSync: [CKRecordRepresentable.Type]) {
        self.persistentStoreCoordinator = persistentStoreCoordinator
        typesToSync = orderedTypesToSync
        syncContext = Self.buildSyncContext(storeCoordinator: persistentStoreCoordinator)
        downstreamProcessor = DownstreamSyncProcessor(syncContext: syncContext, types: orderedTypesToSync, cloudOperationQueue: cloudOperationQueue)
        upstreamProcessor = UpstreamSyncProcessor(syncContext: syncContext, cloudOperationQueue: cloudOperationQueue, types: orderedTypesToSync)

        downstreamProcessor.coordinator = self
        upstreamProcessor.coordinator = self
        cloudKitInitialiser.coordinator = self
    }

    private let syncContext: NSManagedObjectContext
    private let cloudOperationQueue = ConcurrentCKQueue()
    private lazy var cloudKitInitialiser = CloudKitInitialiser(cloudOperationQueue: cloudOperationQueue)
    private var cancellables = Set<AnyCancellable>()

    private lazy var reachability: Reachability? = {
        do {
            return try Reachability()
        } catch {
            logger.error("Reachability could not be initialized: \(error.localizedDescription)")
            return nil
        }
    }()

    private static func buildSyncContext(storeCoordinator: NSPersistentStoreCoordinator) -> NSManagedObjectContext {
        let context = NSManagedObjectContext(concurrencyType: .privateQueueConcurrencyType)
        context.persistentStoreCoordinator = storeCoordinator
        context.name = "SyncEngineContext"
        try! context.setQueryGenerationFrom(.current)
        // Ensure that other changes made to the store trump the changes made in this context, so that UI changes don't get overwritten
        // by sync chnages.
        context.mergePolicy = NSMergePolicy.mergeByPropertyObjectTrump
        return context
    }

    func start() {
        logger.info("SyncCoordinator starting")
        self.cloudOperationQueue.resume()
        self.cloudKitInitialiser.prepareCloudEnvironment { [weak self] in
            guard let self = self else { return }
            logger.info("Cloud environment prepared")

            self.downstreamProcessor.enqueueFetchRemoteChanges()
            self.upstreamProcessor.start(storeCoordinator: self.persistentStoreCoordinator)

            NotificationCenter.default.publisher(for: .CKAccountChanged)
                .sink { [weak self] _ in
                    guard let self = self else { return }
                    logger.info("CKAccountChanged; stopping SyncCoordinator and disabling iCloud Sync")
                    self.disableSync()
                }.store(in: &self.cancellables)

            // Monitoring the network reachabiity will allow us to automatically re-do work when network connectivity resumes
            self.startNetworkMonitoring()
            NotificationCenter.default.publisher(for: .reachabilityChanged)
                .sink(receiveValue: self.networkConnectivityDidChange)
                .store(in: &self.cancellables)
        }
    }

    func transactionsPendingUpload() -> [NSPersistentHistoryTransaction] {
        upstreamProcessor.localTransactionsPendingPushCompletion
    }

    func stop() {
        logger.info("Stopping sync coordinator")
        cancellables.forEach {
            $0.cancel()
        }
        cloudOperationQueue.suspend()
        cloudOperationQueue.cancelAll()
    }
    
    var isRunning: Bool {
        !cloudOperationQueue.operationQueue.isSuspended
    }

    func disableSync() {
        stop()
        GeneralSettings.iCloudSyncEnabled = false
    }

    func handleUnexpectedResponse() {
        logger.critical("Stopping SyncCoordinator due to unexpected response")
        UserEngagement.logError(SyncCoordinatorError.unexpectedResponse)
        stop()
    }
    
    func disableSyncDueOutOfDateLocalAppVersion() {
        logger.error("Stopping SyncCoordinator because the server contains data which is from a newer version of the app")
        stop()
        // TODO Consider caching this info and erasing upon upgrade, so we don't keep attempting to get data on every startup (maybe it doesn't matter)
        // TODO Expose this info the UI 
    }

    func forceFullResync() {
        cloudOperationQueue.cancelAll()
        cloudOperationQueue.addBlock {
            self.syncContext.perform {
                self.eraseSyncMetadata()

                self.downstreamProcessor.resetChangeTracking()
                self.downstreamProcessor.enqueueFetchRemoteChanges()

                self.upstreamProcessor.enqueueUploadOperations()
            }
        }
    }

    func eraseSyncMetadata() {
        let syncHelper = SyncResetter(managedObjectContext: self.syncContext, entityTypes: self.typesToSync.map { $0.entity() })
        syncHelper.eraseSyncMetadata()
    }

    func enqueueFetchRemoteChanges(completion: ((UIBackgroundFetchResult) -> Void)? = nil) {
        self.downstreamProcessor.enqueueFetchRemoteChanges(completion: completion)
    }

    func requestFetch(for recordIDs: [CKRecord.ID]) {
        self.downstreamProcessor.fetchRecords(recordIDs)
    }

    func status() -> SyncStatus {
        var totalCounts = [String: Int]()
        var uploadedCounts = [String: Int]()
        syncContext.performAndWait {
            for type in typesToSync {
                let fetchRequest = type.fetchRequest()
                let countResult = try! syncContext.count(for: fetchRequest)
                totalCounts[type.ckRecordType] = countResult

                fetchRequest.predicate = NSPredicate(format: "ckRecordEncodedSystemFields != nil")
                let uploadedCount = try! syncContext.count(for: fetchRequest)
                uploadedCounts[type.ckRecordType] = uploadedCount
            }
        }

        return SyncStatus(
            objectCountByEntityName: totalCounts,
            uploadedObjectCount: uploadedCounts,
            lastProcessedLocalTransaction: upstreamProcessor.latestConfirmedUploadedTransaction
        )
    }

    // MARK: - Network Monitoring
    private func startNetworkMonitoring() {
        guard let reachability = reachability else { return }
        do {
            try reachability.startNotifier()
        } catch {
            logger.error("Error starting reachability notifier: \(error.localizedDescription)")
        }
    }

    private func networkConnectivityDidChange(_ notification: Notification) {
        guard let reachability = self.reachability else { preconditionFailure("Reachability was nil in a networkChange callback") }
        logger.debug("Network connectivity changed to \(reachability.connection.description)")
        if reachability.connection == .unavailable {
            logger.info("Suspending operation queue due to lack of network connectivity")
            self.cloudOperationQueue.suspend()
        } else {
            logger.info("Resuming operation queue due to available network connectivity")
            self.cloudOperationQueue.resume()
            self.downstreamProcessor.enqueueFetchRemoteChanges()
        }
    }
}
