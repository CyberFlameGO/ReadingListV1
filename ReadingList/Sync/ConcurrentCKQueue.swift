import Foundation
import CloudKit
import CocoaLumberjackSwift
import os.log

class ConcurrentCKQueue {
    init() { }

    private let cloudQueue = DispatchQueue(label: "SyncEngine.CloudQueue", qos: .userInitiated)
    private let container = CKContainer.default()
    private lazy var privateDatabase = container.privateCloudDatabase

    /// A single-concurrent-operation queue used to manage cloud-interation operations.
    lazy var operationQueue: OperationQueue = {
        let operationQueue = OperationQueue()
        operationQueue.underlyingQueue = cloudQueue
        operationQueue.name = "SyncEngine.Cloud"
        operationQueue.maxConcurrentOperationCount = 1
        return operationQueue
    }()

    func addOperation(_ operation: CKDatabaseOperation, qos: QualityOfService = .userInitiated) {
        operation.database = privateDatabase
        operation.qualityOfService = qos
        operationQueue.addOperation(operation)
    }

    func suspend() {
        operationQueue.isSuspended = true
    }

    func resume() {
        operationQueue.isSuspended = false
    }

    func suspendCloudInterop(dueTo error: Error) -> Bool {
        guard let effectiveError = error as? CKError else { return false }
        guard let retryDelay = effectiveError.retryAfterSeconds else {
            DDLogError("Error is not recoverable")
            return false
        }

        DDLogError("Error is recoverable. Will retry after \(retryDelay) seconds")
        self.operationQueue.isSuspended = true
        // TODO Wrong queue, really
        cloudQueue.asyncAfter(deadline: .now() + retryDelay) {
            self.operationQueue.isSuspended = false
        }

        return true
    }
}
