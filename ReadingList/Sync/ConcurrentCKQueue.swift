import Foundation
import CloudKit
import Logging

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

    func addOperation(_ operation: Operation, qos: QualityOfService = .userInitiated) {
        if let ckDatabaseOperation = operation as? CKDatabaseOperation {
            ckDatabaseOperation.database = privateDatabase
        }
        operation.qualityOfService = qos
        operationQueue.addOperation(operation)
    }

    func addOperations(_ operations: [Operation], waitUntilFinished: Bool = false) {
        for operation in operations {
            if let ckDatabaseOperation = operation as? CKDatabaseOperation {
                ckDatabaseOperation.database = privateDatabase
            }
        }
        operationQueue.addOperations(operations, waitUntilFinished: waitUntilFinished)
    }

    func suspend() {
        operationQueue.isSuspended = true
    }

    func cancelAll() {
        operationQueue.cancelAllOperations()
    }

    func resume() {
        operationQueue.isSuspended = false
    }

    func suspendCloudInterop(dueTo error: Error) -> Bool {
        guard let effectiveError = error as? CKError else { return false }
        guard let retryDelay = effectiveError.retryAfterSeconds else {
            logger.error("Error is not recoverable")
            return false
        }

        logger.error("Error is recoverable. Will retry after \(retryDelay) seconds")
        self.operationQueue.isSuspended = true
        // TODO Wrong queue, really
        cloudQueue.asyncAfter(deadline: .now() + retryDelay) {
            self.operationQueue.isSuspended = false
        }

        return true
    }
}
