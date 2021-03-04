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

    func addBlock(_ block: @escaping () -> Void) {
        operationQueue.addOperation(BlockOperation(block: block))
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
}
