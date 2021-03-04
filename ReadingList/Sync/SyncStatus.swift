import Foundation
import CoreData
import CloudKit

struct SyncStatus {
    let objectCountByEntityName: [String: Int]
    let uploadedObjectCount: [String: Int]
    let lastProcessedLocalTransaction: Date?
}
