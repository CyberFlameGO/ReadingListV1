import Foundation
import CoreData

struct SyncResetter {
    let entityTypes: [NSEntityDescription]
    let managedObjectContext: NSManagedObjectContext

    init(managedObjectContext: NSManagedObjectContext, entityTypes: [NSEntityDescription]) {
        self.entityTypes = entityTypes
        self.managedObjectContext = managedObjectContext
    }

    func eraseSyncMetadata() {
        for entity in entityTypes {
            let batchUpdate = NSBatchUpdateRequest(entity: entity)
            batchUpdate.resultType = .updatedObjectIDsResultType
            // We need to use the keypath on a concrete CKRecordRepresentable so that it is @objc visible
            batchUpdate.propertiesToUpdate = [
                #keyPath(Book.ckRecordEncodedSystemFields): NSExpression(forConstantValue: nil)
            ]

            let batchUpdateResults = try! managedObjectContext.execute(batchUpdate) as! NSBatchUpdateResult
            let objectIDs = batchUpdateResults.result as! [NSManagedObjectID]
            for objectID in objectIDs {
                // Turn Managed Objects into Faults
                let managedObject = managedObjectContext.object(with: objectID)
                managedObjectContext.refresh(managedObject, mergeChanges: false)
            }
        }
    }
}
