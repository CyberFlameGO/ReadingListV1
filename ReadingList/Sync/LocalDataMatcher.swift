import Foundation
import CloudKit
import CoreData

struct LocalDataMatcher {
    let syncContext: NSManagedObjectContext
    let types: [CKRecordRepresentable.Type]

    func lookupLocalObject(for remoteRecord: CKRecord) -> CKRecordRepresentable? {
        guard let type = types.first(where: { $0.ckRecordType == remoteRecord.recordType }) else { return nil }

        let recordName = remoteRecord.recordID.recordName
        if let localItem = lookupLocalObject(ofType: type, withIdentifier: recordName) {
            logger.debug("Found local \(type.ckRecordType) with remote identifier \(recordName)")
            return localItem
        }

        let localIdLookup = type.fetchRequest()
        localIdLookup.fetchLimit = 1
        localIdLookup.predicate = NSCompoundPredicate(
            andPredicateWithSubpredicates: [
                NSPredicate(format: "%K == NULL", SyncConstants.remoteIdentifierKeyPath),
                type.matchCandidateItemForRemoteRecord(remoteRecord)
            ]
        )

        if let localItem = (try! syncContext.fetch(localIdLookup)).first as? CKRecordRepresentable {
            logger.debug("Found candidate local \(type.ckRecordType) for remote record \(recordName) using metadata")
            return localItem
        }

        logger.debug("No local \(type.ckRecordType) found for remote record \(recordName)")
        return nil
    }

    func lookupLocalObject(ofType type: CKRecordRepresentable.Type, withIdentifier recordName: String) -> CKRecordRepresentable? {
        let fetchRequest = type.fetchRequest()
        fetchRequest.predicate = type.remoteIdentifierPredicate(recordName)
        fetchRequest.fetchLimit = 1
        return (try! syncContext.fetch(fetchRequest)).first as? CKRecordRepresentable
    }
}
