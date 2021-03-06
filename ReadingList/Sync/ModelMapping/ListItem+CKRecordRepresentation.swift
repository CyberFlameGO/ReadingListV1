import CloudKit
import CoreData
import Logging

extension ListItem: CKRecordRepresentable {
    static let ckRecordType = "ListItem"
    static let allCKRecordKeys = ListItem.CKRecordKey.allCases.map(\.rawValue)
    @NSManaged var ckRecordEncodedSystemFields: Data?
    @NSManaged var remoteIdentifier: String

    static func matchCandidateItemForRemoteRecord(_ record: CKRecord) -> NSPredicate {
        guard record.recordType == ckRecordType else {
            logger.error("Attempted to match a CKRecord of type \(record.recordType) to a ListItem")
            return NSPredicate(boolean: false)
        }
        guard let bookReference = record[ListItem.CKRecordKey.book] as? CKRecord.Reference else {
            logger.error("No book reference on a ListItem")
            return NSPredicate(boolean: false)
        }
        guard let listReference = record[ListItem.CKRecordKey.list] as? CKRecord.Reference else {
            logger.error("No list reference on a ListItem")
            return NSPredicate(boolean: false)
        }
        return NSCompoundPredicate(andPredicateWithSubpredicates: [
            NSPredicate(format: "\(#keyPath(ListItem.book)).\(SyncConstants.remoteIdentifierKeyPath) == %@", bookReference.recordID.recordName),
            NSPredicate(format: "\(#keyPath(ListItem.list)).\(SyncConstants.remoteIdentifierKeyPath) == %@", listReference.recordID.recordName)
        ])
    }

    func getValue(for key: String) -> CKRecordValueProtocol? {
        guard let ckRecordKey = ListItem.CKRecordKey(rawValue: key) else { return nil }
        switch ckRecordKey {
        case .sort: return sort
        case .book:
            guard let bookIdentifier = book?.remoteIdentifier else { return nil }
            return CKRecord.Reference(recordID: CKRecord.ID(recordName: bookIdentifier, zoneID: SyncConstants.zoneID), action: .deleteSelf)
        case .list:
            guard let listIdentifier = list?.remoteIdentifier else { return nil }
            return CKRecord.Reference(recordID: CKRecord.ID(recordName: listIdentifier, zoneID: SyncConstants.zoneID), action: .deleteSelf)
        }
    }

    func setValue(_ value: CKRecordValueProtocol?, for ckRecordKey: String) {
        guard let key = ListItem.CKRecordKey(rawValue: ckRecordKey) else { return }
        switch key {
        case .sort:
            sort = value?.asInt32 ?? 0
        case .book:
            guard let reference = value as? CKRecord.Reference else { return }
            let request = Book.fetchRequest(in: managedObjectContext!)
            request.fetchLimit = 1
            request.predicate = NSPredicate(format: "%K == %@", SyncConstants.remoteIdentifierKeyPath, reference.recordID.recordName)
            guard let matchingBook = try! managedObjectContext!.fetch(request).first else {
                logger.info("No book found with record name \(reference.recordID.recordName) for ListItem")
                return
            }
            setValue(matchingBook, forKey: #keyPath(ListItem.book))
        case .list:
            guard let reference = value as? CKRecord.Reference else { return }
            let request = List.fetchRequest(in: managedObjectContext!)
            request.fetchLimit = 1
            request.predicate = NSPredicate(format: "%K == %@", SyncConstants.remoteIdentifierKeyPath, reference.recordID.recordName)
            guard let matchingList = try! managedObjectContext!.fetch(request).first else {
                logger.info("No List found with record name \(reference.recordID.recordName) for ListItem")
                return
            }
            setValue(matchingList, forKey: #keyPath(ListItem.list))
        }
    }

    func ckRecordKey(forLocalPropertyKey localPropertyKey: String) -> String? {
        return ListItem.CKRecordKey.from(coreDataKey: localPropertyKey)?.rawValue
    }
}
