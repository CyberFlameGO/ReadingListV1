import CloudKit
import CoreData
import Logging

extension ListItem: CKRecordRepresentable {
    static let ckRecordType = "ListItem"
    static let allCKRecordKeys = ListItemCKRecordKey.allCases.map(\.rawValue)
    @NSManaged var ckRecordEncodedSystemFields: Data?
    @NSManaged var remoteIdentifier: String

    static func matchCandidateItemForRemoteRecord(_ record: CKRecord) -> NSPredicate {
        guard record.recordType == ckRecordType else {
            logger.error("Attempted to match a CKRecord of type \(record.recordType) to a ListItem")
            return NSPredicate(boolean: false)
        }
        guard let bookReference = record[.book] as? CKRecord.Reference else {
            logger.error("No book reference on a ListItem")
            return NSPredicate(boolean: false)
        }
        guard let listReference = record[.list] as? CKRecord.Reference else {
            logger.error("No list reference on a ListItem")
            return NSPredicate(boolean: false)
        }
        return NSCompoundPredicate(andPredicateWithSubpredicates: [
            NSPredicate(format: "\(#keyPath(ListItem.book)).\(SyncConstants.remoteIdentifierKeyPath) == %@", bookReference.recordID.recordName),
            NSPredicate(format: "\(#keyPath(ListItem.list)).\(SyncConstants.remoteIdentifierKeyPath) == %@", listReference.recordID.recordName)
        ])
    }

    func getValue(for key: String) -> CKRecordValueProtocol? {
        guard let ckRecordKey = ListItemCKRecordKey(rawValue: key) else { return nil }
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
        guard let key = ListItemCKRecordKey(rawValue: ckRecordKey) else { return }
        switch key {
        case .sort:
            sort = value?.asInt32 ?? 0
        case .book:
            guard let reference = value as? CKRecord.Reference else { return }
            let request = Book.fetchRequest()
            request.fetchLimit = 1
            request.predicate = NSPredicate(format: "%K == %@", SyncConstants.remoteIdentifierKeyPath, reference.recordID.recordName)
            guard let matchingBook = try! managedObjectContext!.fetch(request).first else {
                logger.info("No book found with record name \(reference.recordID.recordName) for ListItem")
                return
            }
            setValue(matchingBook, forKey: #keyPath(ListItem.book))
        case .list:
            guard let reference = value as? CKRecord.Reference else { return }
            let request = List.fetchRequest()
            request.fetchLimit = 1
            request.predicate = NSPredicate(format: "%K == %@", SyncConstants.remoteIdentifierKeyPath, reference.recordID.recordName)
            guard let matchingList = try! managedObjectContext!.fetch(request).first else {
                logger.info("No List found with record name \(reference.recordID.recordName) for ListItem")
                return
            }
            setValue(matchingList, forKey: #keyPath(ListItem.list))
        }
    }

    func localPropertyKeys(forCkRecordKey ckRecordKey: String) -> [String] {
        guard let ckKey = ListItemCKRecordKey(rawValue: ckRecordKey) else { return [] }
        return ckKey.localKeys()
    }

    func ckRecordKey(forLocalPropertyKey localPropertyKey: String) -> String? {
        return ListItemCKRecordKey.from(coreDataKey: localPropertyKey)?.rawValue
    }
}

struct ListItemRecordName {
    let fullRecordName: String
    let bookRemoteIdentifier: String
    let listRemoteIdentifier: String
    private let separator = "__"
    
    init(bookRemoteIdentifier: String, listRemoteIdentifier: String) {
        self.bookRemoteIdentifier = bookRemoteIdentifier
        self.listRemoteIdentifier = listRemoteIdentifier
        self.fullRecordName = "\(bookRemoteIdentifier)\(separator)\(listRemoteIdentifier)"
    }
    
    init?(listItemRecordName: String) {
        let splitComponents = listItemRecordName.components(separatedBy: separator)
        if splitComponents.count != 2 { return nil }
        self.fullRecordName = listItemRecordName
        self.bookRemoteIdentifier = splitComponents[0]
        self.listRemoteIdentifier = splitComponents[1]
    }
}

extension CKRecord {
    subscript (_ key: ListItemCKRecordKey) -> CKRecordValue? {
        get { return self.object(forKey: key.rawValue) }
        set { self.setObject(newValue, forKey: key.rawValue) }
    }
}

enum ListItemCKRecordKey: String, CaseIterable { //swiftlint:disable redundant_string_enum_value
    case book = "book"
    case list = "list"
    case sort = "sort" //swiftlint:enable redundant_string_enum_value

    static func from(coreDataKey: String) -> ListItemCKRecordKey? {
        switch coreDataKey {
        case #keyPath(ListItem.sort): return .sort
        case #keyPath(ListItem.book): return .book
        case #keyPath(ListItem.list): return .list
        default: return nil
        }
    }

    func localKeys() -> [String] {
        switch self {
        case .sort: return [#keyPath(ListItem.sort)]
        case .book: return [#keyPath(ListItem.book)]
        case .list: return [#keyPath(ListItem.list)]
        }
    }
}
