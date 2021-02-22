import CloudKit
import CoreData
import Logging

extension List: CKRecordRepresentable {
    static let ckRecordType = "List"
    static let allCKRecordKeys = ListCKRecordKey.allCases.map(\.rawValue)
    @NSManaged var ckRecordEncodedSystemFields: Data?

    static func matchCandidateItemForRemoteRecord(_ record: CKRecord) -> NSPredicate {
        guard record.recordType == ckRecordType else {
            logger.critical("Attempted to match a CKRecord of type \(record.recordType) to a List")
            return NSPredicate(boolean: false)
        }
        guard let listName = record[ListCKRecordKey.name] as? String else { return NSPredicate(boolean: false) }
        return NSPredicate(format: "%K = %@", #keyPath(List.name), listName)
    }

    func getValue(for ckRecordKey: String) -> CKRecordValueProtocol? {
        guard let key = ListCKRecordKey(rawValue: ckRecordKey) else { return nil }
        switch key {
        case .name: return name as NSString
        case .order: return order.rawValue as NSNumber
        case .sort: return sort as NSNumber
        }
    }

    func setValue(_ value: CKRecordValueProtocol?, for ckRecordKey: String) {
        guard let key = ListCKRecordKey(rawValue: ckRecordKey) else { return }
        switch key {
        case .name:
            if let nameValue = value as? String {
                name = nameValue
            }
        case .order:
            if let orderNumber = value?.asInt16,
               let bookSortOrder = BookSort(rawValue: orderNumber) {
                order = bookSortOrder
            }
        case .sort:
            if let sortNumber = value?.asInt32 {
                sort = sortNumber
            }
        }
    }

    func localPropertyKeys(forCkRecordKey ckRecordKey: String) -> [String] {
        guard let ckKey = ListCKRecordKey(rawValue: ckRecordKey) else { return [] }
        return ckKey.localKeys()
    }

    func ckRecordKey(forLocalPropertyKey localPropertyKey: String) -> String? {
        return ListCKRecordKey.from(coreDataKey: localPropertyKey)?.rawValue
    }
}

extension CKRecord {
    subscript (_ key: ListCKRecordKey) -> CKRecordValue? {
        get { return self.object(forKey: key.rawValue) }
        set { self.setObject(newValue, forKey: key.rawValue) }
    }
}

enum ListCKRecordKey: String, CaseIterable { //swiftlint:disable redundant_string_enum_value
    // REMEMBER to update the record schema version if we adjust this mapping
    case name = "name"
    case sort = "sort"
    case order = "order" //swiftlint:enable redundant_string_enum_value

    static func from(coreDataKey: String) -> ListCKRecordKey? {
        switch coreDataKey {
        case #keyPath(List.name): return .name
        case #keyPath(List.sort): return .sort
        case #keyPath(List.order): return .order
        default: return nil
        }
    }

    func localKeys() -> [String] {
        switch self {
        case .name: return [#keyPath(List.name)]
        case .sort: return [#keyPath(List.sort)]
        case .order: return [#keyPath(List.order)]
        }
    }
}
