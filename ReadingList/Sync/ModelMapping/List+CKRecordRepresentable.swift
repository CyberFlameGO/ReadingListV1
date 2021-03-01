import CloudKit
import CoreData
import Logging

extension List: CKRecordRepresentable {
    static let ckRecordType = "List"
    static let allCKRecordKeys = List.CKRecordKey.allCases.map(\.rawValue)
    @NSManaged var ckRecordEncodedSystemFields: Data?

    static func matchCandidateItemForRemoteRecord(_ record: CKRecord) -> NSPredicate {
        guard record.recordType == ckRecordType else {
            logger.critical("Attempted to match a CKRecord of type \(record.recordType) to a List")
            return NSPredicate(boolean: false)
        }
        guard let listName = record[List.CKRecordKey.name] as? String else { return NSPredicate(boolean: false) }
        return NSPredicate(format: "%K = %@", #keyPath(List.name), listName)
    }

    func getValue(for ckRecordKey: String) -> CKRecordValueProtocol? {
        guard let key = List.CKRecordKey(rawValue: ckRecordKey) else { return nil }
        switch key {
        case .name: return name as NSString
        case .order: return order.rawValue as NSNumber
        case .sort: return sort as NSNumber
        }
    }

    func setValue(_ value: CKRecordValueProtocol?, for ckRecordKey: String) {
        guard let key = List.CKRecordKey(rawValue: ckRecordKey) else { return }
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

    func ckRecordKey(forLocalPropertyKey localPropertyKey: String) -> String? {
        return List.CKRecordKey.from(coreDataKey: localPropertyKey)?.rawValue
    }
}
