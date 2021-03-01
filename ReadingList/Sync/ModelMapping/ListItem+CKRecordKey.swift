import Foundation

extension ListItem {
    enum CKRecordKey: String, CaseIterable { //swiftlint:disable redundant_string_enum_value
        // REMEMBER to update the record schema version if we adjust this mapping
        case book = "book"
        case list = "list"
        case sort = "sort" //swiftlint:enable redundant_string_enum_value

        static func from(coreDataKey: String) -> CKRecordKey? {
            switch coreDataKey {
            case #keyPath(ListItem.sort): return .sort
            case #keyPath(ListItem.book): return .book
            case #keyPath(ListItem.list): return .list
            default: return nil
            }
        }
    }
}
