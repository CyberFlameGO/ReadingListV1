import Foundation

extension List {
    enum CKRecordKey: String, CaseIterable { //swiftlint:disable redundant_string_enum_value
        // REMEMBER to update the record schema version if we adjust this mapping
        case name = "name"
        case sort = "sort"
        case order = "order" //swiftlint:enable redundant_string_enum_value

        static func from(coreDataKey: String) -> CKRecordKey? {
            switch coreDataKey {
            case #keyPath(List.name): return .name
            case #keyPath(List.sort): return .sort
            case #keyPath(List.order): return .order
            default: return nil
            }
        }
    }
}
