import CoreData
import Foundation

extension NSPersistentHistoryChangeType {
    var description: String {
        switch self {
        case .insert: return "Insert"
        case .update: return "Update"
        case .delete: return "Delete"
        @unknown default: return "Unknown"
        }
    }
}
