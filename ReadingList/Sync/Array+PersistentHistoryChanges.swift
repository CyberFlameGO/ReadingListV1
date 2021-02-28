import Foundation
import CoreData

extension Array where Element == NSPersistentHistoryTransaction {
    func changes(involving objectID: NSManagedObjectID) -> [NSPersistentHistoryChange] {
        self.compactMap { $0.changes }
            .flatMap { $0 }
            .filter { $0.changeType == .update && $0.changedObjectID == objectID }
    }

    func ckRecordKeysForChanges(involving objectID: NSManagedObjectID) -> [String] {
        return self.changes(involving: objectID)
            .compactMap { $0.updatedProperties }
            .flatMap { $0 }
            .map { $0.name }
            .distinct()
    }
}

extension Array where Element == NSPersistentHistoryChange {
    /// A description suitable for debugging
    func description() -> String {
        return map {
            var result = "\($0.changeType.description) \($0.changedObjectID.uriRepresentation().path)"
            if $0.changeType == .update {
                result += " [\($0.updatedProperties?.map(\.name).joined(separator: ", ") ?? "")]"
            }
            return result
        }.joined(separator: "\n")
    }
}
