import Foundation
import CloudKit

extension CKModifyRecordsOperation {
    /// Whether the operation is devoid of any CKRecords or CKRecord IDs to upload or delete.
    var isEmpty: Bool {
        if let recordsToSave = recordsToSave, !recordsToSave.isEmpty {
            return false
        }
        if let recordIDsToDelete = recordIDsToDelete, !recordIDsToDelete.isEmpty {
            return false
        }
        return true
    }
}
