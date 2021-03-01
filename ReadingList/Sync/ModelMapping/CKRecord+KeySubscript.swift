import Foundation
import CloudKit

extension CKRecord {
    subscript<Key> (_ key: Key) -> CKRecordValue? where Key: RawRepresentable, Key.RawValue == String {
        get { return self.object(forKey: key.rawValue) }
        set { self.setObject(newValue, forKey: key.rawValue) }
    }
}
