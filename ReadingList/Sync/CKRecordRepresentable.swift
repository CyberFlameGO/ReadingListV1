import Foundation
import CloudKit
import CoreData
import os.log

struct SyncConstants {
    static let remoteIdentifierKeyPath = "remoteIdentifier"
    static let zoneID = CKRecordZone.ID(zoneName: "ReadingListZone", ownerName: CKCurrentUserDefaultName)
    static let recordSchemaVersionKey = "recordSchemaVersion"
    static let recordSchemaVersion: RecordSchemaVersion = .twoPointZeroPointZero
}

enum RecordSchemaVersion: Int {
    /// App version 2.0.0
    case twoPointZeroPointZero = 0
}

protocol CKRecordRepresentable: NSManagedObject {
    static var ckRecordType: String { get }
    static var allCKRecordKeys: [String] { get }
    var isDeleted: Bool { get }

    var remoteIdentifier: String { get set }
    var ckRecordEncodedSystemFields: Data? { get set }

    func localPropertyKeys(forCkRecordKey ckRecordKey: String) -> [String]
    func ckRecordKey(forLocalPropertyKey localPropertyKey: String) -> String?

    static func matchCandidateItemForRemoteRecord(_ record: CKRecord) -> NSPredicate

    func getValue(for key: String) -> CKRecordValueProtocol?
    func setValue(_ value: CKRecordValueProtocol?, for ckRecordKey: String)
}

extension CKRecordRepresentable {
    func getSystemFieldsRecord() -> CKRecord? {
        guard let systemFieldsData = ckRecordEncodedSystemFields else { return nil }
        return CKRecord(systemFieldsData: systemFieldsData)
    }

    func setSystemFields(_ ckRecord: CKRecord?) {
        ckRecordEncodedSystemFields = ckRecord?.encodedSystemFields()
    }

    static func remoteIdentifierPredicate(_ id: String) -> NSPredicate {
        return NSPredicate(format: "%K == %@", SyncConstants.remoteIdentifierKeyPath, id)
    }

    func buildCKRecord(ckRecordKeys: [String]? = nil) -> CKRecord {
        let ckRecord: CKRecord
        if let encodedSystemFields = ckRecordEncodedSystemFields, let ckRecordFromSystemFields = CKRecord(systemFieldsData: encodedSystemFields) {
            ckRecord = ckRecordFromSystemFields
        } else {
            let recordID = CKRecord.ID(recordName: remoteIdentifier, zoneID: SyncConstants.zoneID)
            ckRecord = CKRecord(recordType: Self.ckRecordType, recordID: recordID)
        }
        ckRecord[SyncConstants.recordSchemaVersionKey] = SyncConstants.recordSchemaVersion.rawValue

        let keysToStore: [String]
        if let changedCKRecordKeys = ckRecordKeys, !changedCKRecordKeys.isEmpty {
            keysToStore = changedCKRecordKeys.distinct()
        } else {
            keysToStore = Self.allCKRecordKeys
        }

        for key in keysToStore {
            ckRecord[key] = getValue(for: key)
        }
        return ckRecord
    }

    func setSystemFields(from ckRecord: CKRecord) {
        if remoteIdentifier != ckRecord.recordID.recordName {
            logger.error("Attempted to update local object with remoteIdentifier \(remoteIdentifier) from a CKRecord which has record name \(ckRecord.recordID.recordName)")
            fatalError("Attempted to update local object from CKRecord with different remoteIdentifier")
        }

        if let existingCKRecordSystemFields = getSystemFieldsRecord(), existingCKRecordSystemFields.recordChangeTag == ckRecord.recordChangeTag {
            logger.debug("CKRecord \(ckRecord.recordID.recordName) has same change tag as local book; no update made")
            return
        }

        setSystemFields(ckRecord)
    }

    @discardableResult
    static func create(from ckRecord: CKRecord, in context: NSManagedObjectContext) -> Self {
        let newItem = Self(context: context)
        newItem.remoteIdentifier = ckRecord.recordID.recordName
        newItem.update(from: ckRecord, excluding: [])
        return newItem
    }

    /**
     Updates values in this book with those from the provided CKRecord. Values in this books which have a pending
     change are not updated.
    */
    func update(from ckRecord: CKRecord, excluding excludedKeys: [String]?) {
        setSystemFields(from: ckRecord)

        // TODO Consider whether we should skip metadata updates if the change token is the same (as noticed in the above function call)
        // This book may have local changes which we don't want to overwrite with the values on the server.
        for key in Self.allCKRecordKeys {
            if let excludedKeys = excludedKeys, excludedKeys.contains(key) {
                logger.debug("CKRecordKey '\(key)' not used to update local store due to pending local change")
                continue
            }
            setValue(ckRecord[key], for: key)
        }
    }
}
