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
            logger.debug("Building CKRecord for \(entity.name!) with name \(remoteIdentifier) from stored record data")
            ckRecord = ckRecordFromSystemFields
        } else {
            logger.debug("Building CKRecord for \(entity.name!) with name \(remoteIdentifier)")
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

    /// Returns whether the system fields were updated.
    @discardableResult
    func setSystemFields(from ckRecord: CKRecord) -> Bool {
        if remoteIdentifier != ckRecord.recordID.recordName {
            logger.error("Attempted to update local object with remoteIdentifier \(remoteIdentifier) from a CKRecord which has record name \(ckRecord.recordID.recordName)")
            fatalError("Attempted to update local object from CKRecord with different remoteIdentifier")
        }

        if let existingCKRecordSystemFields = getSystemFieldsRecord(), existingCKRecordSystemFields.recordChangeTag == ckRecord.recordChangeTag {
            return false
        }

        logger.debug("CKRecord \(ckRecord.recordID.recordName) system fields updated")
        setSystemFields(ckRecord)
        return true
    }

    @discardableResult
    static func createObject(from ckRecord: CKRecord, in context: NSManagedObjectContext) -> Self {
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
        guard setSystemFields(from: ckRecord) else {
            logger.debug("CKRecord \(ckRecord.recordID.recordName) has same change tag as local book; no update made")
            return
        }

        for key in Self.allCKRecordKeys {
            // This book may have local changes which we don't want to overwrite with the values on the server.
            if let excludedKeys = excludedKeys, excludedKeys.contains(key) {
                logger.debug("CKRecordKey '\(key)' not used to update local store due to pending local change")
                continue
            }
            setValue(ckRecord[key], for: key)
        }
    }

    /**
     Merges the data in the object with the data from the record, selectively preferring data on a property-by-property basis via
     some heuristics about what data is most likely to be more recent.
     */
    func merge(with ckRecord: CKRecord) {
        setSystemFields(from: ckRecord)

        for key in Self.allCKRecordKeys {
            guard let recordValue: CKRecordValueProtocol = ckRecord[key] else {
                // Prefer data over nil
                continue
            }
            guard let existingValue = getValue(for: key) else {
                setValue(recordValue, for: key)
                return
            }
            if !existingValue.isPreferableTo(recordValue) {
                setValue(recordValue, for: key)
            }
        }
    }
}

extension CKRecordValueProtocol {
    func isPreferableTo(_ other: CKRecordValueProtocol) -> Bool {
        if let selfString = self as? String, let otherString = other as? String {
            return selfString.count > otherString.count
        }
        if let selfDate = self as? Date, let otherDate = other as? Date {
            return selfDate > otherDate
        }
        if let selfInt = self as? Int, let otherInt = other as? Int {
            return selfInt > otherInt
        }
        return false
    }
}
