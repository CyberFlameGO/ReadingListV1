#if DEBUG

import Foundation
import CoreData
import PersistedPropertyWrapper

class Debug {

    @Persisted("showSortNumber", defaultValue: false)
    static var showSortNumber: Bool

    @Persisted("backupRestoreStayDownloading", defaultValue: false)
    static var stayOnBackupRestorationDownloadScreen: Bool

    @Persisted("simulateBackupFailure", defaultValue: false)
    static var simulateBackupFailure: Bool

    private static let screenshotsCommand = "--UITests_Screenshots"

    static func initialiseSettings() {
        if CommandLine.arguments.contains("--reset") {
            UserDefaults.standard.removePersistentDomain(forName: Bundle.main.bundleIdentifier!)
            NSPersistentStoreCoordinator().destroyAndDeleteStore(at: PersistentStoreManager.storeLocation)
        }
    }

    static func initialiseData() {
        if CommandLine.arguments.contains("--UITests_PopulateData") {
            loadData(downloadImages: CommandLine.arguments.contains(screenshotsCommand)) {
                if CommandLine.arguments.contains("--UITests_DeleteLists") {
                    deleteAllLists()
                }
            }
        }
    }

    static func deleteAllLists() {
        let batchDelete = NSBatchDeleteRequest(fetchRequest: List.fetchRequest())
        batchDelete.resultType = .resultTypeObjectIDs
        let result = try! PersistentStoreManager.container.persistentStoreCoordinator.execute(batchDelete, with: PersistentStoreManager.container.viewContext)
        guard let deletedObjectIds = (result as? NSBatchDeleteResult)?.result as? [NSManagedObjectID] else {
            preconditionFailure("Unexpected batch delete result format: \(result)")
        }
        if deletedObjectIds.isEmpty { return }
        NSManagedObjectContext.mergeChanges(fromRemoteContextSave: [NSDeletedObjectsKey: deletedObjectIds],
                                            into: [PersistentStoreManager.container.viewContext])
    }

    static func loadData(downloadImages: Bool, _ completion: (() -> Void)?) {
        let csvPath = Bundle.main.url(forResource: "examplebooks", withExtension: "csv")!
        let settings = BookCSVImportSettings(downloadCoverImages: downloadImages)
        BookCSVImporter(format: .readingList, settings: settings).startImport(fromFileAt: csvPath) { result in
            guard case .success = result else { preconditionFailure("Error in CSV file") }
            DispatchQueue.main.async {
                completion?()
            }
        }
    }
}

#endif
