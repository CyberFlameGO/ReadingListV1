import Foundation
import UIKit
import CoreData
import os.log

class DeleteAllManager {
    /// A global reference to a shared instance, which can persist while switching out the window root view controller..
    static let shared = DeleteAllManager()

    /// Switch to the deletion screen, delete the persistent store, and then switch back to the app's normal root controller when complete.
    func deleteAll() {
        guard let window = AppDelegate.shared.window else { fatalError("No window available when attempting to delete all") }
        logger.info("Replacing window root view controller with deletion placeholder view")
        window.rootViewController = DeleteAll()

        if let syncCoordinator = AppDelegate.shared.syncCoordinator {
            syncCoordinator.stop()
            syncCoordinator.reset()
            AppDelegate.shared.syncCoordinator = nil
            CloudSyncSettings.settings.syncEnabled = false
        }

        DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + 1) {
            logger.info("Destroying persistent store")
            PersistentStoreManager.container.persistentStoreCoordinator.destroyAndDeleteStore(at: PersistentStoreManager.storeLocation)
            PersistentStoreManager.container = nil

            logger.info("Initialising persistent store")
            try! PersistentStoreManager.initalisePersistentStore {
                DispatchQueue.main.asyncAfter(deadline: .now() + 1) {
                    logger.info("Reinitialising persistent store coordinator")
                    AppDelegate.shared.initialiseSyncCoordinator()

                    logger.info("Replacing window root view controller")
                    let newTabBarController = TabBarController()
                    window.rootViewController = newTabBarController
                    newTabBarController.presentImportExportView(importUrl: nil)
                }
            }
        }
    }
}
