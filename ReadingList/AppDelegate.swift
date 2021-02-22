import UIKit
import Logging
import Combine

let logger = Logger(label: "com.andrewbennet.books")

@UIApplicationMain
class AppDelegate: UIResponder, UIApplicationDelegate {

    var window: UIWindow?
    lazy var launchManager = LaunchManager(window: window)
    let upgradeManager = UpgradeManager()

    /// Will be nil until after the persistent store is initialised.
    var syncCoordinator: SyncCoordinator?

    static var shared: AppDelegate {
        return UIApplication.shared.delegate as! AppDelegate
    }

    var tabBarController: TabBarController? {
        return window?.rootViewController as? TabBarController
    }

    func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        launchManager.initialise()

        logger.info("\n\n")
        logger.info("Application launched")
        upgradeManager.performNecessaryUpgradeActions()

        // Remote notifications are required for iCloud sync.
        application.registerForRemoteNotifications()

        // Grab any options which we will take action on after the persistent store is initialised
        let options = launchManager.extractRelevantLaunchOptions(launchOptions)
        launchManager.initialisePersistentStore {
            self.initialiseSyncCoordinator()
            self.launchManager.handleLaunchOptions(options)
        }

        // If there were any options, they will be handled once the store is initialised
        return !options.any()
    }

    private func initialiseSyncCoordinator() {
        // Initialise the Sync Coordinator which will maintain iCloud synchronisation
        let syncCoordinator = SyncCoordinator(
            persistentStoreCoordinator: PersistentStoreManager.container.persistentStoreCoordinator,
            orderedTypesToSync: [Book.self, List.self, ListItem.self]
        )
        self.syncCoordinator = syncCoordinator
        if GeneralSettings.iCloudSyncEnabled {
            syncCoordinator.start()
        }
    }

    func applicationDidBecomeActive(_ application: UIApplication) {
        launchManager.handleApplicationDidBecomeActive()
        
        if let syncCoordinator = self.syncCoordinator, GeneralSettings.iCloudSyncEnabled {
            syncCoordinator.enqueueFetchRemoteChanges()
        }
    }

    func application(_ application: UIApplication, performActionFor shortcutItem: UIApplicationShortcutItem, completionHandler: @escaping (Bool) -> Void) {
        let didHandle = launchManager.handleQuickAction(shortcutItem)
        completionHandler(didHandle)
    }

    func application(_ app: UIApplication, open url: URL, options: [UIApplication.OpenURLOptionsKey: Any] = [:]) -> Bool {
        return launchManager.handleOpenUrl(url)
    }

    func application(_ application: UIApplication, didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data) {
        logger.info("Successfully registered for remote notifications")
    }

    func application(_ application: UIApplication, didFailToRegisterForRemoteNotificationsWithError error: Error) {
        logger.error("Failed to register for remote notifications: \(error.localizedDescription)")
    }

    private var persistentStoreObserver: AnyCancellable?

    func application(_ application: UIApplication, didReceiveRemoteNotification userInfo: [AnyHashable: Any],
                     fetchCompletionHandler completionHandler: @escaping (UIBackgroundFetchResult) -> Void) {
        logger.info("Application received remote notification")
        guard GeneralSettings.iCloudSyncEnabled else {
            completionHandler(.noData)
            return
        }
        if let syncCoordinator = self.syncCoordinator {
            if syncCoordinator.isRunning {
                syncCoordinator.enqueueFetchRemoteChanges(completion: completionHandler)
            } else {
                logger.info("SyncCoordinator was not running; remote notification did not lead to new data")
                completionHandler(.failed)
            }
        } else {
            logger.info("Persistent store was not initialised; waiting for initialisation to complete")
            persistentStoreObserver = NotificationCenter.default.publisher(for: .didCompletePersistentStoreInitialisation, object: nil)
                .receive(on: DispatchQueue.main)
                .sink { _ in
                    logger.info("Persistent store initialisation completion detected; responding to remote change notification")
                    guard let syncCoordinator = self.syncCoordinator else { fatalError("Unexpected nil syncCoordinator") }
                    syncCoordinator.enqueueFetchRemoteChanges(completion: completionHandler)
                }
        }
    }
}
