import Foundation
import UIKit
import CloudKit
import SVProgressHUD
import Reachability
import CoreData
import SwiftUI
import PersistedPropertyWrapper
import os.log

class CloudSyncSettings: ObservableObject {
    private init() { }

    static var settings = CloudSyncSettings()

    @Persisted("iCloudSyncEnabled", defaultValue: false)
    var syncEnabled: Bool
}

extension Binding {
    func didSet(execute: @escaping (Value) -> Void) -> Binding {
        Binding(
            get: { wrappedValue },
            set: {
                wrappedValue = $0
                execute($0)
            }
        )
    }
}

struct CloudSync: View {
    @EnvironmentObject var hostingSplitView: HostingSettingsSplitView
    @ObservedObject var settings = CloudSyncSettings.settings
    @State var accountStatus = CKAccountStatus.couldNotDetermine

    func updateAccountStatus() {
        CKContainer.default().accountStatus { status, _ in
            accountStatus = status
        }
    }

    var footerText: String {
        if accountStatus == .available {
            return "Configure data should be synchronised across all your devices via iCloud."
        } else {
            return "Log in to iCloud to enable iCloud Sync."
        }
    }

    var body: some View {
        SwiftUI.List {
            Section(
                header: HeaderText("Sync", inset: hostingSplitView.isSplit),
                footer: FooterText(footerText, inset: hostingSplitView.isSplit
                )
            ) {
                Toggle(isOn: settings.$syncEnabled.binding.didSet { on in
                    guard let syncCoordinator = AppDelegate.shared.syncCoordinator else {
                        os_log(.error, "SyncCoordinator nil when attempting to enable or disable iCloud sync")
                        return
                    }
                    if on {
                        syncCoordinator.start()
                    } else {
                        syncCoordinator.stop()
                    }
                }) {
                    Text("Enable iCloud Sync")
                }.disabled(accountStatus != .available)
            }
        }.onAppear {
            updateAccountStatus()
        }
        .onReceive(NotificationCenter.default.publisher(for: .CKAccountChanged)) { _ in
            updateAccountStatus()
        }
        .possiblyInsetGroupedListStyle(inset: hostingSplitView.isSplit)
        .navigationBarTitle("iCloud Sync", displayMode: .inline)
    }
}

struct CloudSync_Previews: PreviewProvider {
    static var previews: some View {
        NavigationView {
            CloudSync().environmentObject(HostingSettingsSplitView())
        }
    }
}

class CloudSyncOld: UITableViewController {

    @IBOutlet private weak var enabledSwitch: UISwitch!
    var syncCoordinator: SyncCoordinator? { AppDelegate.shared.syncCoordinator as? SyncCoordinator }

    override func viewDidLoad() {
        super.viewDidLoad()
        enabledSwitch.isOn = GeneralSettings.iCloudSyncEnabled
        if !GeneralSettings.iCloudSyncEnabled/* && syncCoordinator?.reachability.connection == .unavailable */{
            //enabledSwitch.isEnabled = false
        }
        //NotificationCenter.default.addObserver(self, selector: #selector(networkConnectivityDidChange), name: .reachabilityChanged, object: nil)
    }

//    @objc private func networkConnectivityDidChange() {
//        guard !GeneralSettings.iCloudSyncEnabled else { return }
//        enabledSwitch.isEnabled = syncCoordinator?.reachability.connection != .unavailable
//    }

    @IBAction private func iCloudSyncSwitchChanged(_ sender: UISwitch) {
        if sender.isOn {
            syncSwitchTurnedOn()
        } else {
            syncSwitchTurnedOff()
        }
    }

    private func syncSwitchTurnedOn() {
        guard let syncCoordinator = syncCoordinator else { fatalError("Unexpected nil sync coordinator") }

        SVProgressHUD.show(withStatus: "Enabling iCloud")
        DispatchQueue.main.async {
            GeneralSettings.iCloudSyncEnabled = true
            syncCoordinator.start()
//            syncCoordinator.remote.initialise { error in
                DispatchQueue.main.async {
                    SVProgressHUD.dismiss()
                }
//                    if let error = error {
//                        self.handleRemoteInitialiseError(error: error)
//                        self.enabledSwitch.setOn(false, animated: true)
//                    } else if self.nonRemoteBooksExistLocally() {
//                        self.requestSyncMergeAction()
//                    } else {
//                        GeneralSettings.iCloudSyncEnabled = true
//                        syncCoordinator.start()
//                    }
//                }
//            }
        }
    }

    private func nonRemoteBooksExistLocally() -> Bool {
        let fetch = NSManagedObject.fetchRequest(Book.self)
        fetch.predicate = NSPredicate(format: "%K = nil", #keyPath(Book.remoteIdentifier))
        return try! PersistentStoreManager.container.viewContext.count(for: fetch) != 0
    }

    private func requestSyncMergeAction() {
        // TODO: This is a misleading and incorrect message. Check whether there are books in iCloud already
        let alert = UIAlertController(title: "Data Already Exists", message: """
            iCloud sync has been enabled previously, so there may be books in iCloud already.

            You can either merge the list of books on this \(UIDevice.current.model) with the \
            books in iCloud, or you can choose to replace all the data on this \(UIDevice.current.model) \
            with the books already in iCloud.
            """, preferredStyle: .actionSheet)
        alert.addAction(UIAlertAction(title: "Merge", style: .default) { _ in
            GeneralSettings.iCloudSyncEnabled = true
            self.syncCoordinator!.start()
        })
        alert.addAction(UIAlertAction(title: "Replace", style: .destructive) { [unowned self] _ in
            self.presentConfirmReplaceDialog()
        })
        alert.addAction(UIAlertAction(title: "Cancel", style: .cancel) { [unowned self] _ in
            self.enabledSwitch.setOn(false, animated: true)
        })
        alert.popoverPresentationController?.setSourceCell(atIndexPath: IndexPath(row: 0, section: 0), inTable: tableView)
        present(alert, animated: true)
    }

    private func presentConfirmReplaceDialog() {
        let alert = UIAlertController(title: "Confirm Replace", message: """
            Are you sure you wish to replace the data on this \(UIDevice.current.model) with the data on iCloud?

            You will lose any books which are only stored on this \(UIDevice.current.model).
            """, preferredStyle: .actionSheet)
        alert.addAction(UIAlertAction(title: "Replace", style: .destructive) { _ in
            PersistentStoreManager.delete(type: Book.self)
            GeneralSettings.iCloudSyncEnabled = true
            self.syncCoordinator!.start()
        })
        alert.addAction(UIAlertAction(title: "Cancel", style: .cancel) { [unowned self] _ in
            self.enabledSwitch.setOn(false, animated: true)
        })
        alert.popoverPresentationController?.setSourceCell(atIndexPath: IndexPath(row: 0, section: 0), inTable: tableView)
        present(alert, animated: true)
    }

    private func syncSwitchTurnedOff() {
        guard let syncCoordinator = syncCoordinator else { fatalError("Unexpected nil sync coordinator") }

        let alert = UIAlertController(title: "Disable Sync?", message: """
            If you disable iCloud sync, changes you make will no longer be \
            synchronised across your devices, or backed up to iCloud.
            """, preferredStyle: .actionSheet)
        alert.addAction(UIAlertAction(title: "Disable", style: .destructive) { _ in
            // TODO: Consider whether change tokens should be discarded, remote identifiers should be deleted, etc
            //self.syncCoordinator!.stop()
        })
        alert.addAction(UIAlertAction(title: "Keep Enabled", style: .cancel) { [unowned self] _ in
            self.enabledSwitch.isOn = true
        })
        alert.popoverPresentationController?.setSourceCell(atIndexPath: IndexPath(row: 0, section: 0), inTable: tableView)
        present(alert, animated: true)
    }

    private func handleRemoteInitialiseError(error: Error) {
        let alert: UIAlertController
        if let ckError = error as? CKError {
            switch ckError.code {
            case .networkFailure,
                 .networkUnavailable,
                 .serviceUnavailable:
                alert = UIAlertController(title: "Could not connect", message: "Could not connect to iCloud. Please try again later.", preferredStyle: .alert)
            case .notAuthenticated:
                alert = UIAlertController(title: "Not Signed In", message: "iCloud sync could not be enabled because you are not signed in to iCloud.", preferredStyle: .alert)
            default:
                alert = UIAlertController(title: "Could not enable iCloud sync", message: "An error occurred enabling iCloud sync.", preferredStyle: .alert)
            }
        } else {
            alert = UIAlertController(title: "Could not enable iCloud sync", message: "An unexpected error occurred enabling iCloud sync. Please try again later.", preferredStyle: .alert)
        }

        alert.addAction(UIAlertAction(title: "OK", style: .default, handler: nil))
        present(alert, animated: true)
    }
}
