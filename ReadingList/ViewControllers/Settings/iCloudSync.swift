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
    @State var syncDisabledReason: SyncDisabledReason?

    func updateAccountStatus() {
        CKContainer.default().accountStatus { status, _ in
            DispatchQueue.main.async {
                accountStatus = status
            }
        }
    }

    var body: some View {
        SwiftUI.List {
            Section(
                header: HeaderText("Sync", inset: hostingSplitView.isSplit),
                footer: CloudSyncFooter(accountStatus: accountStatus, syncDisabledReason: syncDisabledReason)
            ) {
                Toggle(isOn: settings.$syncEnabled.binding.didSet { isEnabled in
                    guard let syncCoordinator = AppDelegate.shared.syncCoordinator else {
                        os_log(.error, "SyncCoordinator nil when attempting to enable or disable iCloud sync")
                        return
                    }
                    if isEnabled {
                        syncDisabledReason = nil
                        syncCoordinator.start()
                    } else {
                        syncCoordinator.stop()
                    }
                }) {
                    Text("Enable iCloud Sync")
                }.disabled(accountStatus != .available)
            }
            if accountStatus == .available {
                Section(
                    header: HeaderText("Settings", inset: hostingSplitView.isSplit)
                ) {
                    NavigationLink("Advanced", destination: CloudSyncAdvanced())
                }
            }
        }.onAppear {
            updateAccountStatus()
            syncDisabledReason = AppDelegate.shared.syncCoordinator?.disabledReason
        }
        .onReceive(NotificationCenter.default.publisher(for: .CKAccountChanged)) { _ in
            updateAccountStatus()
        }
        .possiblyInsetGroupedListStyle(inset: hostingSplitView.isSplit)
        .navigationBarTitle("iCloud Sync", displayMode: .inline)
    }
}

struct CloudSyncFooter: View {
    let accountStatus: CKAccountStatus
    @EnvironmentObject var hostingSplitView: HostingSettingsSplitView
    let syncDisabledReason: SyncDisabledReason?

    var text: String {
        if accountStatus == .available {
            return "Configure data should be synchronised across all your devices via iCloud."
        } else {
            return "Log in to iCloud to enable iCloud Sync."
        }
    }

    var body: some View {
        Footer(inset: hostingSplitView.isSplit) {
            VStack(alignment: .leading, spacing: 16) {
                HStack {
                    Text(text)
                    Spacer()
                }
                if let syncDisabledReason = syncDisabledReason {
                    CloudSyncDisabledError(syncDisabledReason: syncDisabledReason)
                }
            }
        }
    }
}

struct CloudSyncDisabledError: View {
    let syncDisabledReason: SyncDisabledReason

    var body: some View {
        VStack(alignment: .center, spacing: 16) {
            HStack {
                Image(systemName: "exclamationmark.triangle.fill").foregroundColor(Color(.systemOrange))
                Text("Syncing paused")
            }.font(.body)
            Text(syncDisabledReason.description)
                .lineLimit(nil)
                .font(.footnote)
        }
    }
}

extension SyncDisabledReason: CustomStringConvertible {
    var description: String {
        switch self {
        case .outOfDateApp:
            return "Another device is using a more up-to-date version of Reading List. Please update the app via the App Store to resume syncing."
        case .unexpectedResponse:
            return "An unexpected error occurred during syncing."
        case .userAccountChanged:
            return "The iCloud user account on this device was changed. To sync with the current iCloud user, re-enable iCloud Sync above."
        case .cloudDataDeleted:
            return "The iCloud data was deleted. To re-sync your data with iCloud, re-enable iCloud Sync above."
        }
    }
}

struct CloudSync_Previews: PreviewProvider {
    static var previews: some View {
        NavigationView {
            CloudSync().environmentObject(HostingSettingsSplitView())
        }
    }
}
