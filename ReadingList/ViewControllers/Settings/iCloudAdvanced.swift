import Foundation
import SwiftUI

struct CloudSyncAdvanced: View {
    @State var forceFullResyncSheetShowing = false
    @State var syncInfoShowing = false
    @State var syncStatus: SyncStatus?

    static var dateFormatter: DateFormatter = {
        var formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter
    }()

    func statusText() -> String {
        guard let syncStatus = syncStatus else { return "No status could be determined" }
        var status = """
            Counts:
            \(syncStatus.objectCountByEntityName.map { key, value in
                "  \(key): \(value) (\(syncStatus.uploadedObjectCount[key]!) uploaded)"
            }.joined(separator: "\n"))
            """
        if let lastProcessedTimestamp = syncStatus.lastProcessedLocalTransaction {
            status += """


            Last Processed Change:
            \(Self.dateFormatter.string(from: lastProcessedTimestamp))
            """
        }
        return status
    }

    var body: some View {
        Form {
            Button("See Status") {
                guard let syncCoordinator = AppDelegate.shared.syncCoordinator else { return }
                DispatchQueue.global(qos: .userInteractive).async {
                    let status = syncCoordinator.status()
                    DispatchQueue.main.async {
                        syncStatus = status
                        syncInfoShowing = true
                    }
                }

            }.alert(isPresented: $syncInfoShowing) {
                Alert(
                    title: Text("Sync Status"),
                    message: Text(statusText()),
                    dismissButton: .cancel()
                )
            }
            // Removed as this can cause a long delay while errors are resolved.
            Button("Force Full Resync") {
                forceFullResyncSheetShowing = true
            }.foregroundColor(Color(.systemRed))
            .actionSheet(isPresented: $forceFullResyncSheetShowing) {
                ActionSheet(
                    title: Text("Force Full iCloud Sync"),
                    message: Text("This will re-sync your entire library with iCloud. It is recommended that you only proceed if you are experiencing issues with iCloud sync. This may take a long time to complete. Are you sure you wish to continue?"),
                    buttons: [
                        .destructive(Text("Resync")) {
                            guard let syncCoordinator = AppDelegate.shared.syncCoordinator else { return }
                            UserEngagement.logEvent(.forceFullCloudSync)
                            syncCoordinator.forceFullResync()
                        },
                        .cancel()
                    ])
            }
        }
    }
}
