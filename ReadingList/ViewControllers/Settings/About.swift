import SwiftUI
import UIKit
import WhatsNewKit
import SafariServices
import MessageUI
import ZIPFoundation
import CocoaLumberjackSwift

struct About: View {
    let changeListProvider = ChangeListProvider()
    @State var isShowingMailAlert = false
    @State var isShowingMailView = false
    @State var isShowingLegacyMailAlert = false
    @State var isShowingFaq = false
    @State var isShowingShareLogsSheet = false
    @State var isShowingShareLogsView = false
    @State var logFilePath: URL?
    @State var emailAttachments: [MailView.Attachment]?
    @EnvironmentObject var hostingSplitView: HostingSettingsSplitView

    var body: some View {
        SwiftUI.List {
            Section(header: AboutHeader(), footer: AboutFooter()) {
                IconCell("Website",
                         imageName: "house.fill",
                         backgroundColor: .blue,
                         withChevron: true
                ).presentingSafari(URL(string: "https://readinglist.app")!)

                IconCell("Share",
                         imageName: "paperplane.fill",
                         backgroundColor: .orange
                ).modal(ActivityView(activityItems: [URL(string: "https://\(Settings.appStoreAddress)")!], applicationActivities: nil, excludedActivityTypes: nil))

                IconCell("Twitter",
                         image: TwitterIcon(),
                         withChevron: true
                ).presentingSafari(URL(string: "https://twitter.com/ReadingListApp")!)

                IconCell("Email Developer",
                         imageName: "envelope.fill",
                         backgroundColor: .paleEmailBlue) {
                    if #available(iOS 14.0, *) {
                        isShowingMailAlert = true
                    } else {
                        // Action sheet anchors are messed up on iOS 13;
                        // go straight to the Email view, skipping the sheet
                        if MFMailComposeViewController.canSendMail() {
                            isShowingMailView = true
                        } else {
                            isShowingLegacyMailAlert = true
                        }
                    }
                }
                .actionSheet(isPresented: $isShowingMailAlert) {
                    mailAlert
                }.sheet(isPresented: $isShowingMailView) {
                    emailSheet
                }.alert(isPresented: $isShowingLegacyMailAlert) {
                    legacyMailAlert
                }.safariView(isPresented: $isShowingFaq) {
                    SafariView(url: URL(string: "https://readinglist.app/faqs/")!)
                }

                IconCellBody(
                    text: "Attributions",
                    imageName: "heart.fill",
                    backgroundColor: .green
                ).navigating(
                    // Re-provide the environment object, otherwise we seem to get trouble
                    // when the containing hosting VC gets removed from the window
                    to: Attributions().environmentObject(hostingSplitView)
                )

                IconCell("Privacy Policy",
                         imageName: "lock.fill",
                         backgroundColor: Color(.darkGray)
                ).navigating(to: PrivacyPolicy().environmentObject(hostingSplitView))

                IconCell("Logs",
                         imageName: "ant.fill",
                         backgroundColor: Color(.systemIndigo)) {
                    isShowingShareLogsSheet = true
                }.actionSheet(isPresented: $isShowingShareLogsSheet) {
                    shareLogsSheet
                }.sheet(isPresented: $isShowingShareLogsView) {
                    ActivityViewController(activityItems: [logFilePath as Any])
                }

                if changeListProvider.thisVersionChangeList() != nil {
                    IconCell("Recent Changes",
                             imageName: "wrench.fill",
                             backgroundColor: .blue,
                             withChevron: true
                    ).modal(ChangeListWrapper())
                }
            }
        }.onAppear {
            gatherLogFile()
        }
        .possiblyInsetGroupedListStyle(inset: hostingSplitView.isSplit)
        .navigationBarTitle("About")
    }

    func gatherLogFile() {
        DispatchQueue.global(qos: .userInteractive).async {
            guard let fileLogger = DDLog.allLoggers.compactMap({
                $0 as? DDFileLogger
            }).first else { fatalError("No file logger found") }

            var logFileData: Data?
            if let firstLogFilePath = fileLogger.logFileManager.sortedLogFilePaths.first {
                let logFile = URL(fileURLWithPath: firstLogFilePath)
                self.logFilePath = logFile
                do {
                    try logFileData = Data(contentsOf: logFile)
                } catch {
                    logger.error("Error getting log file data for email: \(error.localizedDescription)")
                }
            }

            if let logFileData = logFileData {
                emailAttachments = [MailView.Attachment(data: logFileData, mimeType: "text/plain", fileName: "ReadingList_Logs.txt")]
            } else {
                emailAttachments = nil
            }
        }
    }

    var legacyMailAlert: Alert {
        Alert(
            title: Text("Copy Email Address?"),
            message: Text("To suggest features or report bugs, please email feedback@readinglist.app"),
            primaryButton: .default(Text("Copy Email Address")) {
                UIPasteboard.general.string = "feedback@readinglist.app"
            },
            secondaryButton: .cancel()
        )
    }

    var mailAlert: ActionSheet {
        let emailButton: ActionSheet.Button
        if MFMailComposeViewController.canSendMail() {
            emailButton = .default(Text("Email"), action: {
                isShowingMailView = true
            })
        } else {
            emailButton = .default(Text("Copy Email Address"), action: {
                UIPasteboard.general.string = "feedback@readinglist.app"
            })
        }
        return ActionSheet(
            title: Text(""),
            message: Text("""
         Hi there!

         To suggest features or report bugs, please email me. I try my best to \
         reply to every email I receive, but this app is a one-person project, so \
         please be patient if it takes a little time for my reply!

         If you do have a specific question, I would suggest first looking on the FAQ \
         in case your answer is there.
         """),
            buttons: [
            emailButton,
            .default(Text("Open FAQ"), action: {
                isShowingFaq = true
            }),
            .cancel(Text("Dismiss"), action: {})
            ]
        )
    }

    var emailSheet: some View {
        MailView(
            isShowing: $isShowingMailView,
            receipients: [
                "Reading List Developer <\(Settings.feedbackEmailAddress)>"
            ],
            messageBody: """
            Your Message Here:




            Extra Info:
            App Version: \(BuildInfo.thisBuild.fullDescription)
            iOS Version: \(UIDevice.current.systemVersion)
            Device: \(UIDevice.current.modelName)
            """,
            subject: "Reading List Feedback",
            attachments: emailAttachments
        )
    }
    
    var shareLogsSheet: ActionSheet {
        ActionSheet(
            title: Text("Share Log Files"),
            message: Text("""
                Log files contain information about the running of the app, which can \
                be useful when diagnosing issues. During email correspondance, I may \
                ask you to provide me with the logs so I can investigate problems.
                """),
            buttons: [
                .default(Text("Share")) {
                    isShowingShareLogsView = true
                },
                .cancel()
            ])
    }
}

struct AboutHeader: View {
    var innerBody: some View {
        (Text("Reading List ").bold() +
        Text("""
            is developed by a single developer – me, Andrew 👋 I hope you are enjoying using the app 😊

            If you value the app, please consider leaving a review, tweeting about it, sharing, or leaving a tip.

            Happy Reading! 📚
            """
        )).font(.subheadline)
        .foregroundColor(Color(.label))
        .padding(.bottom, 20)
        .padding(.top, 10)
    }

    var body: some View {
        if #available(iOS 14.0, *) {
            innerBody
                .textCase(nil)
                .padding(.horizontal, 12)
        } else {
            innerBody
        }
    }
}

struct AboutFooter: View {
    static let numberFormatter: NumberFormatter = {
        var formatter = NumberFormatter()
        formatter.usesGroupingSeparator = false
        return formatter
    }()

    var buildNumber = numberFormatter.string(from: BuildInfo.thisBuild.buildNumber as NSNumber) ?? "0"

    var body: some View {
        VStack(alignment: .center, spacing: 4) {
            Text("v\(BuildInfo.thisBuild.version.description) (\(buildNumber))")
            Text("© Andrew Bennet 2021")
        }
        .frame(maxWidth: .infinity, alignment: .center)
        .font(.caption)
        .foregroundColor(Color(.label))
        .padding(.top, 10)
        .accessibilityElement(children: .ignore)
        .accessibility(label: Text("Version \(BuildInfo.thisBuild.version.description). Build \(buildNumber). Copyright Andrew Bennet, 2021."))
    }
}

extension Color {
    static let twitterBlue = Color(
        .sRGB,
        red: 76 / 255,
        green: 160 / 255,
        blue: 235 / 255,
        opacity: 1
    )

    static let paleEmailBlue = Color(
        .sRGB,
        red: 94 / 255,
        green: 191 / 255,
        blue: 244 / 255,
        opacity: 1
    )
}

fileprivate extension Image {
    func iconTemplate() -> some View {
        self.resizable()
            .renderingMode(.template)
            .foregroundColor(.white)
    }
}

struct TwitterIcon: View {
    var body: some View {
        SettingsIcon(color: .twitterBlue) {
            Image("twitter")
                .iconTemplate()
                .frame(width: 18, height: 18, alignment: .center)
        }
    }
}

struct About_Previews: PreviewProvider {
    static var previews: some View {
        NavigationView {
            About().environmentObject(HostingSettingsSplitView())
        }
    }
}
