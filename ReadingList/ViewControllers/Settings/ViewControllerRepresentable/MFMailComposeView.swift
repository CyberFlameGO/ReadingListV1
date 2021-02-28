import Foundation
import SwiftUI
import MessageUI

struct MailView: UIViewControllerRepresentable {
    @Binding var isShowing: Bool
    let receipients: [String]?
    let messageBody: String?
    let subject: String?
    let attachments: [Attachment]?

    struct Attachment {
        let data: Data
        let mimeType: String
        let fileName: String
    }

    class Coordinator: NSObject, MFMailComposeViewControllerDelegate {
        @Binding var isShowing: Bool

        init(isShowing: Binding<Bool>) {
            _isShowing = isShowing
        }

        func mailComposeController(_ controller: MFMailComposeViewController, didFinishWith result: MFMailComposeResult, error: Error?) {
            isShowing = false
        }
    }

    func makeCoordinator() -> Coordinator {
        return Coordinator(isShowing: $isShowing)
    }

    func makeUIViewController(context: UIViewControllerRepresentableContext<MailView>) -> MFMailComposeViewController {
        let viewController = MFMailComposeViewController()
        viewController.mailComposeDelegate = context.coordinator
        return viewController
    }

    func updateUIViewController(_ uiViewController: MFMailComposeViewController, context: UIViewControllerRepresentableContext<MailView>) {
        if let messageBody = messageBody {
            uiViewController.setMessageBody(messageBody, isHTML: false)
        }
        if let subject = subject {
            uiViewController.setSubject(subject)
        }
        if let attachments = attachments {
            for attachment in attachments {
                uiViewController.addAttachmentData(attachment.data, mimeType: attachment.mimeType, fileName: attachment.fileName)
            }
        }
        uiViewController.setToRecipients(receipients)
    }
}
