import Foundation
import SwiftUI

struct SafariPresentingButton<ButtonLabel>: View where ButtonLabel: View {
    let url: URL
    let buttonLabel: ButtonLabel
    let buttonAction: (() -> Void)?

    init(_ url: URL, @ViewBuilder label: () -> ButtonLabel) {
        self.url = url
        self.buttonAction = nil
        self.buttonLabel = label()
    }

    init(_ url: URL, buttonAction: @escaping () -> Void, @ViewBuilder label: () -> ButtonLabel) {
        self.url = url
        self.buttonAction = buttonAction
        self.buttonLabel = label()
    }

    @State var presenting = false

    var body: some View {
        Button(action: {
            buttonAction?()
            presenting.toggle()
        }) {
            buttonLabel
        }
        .safariView(isPresented: $presenting) {
            SafariView(url: url)
        }
    }
}

extension View {
    func presentingSafari(_ url: URL) -> some View {
        return SafariPresentingButton(url) {
            self
        }.buttonStyle(PlainButtonStyle())
    }
}
