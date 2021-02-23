import Foundation
import SwiftUI

struct HeaderText: View {
    init(_ text: String, inset: Bool) {
        self.text = text
        self.inset = inset
    }

    let text: String
    let inset: Bool

    var topPaddedText: some View {
        Text(text.uppercased()).padding(.top, 20)
    }

    var body: some View {
        if #available(iOS 14.0, *) {
            topPaddedText.padding(.horizontal, inset ? 22 : 0)
        } else {
            topPaddedText
        }
    }
}

struct Footer<InnerView>: View where InnerView: View {
    init(_ text: String, inset: Bool) where InnerView == Text {
        self.view = Text(text)
        self.inset = inset
    }
    
    init(inset: Bool, @ViewBuilder view: () -> InnerView) {
        self.view = view()
        self.inset = inset
    }

    let view: InnerView
    let inset: Bool

    var body: some View {
        if #available(iOS 14.0, *) {
            view.padding(.horizontal, inset ? 22 : 0)
        } else {
            view
        }
    }
}
