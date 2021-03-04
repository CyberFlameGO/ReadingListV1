import SwiftUI

extension View {
    func withButtonAction(_ action: @escaping () -> Void) -> some View {
        Button(action: action) {
            self
        }.buttonStyle(PlainButtonStyle())
    }
}
