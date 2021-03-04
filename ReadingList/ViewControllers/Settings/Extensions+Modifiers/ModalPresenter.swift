import Foundation
import SwiftUI

struct ModalPresenter<Wrapped, Modal>: View where Wrapped: View, Modal: View {
    @State var isPresented = false
    var wrapped: Wrapped
    var modal: Modal

    var body: some View {
        wrapped.withButtonAction {
            isPresented.toggle()
        }.sheet(isPresented: $isPresented) {
            modal
        }
    }
}

extension View {
    func modal<Modal>(_ modal: Modal) -> some View where Modal: View {
        return ModalPresenter(wrapped: self, modal: modal)
    }
}
