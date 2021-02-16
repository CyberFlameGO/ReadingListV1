import SwiftUI
import PersistedPropertyWrapper

extension Persisted: DynamicProperty {
    public var binding: Binding<Exposed> {
        Binding(
            get: {
                self.wrappedValue
            },
            set: {
                self.wrappedValue = $0
            }
        )
    }
}
