import Foundation

enum SyncDisabledReason {
    case outOfDateApp
    case unexpectedResponse
    case userAccountChanged
    case cloudDataDeleted
}
