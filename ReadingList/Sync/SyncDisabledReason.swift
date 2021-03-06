import Foundation

enum SyncDisabledReason {
    case outOfDateApp
    case unexpectedError
    case userAccountChanged
    case cloudDataDeleted
}
