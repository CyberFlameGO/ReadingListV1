import Foundation

enum SyncCoordinatorError: Error, CustomNSError {
    case unexpectedResponse(String)
    case unhandledError(Error)
    case unexpectedErrorType(Error)

    var errorCode: Int {
        switch self {
        case .unexpectedResponse: return 0
        case .unhandledError: return 1
        case .unexpectedErrorType: return 2
        }
    }

    var errorUserInfo: [String: Any] {
        switch self {
        case .unhandledError(let error):
            return (error as NSError).userInfo
        case .unexpectedErrorType(let error):
            return (error as NSError).userInfo
        case .unexpectedResponse(let message):
            return ["description": message]
        }
    }
}
