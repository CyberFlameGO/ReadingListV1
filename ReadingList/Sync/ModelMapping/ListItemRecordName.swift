import Foundation

struct ListItemRecordName {
    let fullRecordName: String
    let bookRemoteIdentifier: String
    let listRemoteIdentifier: String
    private let separator = "__"

    init(bookRemoteIdentifier: String, listRemoteIdentifier: String) {
        self.bookRemoteIdentifier = bookRemoteIdentifier
        self.listRemoteIdentifier = listRemoteIdentifier
        self.fullRecordName = "\(bookRemoteIdentifier)\(separator)\(listRemoteIdentifier)"
    }

    init?(listItemRecordName: String) {
        let splitComponents = listItemRecordName.components(separatedBy: separator)
        if splitComponents.count != 2 { return nil }
        self.fullRecordName = listItemRecordName
        self.bookRemoteIdentifier = splitComponents[0]
        self.listRemoteIdentifier = splitComponents[1]
    }
}
