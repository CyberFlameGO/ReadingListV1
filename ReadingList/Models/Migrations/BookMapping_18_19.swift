import Foundation
import CoreData

class BookMapping_18_19: NSEntityMigrationPolicy { //swiftlint:disable:this type_name

    @objc func listRemoteIdentifier() -> String {
        return UUID().uuidString
    }

    @objc func bookRemoteIdentifier(forGoogleID googleID: String?, manualID: String?) -> String {
        if let googleID = googleID {
            return "gbid:\(googleID)"
        } else if let manualID = manualID {
            return "mid:\(manualID)"
        } else {
            fatalError("Book had neither a google ID nor a manual ID")
        }
    }

    @objc func listItemRemoteIdentifier(forBook book: NSManagedObject, list: NSManagedObject) -> String {
        guard let bookRemoteIdentifier = book.value(forKey: "remoteIdentifier") as? String else {
            fatalError("Book had no remoteIdentifier during ListItem migration")
        }
        guard let listRemoteIdentifier = list.value(forKey: "remoteIdentifier") as? String else {
            fatalError("List had no remoteIdentifier during ListItem migration")
        }
        return "\(bookRemoteIdentifier)__\(listRemoteIdentifier)"
    }
}
