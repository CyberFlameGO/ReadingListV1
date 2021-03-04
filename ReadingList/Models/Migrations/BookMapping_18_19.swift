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

    @objc func listItemRemoteIdentifier(forBook book: NSManagedObject, list: NSManagedObject, manager: NSMigrationManager) -> String {
        guard let destinationBook = manager.destinationInstances(forEntityMappingName: "BookToBook", sourceInstances: [book]).first else {
            fatalError("No migrated book found")
        }
        guard let bookRemoteIdentifier = destinationBook.value(forKey: "remoteIdentifier") as? String else {
            fatalError("Book had no remoteIdentifier during ListItem migration")
        }
        guard let destinationList = manager.destinationInstances(forEntityMappingName: "ListToList", sourceInstances: [list]).first else {
            fatalError("No migrated list found")
        }
        guard let listRemoteIdentifier = destinationList.value(forKey: "remoteIdentifier") as? String else {
            fatalError("List had no remoteIdentifier during ListItem migration")
        }
        return "\(bookRemoteIdentifier)__\(listRemoteIdentifier)"
    }
}
