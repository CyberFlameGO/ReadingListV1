import Foundation
import CoreData

/**
 A list item contains the pairing of a Book and a List, together with an integer sort index. These objects should not typically be created or deleted directly,
 but instead the functions available on the List object be used. These objects exist instead of using an ordered List --> Book relatioship, due to the
 difficulty of expressing that relationship in CloudKit.
 */
@objc(ListItem)
class ListItem: NSManagedObject {
    @NSManaged var sort: Int32
    @NSManaged private(set) var book: Book?
    @NSManaged private(set) var list: List?

    // These two properties are used to resolve relationships if, while syncing, a list item arrives before the associated
    // book or list.
    @NSManaged var bookRemoteIdentifier: String?
    @NSManaged var listRemoteIdentifier: String?

    convenience init(context: NSManagedObjectContext, book: Book, list: List, sort: Int32) {
        self.init(context: context)
        self.book = book
        self.sort = sort
        self.list = list
    }
}
