import Foundation
import CoreData

struct CKReferenceResolver {
    let context: NSManagedObjectContext

    init(context: NSManagedObjectContext) {
        self.context = context
    }

    func resolveReferences() {
        let fetchRequest = ListItem.fetchRequest()
        fetchRequest.predicate = NSPredicate.or([
            NSPredicate(format: "%K == nil", #keyPath(ListItem.book)),
            NSPredicate(format: "%K == nil", #keyPath(ListItem.list))
        ])
        fetchRequest.fetchLimit = 100

        let results = try! context.fetch(fetchRequest) as! [ListItem]
        logger.info("\(results.count) unresolved ListItems to fix")
        for result in results {
            if result.book == nil {
                if let bookRemoteIdentifier = result.bookRemoteIdentifier {
                    let bookFetchRequest = Book.fetchRequest()
                    bookFetchRequest.predicate = Book.withRemoteIdentifier(bookRemoteIdentifier)
                    bookFetchRequest.fetchLimit = 1
                    if let matchingBook = try! context.fetch(bookFetchRequest).first {
                        logger.info("Resolved book \(bookRemoteIdentifier) on list item \(result.remoteIdentifier)")
                        result.setValue(matchingBook, forKeyPath: #keyPath(ListItem.book))
                    } else {
                        logger.warning("Could not find book with remoteIdentifier \(bookRemoteIdentifier)")
                    }
                } else {
                    logger.error("ListItem was missing bookRemoteIdentifier; deleting")
                    result.delete()
                }
            }

            if result.list == nil {
                if let listRemoteIdentifier = result.listRemoteIdentifier {
                    let listFetchRequest = List.fetchRequest()
                    listFetchRequest.predicate = NSPredicate(format: "%K == %@", #keyPath(List.remoteIdentifier), listRemoteIdentifier)
                    listFetchRequest.fetchLimit = 1
                    if let matchingList = try! context.fetch(listFetchRequest).first {
                        logger.info("Resolved list \(listRemoteIdentifier) on list item \(result.remoteIdentifier)")
                        result.setValue(matchingList, forKeyPath: #keyPath(ListItem.list))
                    } else {
                        logger.warning("Could not find list with remoteIdentifier \(listRemoteIdentifier)")
                    }
                } else {
                    logger.error("ListItem was missing listRemoteIdentifier; deleting")
                    result.delete()
                }
            }

            context.saveIfChanged()
        }
    }
}
