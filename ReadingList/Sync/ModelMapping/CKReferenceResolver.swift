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
            guard let recordName = ListItemRecordName(listItemRecordName: result.remoteIdentifier) else {
                logger.critical("Could not parse ListItem remote identifier \(result.remoteIdentifier)")
                result.delete()
                continue
            }
            if result.book == nil {
                let bookFetchRequest = Book.fetchRequest()
                bookFetchRequest.predicate = Book.withRemoteIdentifier(recordName.bookRemoteIdentifier)
                bookFetchRequest.fetchLimit = 1
                if let matchingBook = try! context.fetch(bookFetchRequest).first {
                    logger.info("Resolved book \(recordName.bookRemoteIdentifier) on list item \(result.remoteIdentifier)")
                    result.setValue(matchingBook, forKeyPath: #keyPath(ListItem.book))
                } else {
                    logger.warning("Could not find book with remoteIdentifier \(recordName.bookRemoteIdentifier)")
                }
            }

            if result.list == nil {
                let listFetchRequest = List.fetchRequest()
                listFetchRequest.predicate = NSPredicate(format: "%K == %@", #keyPath(List.remoteIdentifier), recordName.listRemoteIdentifier)
                listFetchRequest.fetchLimit = 1
                if let matchingList = try! context.fetch(listFetchRequest).first {
                    logger.info("Resolved list \(recordName.listRemoteIdentifier) on list item \(result.remoteIdentifier)")
                    result.setValue(matchingList, forKeyPath: #keyPath(ListItem.list))
                } else {
                    logger.warning("Could not find list with remoteIdentifier \(recordName.listRemoteIdentifier)")
                }
            }

            context.saveIfChanged()
        }
    }
}
