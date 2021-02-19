import Foundation
import CoreData
import UIKit

extension ListItem: Sortable {
    var sortIndex: Int32 {
        get { sort }
        set { sort = newValue }
    }
}

final class ListBookDiffableDataSource: EmptyDetectingTableDiffableDataSource<String, NSManagedObjectID>, ResultsControllerSnapshotGeneratorDelegate {

    typealias SectionType = String
    var controller: NSFetchedResultsController<ListItem> {
        willSet {
            // Remove the old controller's delegate (just in case we have a memory leak and it isn't deallocated)
            // and assign the new value's delegate.
            controller.delegate = nil
        }
        didSet {
            controller.delegate = self.changeMediator.controllerDelegate
        }
    }
    var changeMediator: ResultsControllerSnapshotGenerator<ListBookDiffableDataSource>!
    let list: List
    let searchController: UISearchController
    let onContentChanged: () -> Void
    let sortManager: SortManager<ListItem>

    init(_ tableView: UITableView, list: List, controller: NSFetchedResultsController<ListItem>, sortManager: SortManager<ListItem>, searchController: UISearchController, onContentChanged: @escaping () -> Void) {
        self.searchController = searchController
        self.list = list
        self.onContentChanged = onContentChanged
        self.controller = controller
        self.sortManager = sortManager
        super.init(tableView: tableView) { _, indexPath, itemID in
            let cell = tableView.dequeue(BookTableViewCell.self, for: indexPath)
            let listItem = PersistentStoreManager.container.viewContext.object(with: itemID) as! ListItem
            guard let book = listItem.book else { fatalError("Missing book on list item") }
            cell.configureFrom(book, includeReadDates: false)
            return cell
        }

        self.changeMediator = ResultsControllerSnapshotGenerator<ListBookDiffableDataSource> { [unowned self] in
            self.snapshot()
        }
        self.changeMediator.delegate = self
        self.controller.delegate = self.changeMediator.controllerDelegate
    }

    override func tableView(_ tableView: UITableView, canEditRowAt indexPath: IndexPath) -> Bool {
        return true
    }

    override func tableView(_ tableView: UITableView, canMoveRowAt indexPath: IndexPath) -> Bool {
        return canMoveRow()
    }

    override func tableView(_ tableView: UITableView, moveRowAt sourceIndexPath: IndexPath, to destinationIndexPath: IndexPath) {
        moveRow(at: sourceIndexPath, to: destinationIndexPath)
    }

    func updateData(animate: Bool) {
        updateData(controller.snapshot(), animate: animate)
    }

    func updateData(_ snapshot: NSDiffableDataSourceSnapshot<String, NSManagedObjectID>, animate: Bool) {
        apply(snapshot, animatingDifferences: animate)
        onContentChanged()
    }

    func controller(_ controller: NSFetchedResultsController<NSFetchRequestResult>, didChangeProducingSnapshot snapshot: NSDiffableDataSourceSnapshot<String, NSManagedObjectID>, withChangedObjects changedObjects: [NSManagedObjectID]) {
        apply(snapshot, animatingDifferences: true)

        onContentChanged()
    }

    func getItem(at indexPath: IndexPath) -> ListItem {
        guard let itemId = itemIdentifier(for: indexPath) else { fatalError("No item found for index path \(indexPath)") }
        return PersistentStoreManager.container.viewContext.object(with: itemId) as! ListItem
    }

    func getBook(at indexPath: IndexPath) -> Book {
        guard let book = getItem(at: indexPath).book else { fatalError("No book found on list item") }
        return book
    }

    func canMoveRow() -> Bool {
        guard list.order == .listCustom else { return false }
        guard !searchController.hasActiveSearchTerms else { return false }
        return list.items.count > 1
    }

    func moveRow(at sourceIndexPath: IndexPath, to destinationIndexPath: IndexPath) {
        guard list.order == .listCustom else { return }
        guard !searchController.hasActiveSearchTerms else { return }
        guard sourceIndexPath != destinationIndexPath else { return }

        // Disable change notification updates
        let controllerDelegate = controller.delegate
        controller.delegate = nil

        sortManager.move(objectAt: sourceIndexPath, to: destinationIndexPath)
        list.managedObjectContext!.saveAndLogIfErrored()
        try! controller.performFetch()

        // Reneable change notification updates.
        controller.delegate = controllerDelegate
        UserEngagement.logEvent(.reorderList)

        // Delay slightly so that the UI update doesn't interfere with the animation of the row reorder completing.
        // This is quite ugly code, but leads to a less ugly UI.
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) { [unowned self] in
            self.updateData(animate: false)
        }
    }
}
