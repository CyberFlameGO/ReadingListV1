import Foundation
import CoreData
import WidgetKit
import ImageIO
import UIKit
import Combine

@available(iOS 14.0, *)
class BookDataSharer {
    private init() {}

    static var instance = BookDataSharer()
    var persistentContainer: NSPersistentContainer!
    private var cancellables = Set<AnyCancellable>()

    func inititialise(persistentContainer: NSPersistentContainer) {
        self.persistentContainer = persistentContainer
        NotificationCenter.default.publisher(for: .NSManagedObjectContextDidSave, object: persistentContainer.viewContext)
            .sink { [weak self] _ in
                self?.handleChanges()
            }
            .store(in: &cancellables)
        NotificationCenter.default.publisher(for: .NSManagedObjectContextDidMergeChangesObjectIDs, object: persistentContainer.viewContext)
            .sink { [weak self] _ in
                self?.handleChanges()
            }
            .store(in: &cancellables)
        handleChanges(forceUpdate: false)
    }

    func stop() {
        self.persistentContainer = nil
        cancellables.forEach { $0.cancel() }
    }

    private let bookRetrievalCount = 8

    @objc func handleChanges(forceUpdate: Bool = false) {
        let backgroundContext = persistentContainer.newBackgroundContext()
        backgroundContext.perform { [unowned self] in
            let readingFetchRequest = fetchRequest(context: backgroundContext, itemLimit: bookRetrievalCount, readState: .reading)
            var currentBooks = try! backgroundContext.fetch(readingFetchRequest)

            if currentBooks.count < bookRetrievalCount {
                let toReadFetchRequest = fetchRequest(context: backgroundContext, itemLimit: bookRetrievalCount - currentBooks.count, readState: .toRead)
                currentBooks.append(contentsOf: try! backgroundContext.fetch(toReadFetchRequest))
            }

            let finishedBooksRequest = fetchRequest(context: backgroundContext, itemLimit: bookRetrievalCount, readState: .finished, sortOrderOverride: .finishDate)
            let finishedBooks = try! backgroundContext.fetch(finishedBooksRequest)

            let currentBooksData = currentBooks.map { $0.buildSharedData() }
            let finishedBooksData = finishedBooks.map { $0.buildSharedData() }

            if forceUpdate || currentBooksData != SharedBookData.currentBooks {
                logger.info("Updating and reloading Current Books widget timelines")
                DispatchQueue.main.async {
                    SharedBookData.currentBooks = currentBooksData
                    WidgetCenter.shared.reloadTimelines(ofKind: WidgetKind.currentBooks)
                }
            }
            if forceUpdate || finishedBooksData != SharedBookData.finishedBooks {
                logger.info("Updating and reloading Finished Books widget timelines")
                DispatchQueue.main.async {
                    SharedBookData.finishedBooks = finishedBooksData
                    WidgetCenter.shared.reloadTimelines(ofKind: WidgetKind.finishedBooks)
                }
            }
        }
    }

    private func fetchRequest(context: NSManagedObjectContext, itemLimit: Int, readState: BookReadState, sortOrderOverride: BookSort? = nil) -> NSFetchRequest<Book> {
        let fetchRequest = NSFetchRequest<Book>()
        fetchRequest.entity = Book.entity(in: context)
        fetchRequest.fetchLimit = itemLimit
        fetchRequest.returnsObjectsAsFaults = false
        fetchRequest.predicate = NSPredicate(format: "%K == %ld", #keyPath(Book.readState), readState.rawValue)
        if let sortOrderOverride = sortOrderOverride {
            fetchRequest.sortDescriptors = sortOrderOverride.bookSortDescriptors
        } else {
            fetchRequest.sortDescriptors = BookSort.byReadState[readState]!.bookSortDescriptors
        }
        return fetchRequest
    }
}

fileprivate extension Book {
    var identifier: BookIdentifier {
        if let googleBooksId = googleBooksId {
            return .googleBooksId(googleBooksId)
        } else if let manualBookId = manualBookId {
            return .manualId(manualBookId)
        } else {
            preconditionFailure()
        }
    }

    private func getCoverImageDataToUse() -> Data? {
        guard let coverImage = coverImage else { return nil }

        // Only resize the image if is more than 100KB in size
        guard coverImage.count > 100 * 1024 else { return coverImage }
        logger.info("Generating a thumbnail image for cover data (initial size \(coverImage.count)")

        guard let image = UIImage(data: coverImage) else {
            logger.error("Could not initialise image from data")
            return nil
        }

        // A large file-size image but with small width... if this happens, let's just skip the image.
        guard image.size.height > 100 else {
            logger.error("Unexpected image size when generating a thumbnail: \(image.size.width) x \(image.size.height)")
            return nil
        }

        // Use ImageIO to generate a small thumbnail image. We do this so that we don't stuff massive images in the
        // shared data and also in the SwiftUI serialised view. We suspect that iOS throttled the updates when they were
        // large in size.
        let options = [
            kCGImageSourceCreateThumbnailWithTransform: true,
            kCGImageSourceCreateThumbnailFromImageAlways: true,
            kCGImageSourceThumbnailMaxPixelSize: 100 // Max permitted image height by any widget is 100
        ] as CFDictionary

        guard let imageSource = CGImageSourceCreateWithData(coverImage as NSData, options),
              let imageReference = CGImageSourceCreateThumbnailAtIndex(imageSource, 0, options) else {
            logger.error("Error generating image thumbnail")
            return nil
        }
        guard let thumbnailData = UIImage(cgImage: imageReference).pngData() else {
            logger.error("Thumbnail image generation returned no data")
            return nil
        }
        logger.error("Thumbnail image generated with size \(thumbnailData.count)")
        return thumbnailData
    }

    func buildSharedData() -> SharedBookData {
        SharedBookData(
            title: title,
            authorDisplay: authors.fullNames,
            identifier: identifier,
            coverImage: getCoverImageDataToUse(),
            percentageComplete: Int(currentPercentage),
            startDate: startedReading,
            finishDate: finishedReading
        )
    }
}
