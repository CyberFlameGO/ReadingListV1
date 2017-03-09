//
//  book.swift
//  books
//
//  Created by Andrew Bennet on 09/11/2015.
//  Copyright © 2015 Andrew Bennet. All rights reserved.
//

import Foundation
import CoreData

@objc(Book)
class Book: NSManagedObject {
    // Book Metadata
    @NSManaged private(set) var title: String
    @NSManaged private(set) var authorList: String?
    @NSManaged private(set) var isbn13: String?
    @NSManaged private(set) var pageCount: NSNumber?
    @NSManaged private(set) var publishedDate: Date?
    @NSManaged private(set) var bookDescription: String?
    @NSManaged private(set) var coverImage: Data?
    
    // Reading Information
    @NSManaged var readState: BookReadState
    @NSManaged var startedReading: Date?
    @NSManaged var finishedReading: Date?
    
    // Other Metadata
    @NSManaged var sort: NSNumber?
    
    func populate(from metadata: BookMetadata) {
        title = metadata.title
        authorList = metadata.authorList
        isbn13 = metadata.isbn13
        pageCount = metadata.pageCount as NSNumber?
        publishedDate = metadata.publishedDate
        bookDescription = metadata.bookDescription
        coverImage = metadata.coverImage
    }
    
    func populate(from readingInformation: BookReadingInformation) {
        readState = readingInformation.readState
        startedReading = readingInformation.startedReading
        finishedReading = readingInformation.finishedReading
        // Wipe out the sort if we have moved out of this section
        if readState != .toRead {
            sort = nil
        }
    }
    
    func setDate(_ date: Date, forState: BookReadState) {
        switch readState {
        case .reading:
            startedReading = date
        case .finished:
            finishedReading = date
        default:
            break
        }
    }
}



/// A mutable, non-persistent representation of the metadata fields of a Book object.
/// Useful for maintaining in-creation books, or books being edited.
class BookMetadata {
    var title: String!
    var isbn13: String?
    var authorList: String?
    var pageCount: Int?
    var publishedDate: Date?
    var bookDescription: String?
    var coverUrl: URL?
    var coverImage: Data?
}

/// A mutable, non-persistent representation of a the reading status of a Book object.
/// Useful for maintaining in-creation books, or books being edited.
class BookReadingInformation {
    let readState: BookReadState
    let startedReading: Date?
    let finishedReading: Date?
    
    /// Will only populate the start date if started; will only populate the finished date if finished
    init(readState: BookReadState, startedWhen: Date?, finishedWhen: Date?) {
        self.readState = readState
        switch readState {
        case .toRead:
            self.startedReading = nil
            self.finishedReading = nil
        case .reading:
            self.startedReading = startedWhen!
            self.finishedReading = nil
        case .finished:
            self.startedReading = startedWhen!
            self.finishedReading = finishedWhen!
        }
    }
}

/// The availale reading progress states
@objc enum BookReadState : Int32, CustomStringConvertible {
    case reading = 1
    case toRead = 2
    case finished = 3
    
    var description: String {
        switch self{
        case .reading:
            return "Reading"
        case .toRead:
            return "To Read"
        case .finished:
            return "Finished"
        }
    }
}
