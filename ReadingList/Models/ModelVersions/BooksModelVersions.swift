import Foundation
import CoreData

enum BooksModelVersion: Int, CaseIterable {
    case version5 = 5
    case version6 = 6
    case version7 = 7
    case version8 = 8
    case version9 = 9
    case version10 = 10
    case version11 = 11
    case version12 = 12
    case version13 = 13
    case version14 = 14
    case version15 = 15
    case version16 = 16
    case version17 = 17
    case version18 = 18
    case version19 = 19
}

extension BooksModelVersion: ModelVersion {
    var modelName: String { "books_\(rawValue)" }
    var mappingModelToSuccessorName: String { "BookMapping_\(rawValue)_\(rawValue + 1)" }
    static var modelBundle: Bundle { Bundle(for: Book.self) }
    static var modelDirectoryName: String { "books.momd" }
}
