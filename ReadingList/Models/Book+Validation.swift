import Foundation

extension Book {
    @objc func validateAuthors(_ value: AutoreleasingUnsafeMutablePointer<AnyObject?>) throws {
        // nil authors property will be validated by the validation set on the model
        guard let authors = value.pointee as? [Author] else { return }
        if authors.isEmpty {
            throw BookValidationError.noAuthors
        }
    }

    @objc func validateTitle(_ value: AutoreleasingUnsafeMutablePointer<AnyObject?>) throws {
        // nil title property will be validated by the validation set on the model
        guard let title = value.pointee as? String else { return }
        if title.isEmptyOrWhitespace {
            throw BookValidationError.missingTitle
        }
    }

    @objc func validateIsbn13(_ value: AutoreleasingUnsafeMutablePointer<AnyObject?>) throws {
        guard let isbn13 = value.pointee as? Int64 else { return }
        if !ISBN13.isValid(isbn13) {
            throw BookValidationError.invalidIsbn
        }
    }

    @objc func validateLanguageCode(_ value: AutoreleasingUnsafeMutablePointer<AnyObject?>) throws {
        guard let languageCode = value.pointee as? String else { return }
        if LanguageIso639_1(rawValue: languageCode) == nil {
            throw BookValidationError.invalidLanguageCode
        }
    }

    override func validateForUpdate() throws {
        try super.validateForUpdate()
        try interPropertyValiatation()
    }

    override func validateForInsert() throws {
        try super.validateForInsert()
        try interPropertyValiatation()
    }

    func interPropertyValiatation() throws {
        switch readState {
        case .toRead:
            if startedReading != nil || finishedReading != nil {
                throw BookValidationError.invalidReadDates
            }
        case .reading:
            if startedReading == nil || finishedReading != nil {
                throw BookValidationError.invalidReadDates
            }
        case .finished:
            if startedReading == nil || finishedReading == nil {
                throw BookValidationError.invalidReadDates
            }
        }
        if readState != .reading && (currentPage != nil || currentPercentage != nil) {
            throw BookValidationError.presentCurrentPage
        }
        if googleBooksId == nil && manualBookId == nil {
            throw BookValidationError.missingIdentifier
        }
    }
}

enum BookValidationError: Int, Error {
    case missingTitle = 1
    case invalidIsbn = 2
    case invalidReadDates = 3
    case invalidLanguageCode = 4
    case missingIdentifier = 5
    case noAuthors = 6
    case presentCurrentPage = 7
}
