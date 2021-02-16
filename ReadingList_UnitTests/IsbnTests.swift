import Foundation
@testable import ReadingList
import XCTest

class IsbnTests: XCTestCase {
    func testIsbnValidation() {
        XCTAssertFalse(ISBN13.isValid(9781111111111))
        XCTAssertTrue(ISBN13.isValid(9781787330672))
        XCTAssertTrue(ISBN13.isValid(9780330543934))
    }
}
