import Foundation
import CoreData
import Combine

extension NSManagedObjectContext {
    typealias ObjectIDsPublisher = AnyPublisher<Set<NSManagedObjectID>, NotificationCenter.Publisher.Failure>

    func savedUpdatedObjectsPublisher() -> ObjectIDsPublisher {
        NotificationCenter.default.publisher(for: .NSManagedObjectContextDidSave, object: self)
            .compactMap { notification -> Set<NSManagedObjectID>? in
                guard let userInfo = notification.userInfo,
                      let updatedObjects = userInfo[NSUpdatedObjectsKey] as? Set<NSManagedObject> else { return nil }
                return Set(updatedObjects.map(\.objectID))
            }.eraseToAnyPublisher()
    }

    func mergedUpdatedObjectsPublisher() -> ObjectIDsPublisher {
        NotificationCenter.default.publisher(for: .NSManagedObjectContextDidMergeChangesObjectIDs, object: self)
            .compactMap { notification -> Set<NSManagedObjectID>? in
                guard let userInfo = notification.userInfo else { return nil }
                return userInfo[NSUpdatedObjectIDsKey] as? Set<NSManagedObjectID>
            }.eraseToAnyPublisher()
    }

    func savedDeletedObjectsPublisher() -> ObjectIDsPublisher {
        NotificationCenter.default.publisher(for: .NSManagedObjectContextDidSave, object: self)
            .compactMap { notification -> Set<NSManagedObjectID>? in
                guard let userInfo = notification.userInfo,
                      let updatedObjects = userInfo[NSDeletedObjectsKey] as? Set<NSManagedObject> else { return nil }
                return Set(updatedObjects.map(\.objectID))
            }.eraseToAnyPublisher()
    }

    func mergedDeletedObjectsPublisher() -> ObjectIDsPublisher {
        NotificationCenter.default.publisher(for: .NSManagedObjectContextDidMergeChangesObjectIDs, object: self)
            .compactMap { notification -> Set<NSManagedObjectID>? in
                guard let userInfo = notification.userInfo else { return nil }
                return userInfo[NSDeletedObjectIDsKey] as? Set<NSManagedObjectID>
            }.eraseToAnyPublisher()
    }

    func updatedObjectsPublisher() -> ObjectIDsPublisher {
        Publishers.Merge(savedUpdatedObjectsPublisher(), mergedUpdatedObjectsPublisher())
            .eraseToAnyPublisher()
    }

    func deletedObjectsPublisher() -> ObjectIDsPublisher {
        Publishers.Merge(savedDeletedObjectsPublisher(), mergedDeletedObjectsPublisher())
            .eraseToAnyPublisher()
    }
}
