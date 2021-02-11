import Foundation

public struct ListDictionary<Key, Value> where Key : Hashable {
    private var inner: [Key: [Value]] = [:]
    
    public init() { }

    public subscript(key: Key) -> [Value]? {
        get {
            inner[key]
        }
        set {
            inner[key] = newValue
        }
    }
    
    public mutating func removeAll() {
        inner.removeAll()
    }
    
    public mutating func append(to key: Key, _ value: Value) {
        func innerAdd(key: Key, array: inout [Value]?, value: Value) {
            if array == nil {
                inner[key] = [value]
            } else {
                array!.append(value)
            }
        }
    }
}
