import Foundation

public struct Map : AsBin, CustomStringConvertible {
    public var map : [AnyHashable : AsBin] = [AnyHashable : AsBin]();

    public subscript<T>(index : AnyHashable) -> T? {
        set {
            if let newValue = newValue as? AsBin {
                self.map[index] = newValue
            }
        }
        get {
            return self.map[index] as? T;
        }
    }

    public init() {

    }

    public init(map: [AnyHashable : AsBin]) {
        self.map = map;
    }

    var count : Int {
        return self.map.count;
    }

    var values : Dictionary<AnyHashable, AsBin>.Values {
        return self.map.values;
    }

    var keys : Dictionary<AnyHashable, AsBin>.Keys {
        return self.map.keys;
    }

    mutating func remove(key : AnyHashable) {
        self.map.removeValue(forKey : key);
    }

    mutating func removeAll() {
        self.map.removeAll();
    }

    func get<T>(key : AnyHashable) -> T? {
        return map[key] as? T;
    }

    public var description: String {
        return self.map.description;
    }
}
