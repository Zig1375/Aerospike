import Foundation

public struct List : AsBin, CustomStringConvertible {
    public var array : [AsBin] = [AsBin]();

    subscript<T>(index : Int) -> T? {
        set {
            if let newValue = newValue as? AsBin {
                self.array[index] = newValue;
            }
        }
        get {
            return self.array[index] as? T;
        }
    }

    public init() {

    }

    public init(array: [AsBin]) {
        self.array = array;
    }

    var count : Int {
        return self.array.count;
    }

    mutating func append(_ value : AsBin) {
        self.array.append(value);
    }

    mutating func remove(_ index : Int) -> AsBin? {
        return self.array.remove(at : index);
    }

    public var description: String {
        return self.array.description;
    }
}
