import Foundation

public struct AsBin {
    public let value: Any;
    public let type: BinType;
    public let increment: Bool;

    public init(value: Any, type: BinType) {
        self.value = value;
        self.type  = type;
        self.increment = false;
    }

    public init(_ value: Int, increment: Bool = false) {
        self.value = Int64(value);
        self.type = .Integer;
        self.increment = increment;
    }

    public init(_ value: Int64, increment: Bool = false) {
        self.value = value;
        self.type = .Integer;
        self.increment = increment;
    }


    public init(_ value: Double, increment: Bool = false) {
        self.value = value;
        self.type = .Double;
        self.increment = increment;
    }

    public init(_ value: String) {
        self.value = value;
        self.type = .String;
        self.increment = false;
    }

    public init(_ value: Bool) {
        self.value = Int64(value ? 1 : 0);
        self.type = .Boolean;
        self.increment = false;
    }

    public var string  : String  { return self.get(); }
    public var boolean : Bool    { return self.get(); }
    public var double  : Double? { return self.get(); }
    public var integer : Int?    { return self.get(); }


    public func get() -> String {
        return "\(self.value)";
    }

    public func get() -> Double? {
        return self.value as? Double;
    }

    public func get() -> Int64? {
        return self.value as? Int64;
    }

    public func get() -> Int? {
        if let i: Int64 = self.get() {
            return Int(i);
        }

        return nil;
    }

    public func get() -> Bool {
        if let i: Int64 = self.get() {
            return i != 0;
        }

        return false;
    }

}

public enum BinType: UInt8 {
    case Undef   = 0;
    case Nil     = 1;
    case Boolean = 2;
    case Integer = 3;
    case String  = 4;
    case List    = 5;
    case Map     = 6;
    case Rec     = 7;
    case Pair    = 8;
    case Bytes   = 9;
    case Double  = 10;
    case GeoJson = 11;
};