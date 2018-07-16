import Foundation

public struct AsBin {
    public let value: Any;
    public let type: BinType;

    public init(value: Any, type: BinType) {
        self.value = value;
        self.type  = type;
    }

    public init(_ value: Int) {
        self.value = Int64(value);
        self.type = .Integer;
    }

    public init(_ value: Int64) {
        self.value = value;
        self.type = .Integer;
    }


    public init(_ value: Double) {
        self.value = value;
        self.type = .Double;
    }

    public init(_ value: String) {
        self.value = value;
        self.type = .String;
    }

    public init(_ value: Bool) {
        self.value = Int64(value ? 1 : 0);
        self.type = .Boolean;
    }

    var string  : String  { return self.get(); }
    var boolean : Bool    { return self.get(); }
    var double  : Double? { return self.get(); }
    var integer : Int?    { return self.get(); }


    func get() -> String {
        return "\(self.value)";
    }

    func get() -> Double? {
        return self.value as? Double;
    }

    func get() -> Int64? {
        return self.value as? Int64;
    }

    func get() -> Int? {
        if let i: Int64 = self.get() {
            return Int(i);
        }

        return nil;
    }

    func get() -> Bool {
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