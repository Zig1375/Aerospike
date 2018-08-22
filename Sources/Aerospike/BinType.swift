import Foundation

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