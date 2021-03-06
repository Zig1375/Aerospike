import Foundation

class Utils {
    static func getText(_ buf : UnsafePointer<Int8>) -> String {
        if let utf8String = String.init(validatingUTF8 : buf) {
            return utf8String;
        }

        return "";
    }

    static func getText2<T>(_ to: inout T, _ size: Int) -> String {
        return withUnsafePointer(to: &to) {
            $0.withMemoryRebound(to: UInt8.self, capacity: size) {
                String(cString: $0)
            }
        }
    }

    static func stringToUPointer(_ str: String) -> UnsafeMutablePointer<UInt8>? {
        let arr : [UInt8] = Array(str.utf8);
        let buf = UnsafeMutablePointer<UInt8>.allocate(capacity: arr.count)
        memcpy(buf, arr, arr.count)
        return buf
    }
}
