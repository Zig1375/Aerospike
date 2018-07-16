import Foundation
import libaerospike

public class AsRecord {
    private let isRead: Bool;
    private var bins = [String: AsBin]();

    public subscript(key: String) -> AsBin? {
        get {
            return self.bins[key];
        }
    }

    public init() {
        self.isRead = false;
    }

    public init(rec: UnsafeMutablePointer<as_record>?) {
        self.isRead = true;
        var it = as_record_iterator();
        as_record_iterator_init(&it, rec);

        while ( as_record_iterator_has_next(&it) ) {
            let bin = as_record_iterator_next(&it);
            let binName = as_bin_get_name(bin);

            if let binName = binName {
                let name = Utils.getText(binName);

                if let type = BinType(rawValue: as_bin_get_type(bin)) {
                    if let bin = parse(type: type, value: as_bin_get_value(bin)) {
                        self.bins[name] = bin;
                    }
                }
            }
        }
    }

    public func add(name: String, bin: AsBin) {
        if (self.isRead) {
            return;
        }

        self.bins[name] = bin;
    }

    public func getBins() -> [String: AsBin] {
        return self.bins;
    }

    private func parse(type: BinType, value: UnsafeMutablePointer<as_bin_value>) -> AsBin? {
        switch (type) {
            case .Boolean:
                let b: Bool = (value.pointee.integer.value != 0);
                return AsBin(value: b, type: .Boolean);

            case .Integer:
                return AsBin(value: value.pointee.integer.value, type: .Integer);

            case .Double:
                return AsBin(value: value.pointee.dbl.value, type: .Double);

            case .String:
                return AsBin(value: Utils.getText(value.pointee.string.value), type: .String);

            default:
                return nil;
        }
    }
}
