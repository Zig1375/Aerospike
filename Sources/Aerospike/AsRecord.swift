import Foundation
import libaerospike

public class AsRecord: CustomStringConvertible {
    private let isRead: Bool;
    private var bins = [String: AsBin]();

    public subscript<T>(key: String) -> T? {
        get {
            return self.bins[key] as? T;
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
                    if (type == .List) {
                        if let bin = AsRecord.parseList(list: as_record_get_list(rec, name)) {
                            self.bins[name] = bin;
                        }
                    } else if (type == .Map) {
                        if let bin = AsRecord.parseMap(map: as_record_get_map(rec, name)) {
                            self.bins[name] = bin;
                        }
                    } else {
                        if let bin = parse(type: type, value: as_bin_get_value(bin)) {
                            self.bins[name] = bin;
                        }
                    }
                }
            }
        }

        as_record_iterator_destroy(&it);
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
                return (value.pointee.integer.value != 0);

            case .Integer:
                return Int(value.pointee.integer.value);

            case .Double:
                return value.pointee.dbl.value;

            case .String:
                return Utils.getText(value.pointee.string.value);

            default:
                return nil;
        }
    }

    static private func parseMap(map: UnsafeMutablePointer<as_map>) -> Map? {
        var it = as_map_iterator_u();
        as_map_iterator_init(&it, map);

        defer {
            as_hashmap_iterator_destroy(&it.hashmap);
        }

        var result = Map();
        while ( as_hashmap_iterator_has_next(&it.hashmap) ) {
            let val = as_hashmap_iterator_next(&it.hashmap);
            let pair = as_pair_fromval(val);

            guard let val_key = as_pair_1(pair), let val_val = as_pair_2(pair), let key : AnyHashable = AsRecord.parseAsVal(val: val_key) as? AnyHashable else {
                return nil;
            }

            if let bin = AsRecord.parseAsVal(val: val_val) {
                result[key] = bin;
            } else {
                return nil;
            }
        }

        return result;
    }

    static private func parseList(list: UnsafeMutablePointer<as_list>) -> List? {
        var it = as_list_iterator_u();
        as_list_iterator_init(&it, list);

        defer {
            as_arraylist_iterator_destroy(&it.arraylist);
        }

        var result = List();
        while ( as_arraylist_iterator_has_next(&it.arraylist) ) {
            if let val = as_arraylist_iterator_next(&it.arraylist), let bin = AsRecord.parseAsVal(val: val) {
                result.append(bin);
            } else {
                return nil;
            }
        }

        return result;
    }

    static public func parseAsVal(val: UnsafePointer<as_val>) -> AsBin? {
        guard let type = BinType(rawValue: val.pointee.type) else {
            return nil;
        }

        switch(type) {
            case .Integer:
                if let t = as_integer_fromval(val) {
                    return Int(as_integer_get(t));
                }

            case .Double:
                if let t = as_double_fromval(val) {
                    return as_double_get(t);
                }

            case .String:
                if let t = as_string_fromval(val), let s = as_string_get(t) {
                    return Utils.getText(s);
                }

            case .List:
                let m = UnsafeMutablePointer<as_val>(mutating: val);
                if let t = as_list_fromval(m), let bin = AsRecord.parseList(list: t) {
                    return bin;
                }

            case .Map:
                let m = UnsafeMutablePointer<as_val>(mutating: val);
                if let t = as_map_fromval(m), let bin = AsRecord.parseMap(map: t) {
                    return bin;
                }

            default:
                return nil;
        }

        return nil;
    }

    public var description: String {
        return self.bins.description;
    }
}
