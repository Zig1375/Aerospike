import Foundation
import libaerospike

public class Connection {
    private var conn: aerospike;
    private var err: as_error;
    private let namespace: String?;

    public init?(host: String, port: UInt16 = 3000, namespace: String? = nil) {
        self.namespace = namespace;

        var config: as_config = as_config();
        self.err = as_error();

        as_config_init(&config);
        as_config_add_host(&config, host, port);

        self.conn = aerospike();
        aerospike_init(&self.conn, &config);

        if (aerospike_connect(&self.conn, &self.err) != AEROSPIKE_OK) {
            print("AEROSPIKE => CONNECT ERROR(\(self.err.code.rawValue)) \(Utils.getText2(&self.err.message, 1024))");
            return nil;
        }
    }

    deinit {
        aerospike_close(&self.conn, &self.err);
        aerospike_destroy(&self.conn);
    }

    public func get(namespase: String? = nil, set: String, key: String) -> AsRecord? {
        guard let ns = namespase ?? self.namespace else {
            print("AEROSPIKE => Namespace is required");
            return nil;
        }

        var asKey: as_key = as_key();
        as_key_init_str(&asKey, ns, set, key);

        var p_rec: UnsafeMutablePointer<as_record>? = nil;

        switch(aerospike_key_get(&self.conn, &self.err, nil, &asKey, &p_rec)) {
            case AEROSPIKE_OK:
                break;

            case AEROSPIKE_ERR_RECORD_NOT_FOUND:
                print("AEROSPIKE => key: \(key) => NOT FOUND");
                return nil;

            default:
                print("AEROSPIKE => key: \(key) => ERROR(\(self.err.code.rawValue)) \(Utils.getText2(&self.err.message, 1024))");
                return nil;
        }

        let record: AsRecord?;
        if let p_rec = p_rec {
            record = AsRecord(rec: p_rec);
        } else {
            record = nil;
        }

        as_record_destroy(p_rec);
        return record;
    }

    public func set(namespase: String? = nil, set: String, key: String, record: AsRecord) {
        guard let ns = namespase ?? self.namespace else {
            print("AEROSPIKE => Namespace is required");
            return;
        }

        let bins = record.getBins();
        if (bins.count == 0) {
            print("AEROSPIKE => No any Bin");
            return;
        }

        var asKey: as_key = as_key();
        as_key_init_str(&asKey, ns, set, key);

        var ops = as_operations();
        as_operations_init(&ops, UInt16(bins.count));

        for (name, bin) in bins {
            switch (bin.type) {
                case .Integer:
                    if (bin.increment) {
                        as_operations_add_incr(&ops, name, bin.get()!);
                    } else {
                        as_operations_add_write_int64(&ops, name, bin.get()!);
                    }

                case .Double:
                    if (bin.increment) {
                        as_operations_add_incr_double(&ops, name, bin.get()!);
                    } else {
                        as_operations_add_write_double(&ops, name, bin.get()!);
                    }


                case .String:
                    as_operations_add_write_str(&ops, name, bin.string);

                case .Boolean:
                    as_operations_add_write_int64(&ops, name, (bin.boolean) ? 1 : 0 );

                default:
                    break;
            }
        }

        var rec : UnsafeMutablePointer<as_record>? = nil;
        if (aerospike_key_operate(&self.conn, &self.err, nil, &asKey, &ops, &rec) == AEROSPIKE_OK) {
            as_record_destroy(&rec!.pointee);
        } else {
            print("AEROSPIKE => key: \(key) => ERROR(\(self.err.code.rawValue)) \(Utils.getText2(&self.err.message, 1024))");
        }

        as_operations_destroy(&ops);
    }


    public func udfRegister(name: String, content: String) -> Bool {
        guard let umpContent = Utils.stringToUPointer(content) else {
            return false;
        }

        var result = true;
        var udf_content = as_bytes();
        as_bytes_init_wrap(&udf_content, umpContent, UInt32(content.utf8.count), true);

        if (aerospike_udf_put(&self.conn, &self.err, nil, name, AS_UDF_TYPE_LUA, &udf_content) != AEROSPIKE_OK) {
            print("AEROSPIKE => aerospike_udf_put => ERROR(\(self.err.code.rawValue)) \(Utils.getText2(&self.err.message, 1024))");
            result = false;
        }

        as_bytes_destroy(&udf_content);
        return result;
    }

    public func udfRemove(name: String) -> Bool {
        if (aerospike_udf_remove(&self.conn, &self.err, nil, name) != AEROSPIKE_OK) {
            print("AEROSPIKE => aerospike_udf_remove => ERROR(\(self.err.code.rawValue)) \(Utils.getText2(&self.err.message, 1024))");
            return false;
        }

        return true;
    }

    public func udfApply(module: String, func fname: String, namespase: String? = nil, set: String, key: String, args: [AsBin] = []) -> AsBin? {
        guard let ns = namespase ?? self.namespace else {
            print("AEROSPIKE => Namespace is required");
            return nil;
        }

        var asKey: as_key = as_key();
        as_key_init_str(&asKey, ns, set, key);

        var asArgs = as_arraylist();
        as_arraylist_init(&asArgs, UInt32(args.count), 0);
        for bin in args {
            switch(bin.type) {
                case .String:
                    as_arraylist_append_str(&asArgs, bin.string);

                case .Integer:
                    if let i = bin.integer {
                        as_arraylist_append_int64(&asArgs, Int64(i));
                    } else {
                        print("Incorrect value in args");
                        return nil;
                    }

                case .Double:
                    if let i = bin.double {
                        as_arraylist_append_double(&asArgs, i);
                    } else {
                        print("Incorrect value in args");
                        return nil;
                    }

                default:
                    print("Unsupported type in args");
                    return nil;
            }

        }

        var p_result: UnsafeMutablePointer<as_val>? = nil;
        if (aerospike_key_apply(&self.conn, &self.err, nil, &asKey, module, fname, castArrayListToList(&asArgs), &p_result) != AEROSPIKE_OK) {
            print("AEROSPIKE => aerospike_key_apply => ERROR(\(self.err.code.rawValue)) \(Utils.getText2(&self.err.message, 1024))");
            return nil;
        }

        if let rval = p_result {
            return AsRecord.valToBin(val: UnsafePointer(rval))
        }

        return nil;
    }
}
