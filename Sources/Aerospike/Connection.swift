import Foundation
import libaerospike

public class Connection {
    private var conn: aerospike;
    private let namespace: String?;
    private var error: AerospikeError? = nil;
    private var isOpen: Bool = false;

    public init(host: String, port: UInt16 = 3000, namespace: String? = nil) throws {
        self.namespace = namespace;

        var config: as_config = as_config();
        var err = as_error();

        as_config_init(&config);
        as_config_add_host(&config, host, port);

        self.conn = aerospike();
        aerospike_init(&self.conn, &config);

        if (aerospike_connect(&self.conn, &err) != AEROSPIKE_OK) {
            throw AerospikeError.Error(message: Utils.getText2(&err.message, 1024), code: err.code.rawValue)
        }

        self.isOpen = true;
    }

    deinit {
        if (self.isOpen) {
            var err = as_error();
            aerospike_close(&self.conn, &err);
            aerospike_destroy(&self.conn);
        }
    }

    public func close() {
        var err = as_error();
        aerospike_close(&self.conn, &err);
        aerospike_destroy(&self.conn);

        self.isOpen = false;
    }

    public func get(namespace: String? = nil, set: String, key: String, policy: AerospikePolicyRead? = nil) -> AsRecord? {
        guard let ns = namespace ?? self.namespace else {
            self.error = AerospikeError.Required(message: "Required namespace");
            return nil;
        }

        var asKey: as_key = as_key();
        as_key_init(&asKey, ns, set, key);

        defer {
            as_key_destroy(&asKey);
        }

        var p_rec: UnsafeMutablePointer<as_record>? = nil;
        var err = as_error();


        let result: as_status;
        if let p = policy {
            var pp = p.getPolicy();
            result = aerospike_key_get(&self.conn, &err, &pp, &asKey, &p_rec)
        } else {
            result = aerospike_key_get(&self.conn, &err, nil, &asKey, &p_rec)
        }

        switch(result) {
            case AEROSPIKE_OK:
                break;

            case AEROSPIKE_ERR_RECORD_NOT_FOUND:
                return nil;

            default:
                self.error = AerospikeError.Error(message: Utils.getText2(&err.message, 1024), code: err.code.rawValue);
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

    public func set(namespace: String? = nil, set: String, key: String, record: AsRecord, policy: AerospikePolicyOperate? = nil) {
        guard let ns = namespace ?? self.namespace else {
            self.error = AerospikeError.Required(message: "Required namespace");
            return;
        }

        let bins = record.getBins();
        if (bins.count == 0) {
            self.error = AerospikeError.Required(message: "No any Bin");
            return;
        }

        var asKey: as_key = as_key();
        as_key_init(&asKey, ns, set, key);

        defer {
            as_key_destroy(&asKey);
        }

        var ops = as_operations();
        as_operations_init(&ops, UInt16(bins.count));

        for (name, bin) in bins {
            switch (bin) {
                case is Int:
                    as_operations_add_write_int64(&ops, name, Int64(bin as! Int));

                case is Double:
                    as_operations_add_write_double(&ops, name, bin as! Double);

                case is String:
                    as_operations_add_write_str(&ops, name, bin as! String);

                case is Bool:
                    as_operations_add_write_int64(&ops, name, (bin as! Bool) ? 1 : 0 );

                case is List:
                    break;

                case is Map:
                    break;

                default:
                    break;
            }
        }

        var rec : UnsafeMutablePointer<as_record>? = nil;
        var err = as_error();

        let result: as_status;
        if let p = policy {
            var pp = p.getPolicy();
            result = aerospike_key_operate(&self.conn, &err, &pp, &asKey, &ops, &rec)
        } else {
            result = aerospike_key_operate(&self.conn, &err, nil, &asKey, &ops, &rec)
        }

        if (result == AEROSPIKE_OK) {
            as_record_destroy(&rec!.pointee);
        } else {
            self.error = AerospikeError.Error(message: Utils.getText2(&err.message, 1024), code: err.code.rawValue);
            return;
        }

        as_operations_destroy(&ops);
    }

    public func udfRegister(name: String, content: String) -> Bool {
        guard let umpContent = Utils.stringToUPointer(content) else {
            return false;
        }

        var udf_content = as_bytes();
        as_bytes_init_wrap(&udf_content, umpContent, UInt32(content.utf8.count), true);

        var err = as_error();
        if (aerospike_udf_put(&self.conn, &err, nil, name, AS_UDF_TYPE_LUA, &udf_content) != AEROSPIKE_OK) {
            self.error = AerospikeError.Error(message: Utils.getText2(&err.message, 1024), code: err.code.rawValue);
            return false;
        }

        as_bytes_destroy(&udf_content);
        return true;
    }

    public func udfRemove(name: String) -> Bool {
        var err = as_error();
        if (aerospike_udf_remove(&self.conn, &err, nil, name) != AEROSPIKE_OK) {
            self.error = AerospikeError.Error(message: Utils.getText2(&err.message, 1024), code: err.code.rawValue);
            return false;
        }

        return true;
    }

    public func udfApply(module: String, func fname: String, namespace: String? = nil, set: String, key: String, args: [AsBin] = [], policy: AerospikePolicyApply? = nil) -> AsBin? {
        guard let ns = namespace ?? self.namespace else {
            self.error = AerospikeError.Required(message: "Required namespace");
            return nil;
        }

        var asKey: as_key = as_key();
        as_key_init(&asKey, ns, set, key);

        var asArgs = as_arraylist();
        as_arraylist_init(&asArgs, UInt32(args.count), 0);
        for bin in args {
            switch(bin) {
                case is String:
                    as_arraylist_append_str(&asArgs, bin as! String);

                case is Int:
                    as_arraylist_append_int64(&asArgs, Int64(bin as! Int));

                case is Double:
                    as_arraylist_append_double(&asArgs, bin as! Double);

                default:
                    self.error = AerospikeError.Required(message: "Unsupported type in args");
                    return nil;
            }
        }

        var p_result: UnsafeMutablePointer<as_val>? = nil;

        defer {
            as_arraylist_destroy(&asArgs);
            as_key_destroy(&asKey);

            if (p_result != nil) {
                as_val_val_destroy(&p_result!.pointee);
            }
        }

        var err = as_error();
        let result: as_status;
        if let p = policy {
            var pp = p.getPolicy();
            result = aerospike_key_apply2(&self.conn, &err, &pp, &asKey, module, fname, &asArgs, &p_result);
        } else {
            result = aerospike_key_apply2(&self.conn, &err, nil, &asKey, module, fname, &asArgs, &p_result);
        }

        if (result != AEROSPIKE_OK) {
            self.error = AerospikeError.Error(message: Utils.getText2(&err.message, 1024), code: err.code.rawValue);
            return nil;
        }

        if let rval = p_result {
            return AsRecord.parseAsVal(val: UnsafePointer(rval))
        }

        return nil;
    }

    public func getLastError() -> AerospikeError? {
        let err = self.error;
        self.error = nil;

        return err;
    }
}
