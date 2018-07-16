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

        var rec = as_record();
        as_record_init(&rec, UInt16(bins.count));

        for (name, bin) in bins {
            switch (bin.type) {
                case .Integer:
                    as_record_set_integer(&rec, name, as_integer_new(bin.get()!));
                    break;

                case .Double:
                    as_record_set_double(&rec, name, bin.double!);

                case .String:
                    as_record_set_str(&rec, name, bin.string);

                case .Boolean:
                    as_record_set_integer(&rec, name, as_integer_new( (bin.boolean) ? 1 : 0 ));

                default:
                    break;
            }
        }

        if (aerospike_key_put(&self.conn, &self.err, nil, &asKey, &rec) != AEROSPIKE_OK) {
            print("AEROSPIKE => key: \(key) => ERROR(\(self.err.code.rawValue)) \(Utils.getText2(&self.err.message, 1024))");
        }
        as_record_destroy(&rec);
    }
}
