import Foundation
import libaerospike

public class AerospikePolicyRead: AerospikePolicy {
    public let consistencyLevel: AerospikeConsistencyLevel;
    public let replica: AerospikePolicyReplica;
    public let key: AerospikePolicyKey;
    public let deserialize: Bool;
    public let linearizeRead: Bool;

    public init(consistencyLevel: AerospikeConsistencyLevel = .ONE, deserialize: Bool = true, key: AerospikePolicyKey = .DIGEST, linearizeRead: Bool = false, replica: AerospikePolicyReplica = .SEQUENCE, maxRetries: UInt32 = 2, sleepBetweenRetries: UInt32 = 0, socketTimeout: UInt32 = 0, totalTimeout: UInt32 = 1000) {
        self.consistencyLevel = consistencyLevel;
        self.deserialize = deserialize;
        self.linearizeRead = linearizeRead;
        self.replica = replica;
        self.key = key;

        super.init(maxRetries: maxRetries, sleepBetweenRetries: sleepBetweenRetries, socketTimeout: socketTimeout, totalTimeout: totalTimeout);
    }

    public func getPolicy() -> as_policy_read {
        var policy = as_policy_read();
        policy.base = getBasePolicy();

        if (self.consistencyLevel == .ONE) {
            policy.consistency_level = AS_POLICY_CONSISTENCY_LEVEL_ONE;
        } else {
            policy.consistency_level = AS_POLICY_CONSISTENCY_LEVEL_ALL;
        }

        policy.deserialize   = deserialize;
        policy.linearize_read = linearizeRead;

        if (self.key == .DIGEST) {
            policy.key = AS_POLICY_KEY_DIGEST;
        } else {
            policy.key = AS_POLICY_KEY_SEND;
        }

        switch(self.replica) {
            case .MASTER   : policy.replica = AS_POLICY_REPLICA_MASTER;
            case .ANY      : policy.replica = AS_POLICY_REPLICA_ANY;
            case .SEQUENCE : policy.replica = AS_POLICY_REPLICA_SEQUENCE;
        }

        return policy;
    }
}
