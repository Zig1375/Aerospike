import Foundation
import libaerospike

public class AerospikePolicyApply: AerospikePolicy {
    public let commitLevel: AerospikeCommitLevel;
    public let durableDelete: Bool;
    public let gen: AerospikePolicyGen;
    public let genValue: UInt16;
    public let key: AerospikePolicyKey;
    public let linearizeRead: Bool;
    public let replica: AerospikePolicyReplica;
    public let ttl: UInt32;

    public init(
            commitLevel: AerospikeCommitLevel = .ALL,
            durableDelete: Bool = false,
            gen: AerospikePolicyGen = .IGNORE,
            genValue: UInt16 = 0,
            key: AerospikePolicyKey = .DIGEST,
            linearizeRead: Bool = false,
            replica: AerospikePolicyReplica = .SEQUENCE,
            ttl: UInt32 = 2,
            maxRetries: UInt32 = 2,
            sleepBetweenRetries: UInt32 = 0,
            socketTimeout: UInt32 = 0,
            totalTimeout: UInt32 = 1000) {

        self.commitLevel = commitLevel;
        self.durableDelete = durableDelete;
        self.gen = gen;
        self.genValue = genValue;
        self.key = key;
        self.linearizeRead = linearizeRead;
        self.replica = replica;
        self.ttl = ttl;

        super.init(maxRetries: maxRetries, sleepBetweenRetries: sleepBetweenRetries, socketTimeout: socketTimeout, totalTimeout: totalTimeout);
    }

    public func getPolicy() -> as_policy_apply {
        var policy = as_policy_apply();
        policy.base = getBasePolicy();

        switch(self.commitLevel) {
            case .ALL    : policy.commit_level = AS_POLICY_COMMIT_LEVEL_ALL;
            case .MASTER : policy.commit_level = AS_POLICY_COMMIT_LEVEL_MASTER;
        }

        policy.durable_delete = self.durableDelete;

        switch(self.gen) {
            case .IGNORE : policy.gen = AS_POLICY_GEN_IGNORE;
            case .EQ     : policy.gen = AS_POLICY_GEN_EQ;
            case .GT     : policy.gen = AS_POLICY_GEN_GT;
        }

        policy.gen_value = self.genValue;

        switch(self.key) {
            case .DIGEST : policy.key = AS_POLICY_KEY_DIGEST;
            case .SEND   : policy.key = AS_POLICY_KEY_SEND;
        }

        policy.linearize_read = self.linearizeRead;

        switch(self.replica) {
            case .MASTER   : policy.replica = AS_POLICY_REPLICA_MASTER;
            case .ANY      : policy.replica = AS_POLICY_REPLICA_ANY;
            case .SEQUENCE : policy.replica = AS_POLICY_REPLICA_SEQUENCE;
        }
        policy.ttl = self.ttl;

        return policy;
    }
}
