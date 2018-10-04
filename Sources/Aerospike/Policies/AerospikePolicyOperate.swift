import Foundation
import libaerospike

public class AerospikePolicyOperate: AerospikePolicy {
    public let commitLevel: AerospikeCommitLevel;
    public let consistencyLevel: AerospikeConsistencyLevel;
    public let deserialize: Bool;
    public let durableDelete: Bool;
    public let exists: AerospikePolicyExists; // .UPDATE
    public let gen: AerospikePolicyGen;  // .IGNORE
    public let key: AerospikePolicyKey;
    public let linearizeRead: Bool;
    public let replica: AerospikePolicyReplica;


    public init(
            commitLevel: AerospikeCommitLevel = .ALL,
            consistencyLevel: AerospikeConsistencyLevel = .ONE,
            deserialize: Bool = true,
            durableDelete: Bool = false,
            exists: AerospikePolicyExists = .UPDATE,
            gen: AerospikePolicyGen = .IGNORE,
            key: AerospikePolicyKey = .DIGEST,
            linearizeRead: Bool = false,
            replica: AerospikePolicyReplica = .SEQUENCE,
            maxRetries: UInt32 = 2,
            sleepBetweenRetries: UInt32 = 0,
            socketTimeout: UInt32 = 0,
            totalTimeout: UInt32 = 1000) {

        self.commitLevel = commitLevel;
        self.consistencyLevel = consistencyLevel;
        self.deserialize = deserialize;
        self.durableDelete = durableDelete;
        self.exists = exists;
        self.gen = gen;
        self.key = key;
        self.linearizeRead = linearizeRead;
        self.replica = replica;

        super.init(maxRetries: maxRetries, sleepBetweenRetries: sleepBetweenRetries, socketTimeout: socketTimeout, totalTimeout: totalTimeout);
    }

    public func getPolicy() -> as_policy_operate {
        var policy = as_policy_operate();
        policy.base = getBasePolicy();

        switch(self.commitLevel) {
            case .ALL    : policy.commit_level = AS_POLICY_COMMIT_LEVEL_ALL;
            case .MASTER : policy.commit_level = AS_POLICY_COMMIT_LEVEL_MASTER;
        }

        switch(self.consistencyLevel) {
            case .ONE : policy.consistency_level = AS_POLICY_CONSISTENCY_LEVEL_ONE;
            case .ALL : policy.consistency_level = AS_POLICY_CONSISTENCY_LEVEL_ALL;
        }

        policy.deserialize    = deserialize;
        policy.durable_delete = durableDelete;

        switch(self.exists) {
            case .IGNORE            : policy.exists = AS_POLICY_EXISTS_IGNORE;
            case .CREATE            : policy.exists = AS_POLICY_EXISTS_CREATE;
            case .UPDATE            : policy.exists = AS_POLICY_EXISTS_UPDATE;
            case .REPLACE           : policy.exists = AS_POLICY_EXISTS_REPLACE;
            case .CREATE_OR_REPLACE : policy.exists = AS_POLICY_EXISTS_CREATE_OR_REPLACE;
        }

        switch(self.gen) {
            case .IGNORE : policy.gen = AS_POLICY_GEN_IGNORE;
            case .EQ     : policy.gen = AS_POLICY_GEN_EQ;
            case .GT     : policy.gen = AS_POLICY_GEN_GT;
        }

        switch(self.key) {
            case .DIGEST : policy.key = AS_POLICY_KEY_DIGEST;
            case .SEND   : policy.key = AS_POLICY_KEY_SEND;
        }

        policy.linearize_read = linearizeRead;

        switch(self.replica) {
            case .MASTER   : policy.replica = AS_POLICY_REPLICA_MASTER;
            case .ANY      : policy.replica = AS_POLICY_REPLICA_ANY;
            case .SEQUENCE : policy.replica = AS_POLICY_REPLICA_SEQUENCE;
        }

        return policy;
    }
}
