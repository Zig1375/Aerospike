import Foundation
import libaerospike


public class AerospikePolicy {
    public let maxRetries: UInt32;
    public let sleepBetweenRetries: UInt32;
    public let socketTimeout: UInt32;
    public let totalTimeout: UInt32;

    public init(maxRetries: UInt32, sleepBetweenRetries: UInt32 = 0, socketTimeout: UInt32 = 0, totalTimeout: UInt32 = 1000) {
        self.maxRetries          = maxRetries;
        self.sleepBetweenRetries = sleepBetweenRetries;
        self.socketTimeout       = socketTimeout;
        self.totalTimeout        = totalTimeout;
    }

    public func getBasePolicy() -> as_policy_base{
        var base = as_policy_base();
        base.max_retries = self.maxRetries;
        base.sleep_between_retries = self.sleepBetweenRetries;
        base.socket_timeout = self.socketTimeout;
        base.total_timeout = self.totalTimeout;

        return base;
    }
}

public enum AerospikeConsistencyLevel {
    case ONE;
    case ALL;
}

public enum AerospikePolicyReplica {
    case MASTER;
    case ANY;
    case SEQUENCE;
}

public enum AerospikePolicyKey {
    case SEND;
    case DIGEST;
}

public enum AerospikePolicyGen {
    case IGNORE;
    case EQ;
    case GT;
}

public enum AerospikePolicyExists {
    case IGNORE;
    case CREATE;
    case UPDATE;
    case REPLACE;
    case CREATE_OR_REPLACE;
}

public enum AerospikeCommitLevel {
    case ALL;
    case MASTER;
}