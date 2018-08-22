public enum AerospikeError: Error {
    case Error(message: String, code : UInt32);
    case Required(message: String);
}
