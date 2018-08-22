public enum AerospikeError: Error {
    case Error(message: String, code : Int32);
    case Required(message: String);
}
