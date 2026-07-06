enum KVStore {
    LinKV = "lin-kv",
    SeqKV = "seq-kv",
    LWWKV = "lww-kv",
}

enum ErrorCode {
    Timeout = 0,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    KeyAlreadyExists = 21,
    PreconditionFailed = 22,
    TxnConflict = 30,
}
