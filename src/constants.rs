use std::sync::{Arc, OnceLock};

use anyhow::{Result, bail};

pub trait KvStoreName: Sized {
    const NAME: &'static str;

    fn dest() -> Arc<str> {
        static CELL: OnceLock<Arc<str>> = OnceLock::new();
        CELL.get_or_init(|| Arc::from(Self::NAME)).clone()
    }
}

pub struct LinKv;
impl KvStoreName for LinKv {
    const NAME: &'static str = "lin-kv";
}

pub struct SeqKv;
impl KvStoreName for SeqKv {
    const NAME: &'static str = "seq-kv";
}

pub struct LwwKv;
impl KvStoreName for LwwKv {
    const NAME: &'static str = "lww-kv";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Timeout,
    NotSupported,
    TemporarilyUnavailable,
    MalformedRequest,
    Crash,
    Abort,
    KeyDoesNotExist,
    KeyAlreadyExists,
    PreconditionFailed,
    TxnConflict,
}

impl ErrorCode {
    pub fn from_code(code: u64) -> Result<Self> {
        let error_code = match code {
            0 => ErrorCode::Timeout,
            10 => ErrorCode::NotSupported,
            11 => ErrorCode::TemporarilyUnavailable,
            12 => ErrorCode::MalformedRequest,
            13 => ErrorCode::Crash,
            14 => ErrorCode::Abort,
            20 => ErrorCode::KeyDoesNotExist,
            21 => ErrorCode::KeyAlreadyExists,
            22 => ErrorCode::PreconditionFailed,
            30 => ErrorCode::TxnConflict,
            other => bail!("received unknown error code {}", other),
        };
        Ok(error_code)
    }

    pub fn is_retryable(&self) -> bool {
        let is_retryable = matches!(
            self,
            ErrorCode::Timeout | ErrorCode::TemporarilyUnavailable | ErrorCode::Crash
        );
        is_retryable
    }
}
