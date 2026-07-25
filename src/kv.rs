use std::fmt::{self, Display};
use std::marker::PhantomData;

use serde_json::Value;

use crate::constants::{ErrorCode, KvStoreName};
use crate::framework::{Context, RetryPolicy, RpcError};
use crate::message::Type;
use crate::serde_ext::SerdeJsonExt;

#[derive(Debug)]
pub enum KvError {
    KeyDoesNotExist,
    KeyAlreadyExists,
    PreconditionFailed,
    Other(anyhow::Error),
}

impl Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvError::KeyDoesNotExist => write!(f, "key does not exist"),
            KvError::KeyAlreadyExists => write!(f, "key already exists"),
            KvError::PreconditionFailed => write!(f, "cas precondition failed"),
            KvError::Other(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for KvError {}

impl From<anyhow::Error> for KvError {
    fn from(err: anyhow::Error) -> Self {
        KvError::Other(err)
    }
}

impl From<RpcError> for KvError {
    fn from(err: RpcError) -> Self {
        let RpcError::Remote(msg) = err else {
            return KvError::Other(anyhow::anyhow!("{err}"));
        };
        let code = msg.get("code").ok().and_then(|v| v.as_num().ok());
        match code.and_then(|c| ErrorCode::from_code(c).ok()) {
            Some(ErrorCode::KeyDoesNotExist) => KvError::KeyDoesNotExist,
            Some(ErrorCode::KeyAlreadyExists) => KvError::KeyAlreadyExists,
            Some(ErrorCode::PreconditionFailed) => KvError::PreconditionFailed,
            _ => KvError::Other(anyhow::anyhow!("kv rpc failed: {:?}", msg)),
        }
    }
}

pub struct KvClient<'a, S: KvStoreName> {
    ctx: &'a Context,
    retry: RetryPolicy,
    _store: PhantomData<S>,
}

impl<'a, S: KvStoreName> KvClient<'a, S> {
    pub fn new(ctx: &'a Context) -> Self {
        KvClient {
            ctx,
            retry: RetryPolicy::default(),
            _store: PhantomData,
        }
    }

    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub async fn read(&self, key: &str) -> Result<Option<Value>, KvError> {
        let result = self
            .ctx
            .rpc(
                S::dest(),
                Type::Read,
                vec![("key", Value::String(key.to_string()))],
                self.retry,
            )
            .await;
        match result {
            Ok(reply) => Ok(Some(reply.get("value")?.clone())),
            Err(err) => match KvError::from(err) {
                KvError::KeyDoesNotExist => Ok(None),
                other => Err(other),
            },
        }
    }

    pub async fn write(&self, key: &str, value: Value) -> Result<(), KvError> {
        self.ctx
            .rpc(
                S::dest(),
                Type::Write,
                vec![("key", Value::String(key.to_string())), ("value", value)],
                self.retry,
            )
            .await?;
        Ok(())
    }

    pub async fn cas(
        &self,
        key: &str,
        from: Value,
        to: Value,
        create_if_not_exists: bool,
    ) -> Result<(), KvError> {
        self.ctx
            .rpc(
                S::dest(),
                Type::Cas,
                vec![
                    ("key", Value::String(key.to_string())),
                    ("from", from),
                    ("to", to),
                    ("create_if_not_exists", Value::Bool(create_if_not_exists)),
                ],
                self.retry,
            )
            .await?;
        Ok(())
    }
}
