use std::fmt::{self, Display};
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use serde_json::Value;
use tokio::time::sleep;

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

pub enum CasStep<T> {
    Wait(Duration),
    Apply(Value, T),
    Done(T),
}

pub struct KvClient<S: KvStoreName> {
    ctx: Rc<Context>,
    retry: RetryPolicy,
    _store: PhantomData<S>,
}

impl<S: KvStoreName> KvClient<S> {
    pub fn new(ctx: Rc<Context>) -> Self {
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

    pub async fn cas_loop<T, F, D>(
        &self,
        key: &str,
        create_if_not_exists: bool,
        initial_guess: Option<Value>,
        default: D,
        mut compute: F,
    ) -> Result<T, KvError>
    where
        D: Fn() -> Value,
        F: FnMut(&Value) -> Result<CasStep<T>, KvError>,
    {
        let mut current = initial_guess;
        loop {
            let from = match current.take() {
                Some(v) => v,
                None => self.read(key).await?.unwrap_or_else(&default),
            };
            let (to, extra) = match compute(&from)? {
                CasStep::Apply(to, extra) => (to, extra),
                CasStep::Done(extra) => return Ok(extra),
                CasStep::Wait(duration) => {
                    sleep(duration).await;
                    continue;
                }
            };
            match self.cas(key, from, to.clone(), create_if_not_exists).await {
                Ok(()) => return Ok(extra),
                Err(KvError::PreconditionFailed) => {
                    let fresh = self.read(key).await?;
                    if fresh.as_ref() == Some(&to) {
                        return Ok(extra);
                    }
                    current = Some(fresh.unwrap_or_else(&default));
                }
                Err(e) => return Err(e),
            }
        }
    }
}
