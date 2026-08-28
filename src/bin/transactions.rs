use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use gossip::{App, Context, Message, RetryPolicy, Type, run};
use serde_json::{Number, Value};

const SYNC_TIMEOUT: Duration = Duration::from_millis(500);
const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_millis(2000);

#[derive(Clone, Copy, PartialEq, Eq)]
enum OpKind {
    Read,
    Write,
}

impl OpKind {
    fn as_str(self) -> &'static str {
        match self {
            OpKind::Read => "r",
            OpKind::Write => "w",
        }
    }
}

#[derive(Clone, Copy)]
enum Timer {
    AntiEntropy,
}

#[derive(Clone)]
struct Versioned {
    value: i64,
    ts: u64,
    node: Arc<str>,
}

impl Versioned {
    fn supersedes(&self, other: &Versioned) -> bool {
        (self.ts, &*self.node) > (other.ts, &*other.node)
    }
}

#[derive(Default)]
struct Transactions {
    committed: Rc<RefCell<HashMap<i64, Versioned>>>,
    clock: Rc<Cell<u64>>,
}

impl App for Transactions {
    type Timer = Timer;

    fn init(_ctx: &Context) -> Self {
        Transactions::default()
    }

    fn timers() -> Vec<(Timer, Duration)> {
        let timers = vec![(Timer::AntiEntropy, ANTI_ENTROPY_INTERVAL)];
        timers
    }

    /// Blocks message processing until every peer has answered or timed
    /// out
    async fn on_start(&mut self, ctx: Rc<Context>) -> Result<()> {
        let retry = RetryPolicy {
            max_attempts: 1,
            per_attempt_timeout: SYNC_TIMEOUT,
            backoff: Duration::ZERO,
        };
        let peers: Vec<Arc<str>> = ctx.peers().cloned().collect();
        let replies = futures::future::join_all(
            peers
                .into_iter()
                .map(|peer| ctx.rpc(peer, Type::Sync, vec![], retry)),
        )
        .await;

        let mut committed = self.committed.borrow_mut();
        for reply in replies.into_iter().flatten() {
            let Ok(state) = reply.get("state").and_then(parse_kv_store) else {
                continue;
            };
            merge(&mut committed, &self.clock, state);
        }
        Ok(())
    }

    async fn handle(&mut self, ctx: Rc<Context>, msg: Message) -> Result<()> {
        match msg.type_ {
            Type::Txn => self.handle_txn(&ctx, msg).await,
            Type::GossipTxn => self.on_gossip(&ctx, msg).await,
            Type::Sync => self.on_sync(&ctx, msg).await,
            // ignore all replies
            _ if msg.in_reply_to.is_some() => Ok(()),
            other => bail!("unexpected message {:?}", other),
        }
    }

    async fn on_timer(&mut self, ctx: Rc<Context>, timer: Timer) -> Result<()> {
        match timer {
            Timer::AntiEntropy => {
                self.spawn_anti_entropy(ctx);
                Ok(())
            }
        }
    }
}

impl Transactions {
    fn tick(&self) -> u64 {
        let ts = self.clock.get() + 1;
        self.clock.set(ts);
        ts
    }

    async fn handle_txn(&mut self, ctx: &Context, msg: Message) -> Result<()> {
        let ops = msg
            .get("txn")?
            .as_array()
            .ok_or_else(|| anyhow!("txn is not an array"))?
            .clone();

        // internal buffer to apply writes to so concurrent transactions don't see dirty writes
        let mut buffer: HashMap<i64, i64> = HashMap::new();
        let mut result = Vec::with_capacity(ops.len());
        for op in &ops {
            let arr = op.as_array().ok_or_else(|| anyhow!("op is not an array"))?;
            let [kind, key, value] = arr.as_slice() else {
                bail!("op does not have 3 elements");
            };
            let kind = match kind.as_str() {
                Some("r") => OpKind::Read,
                Some("w") => OpKind::Write,
                _ => bail!("unknown txn op kind {:?}", kind),
            };
            let key = key
                .as_i64()
                .ok_or_else(|| anyhow!("op key is not an integer"))?;
            match kind {
                OpKind::Read => {
                    let value = buffer.get(&key).copied().or_else(|| {
                        self.committed
                            .borrow()
                            .get(&key)
                            .map(|version| version.value)
                    });
                    let value = value
                        .map(|v| Value::Number(Number::from(v)))
                        .unwrap_or(Value::Null);
                    result.push(Value::Array(vec![
                        Value::String(kind.as_str().to_string()),
                        Value::Number(Number::from(key)),
                        value,
                    ]));
                }
                OpKind::Write => {
                    let value = value
                        .as_i64()
                        .ok_or_else(|| anyhow!("write op missing value"))?;
                    buffer.insert(key, value);
                    result.push(op.clone());
                }
            }
        }

        let ts = self.tick();
        let writes: HashMap<i64, Versioned> = buffer
            .into_iter()
            .map(|(key, value)| {
                let version = Versioned {
                    value,
                    ts,
                    node: ctx.node_id.clone(),
                };
                (key, version)
            })
            .collect();

        let changed = {
            let mut committed = self.committed.borrow_mut();
            merge(&mut committed, &self.clock, writes)
        };

        // send gossips but don't wait for acks. Otherwise we don't get total
        // availability during partitions
        if !changed.is_empty() {
            gossip(ctx, changed, None).await?;
        }

        ctx.reply(&msg, Type::TxnOk, vec![("txn", Value::Array(result))])
            .await
    }

    async fn on_gossip(&mut self, ctx: &Context, msg: Message) -> Result<()> {
        let writes = parse_kv_store(msg.get("writes")?)?;

        let changed = {
            let mut committed = self.committed.borrow_mut();
            merge(&mut committed, &self.clock, writes)
        };

        // propagate to our neighbors. A write we already lost to stops here
        if !changed.is_empty() {
            gossip(ctx, changed, Some(&msg.src)).await?;
        }
        Ok(())
    }

    async fn on_sync(&self, ctx: &Context, msg: Message) -> Result<()> {
        let state = serialize_kv_store(&self.committed.borrow());
        ctx.reply(&msg, Type::SyncOk, vec![("state", state)]).await
    }

    /// Periodically pulls a full snapshot from every peer and merges it in
    fn spawn_anti_entropy(&self, ctx: Rc<Context>) {
        let peers: Vec<Arc<str>> = ctx.peers().cloned().collect();
        if peers.is_empty() {
            return;
        }
        let committed = self.committed.clone();
        let clock = self.clock.clone();
        tokio::task::spawn_local(async move {
            let retry = RetryPolicy {
                max_attempts: 1,
                per_attempt_timeout: SYNC_TIMEOUT,
                backoff: Duration::ZERO,
            };
            let replies = futures::future::join_all(
                peers
                    .into_iter()
                    .map(|peer| ctx.rpc(peer, Type::Sync, vec![], retry)),
            )
            .await;

            let mut committed = committed.borrow_mut();
            for reply in replies.into_iter().flatten() {
                let Ok(state) = reply.get("state").and_then(parse_kv_store) else {
                    continue;
                };
                merge(&mut committed, &clock, state);
            }
        });
    }
}

/// Applies incoming writes, keeping the winner of each key and raising our
/// clock past every stamp we have seen. Returns only what actually changed
fn merge(
    committed: &mut HashMap<i64, Versioned>,
    clock: &Cell<u64>,
    incoming: HashMap<i64, Versioned>,
) -> HashMap<i64, Versioned> {
    let mut changed = HashMap::new();
    for (key, version) in incoming {
        clock.set(clock.get().max(version.ts));
        if committed
            .get(&key)
            .is_some_and(|current| !version.supersedes(current))
        {
            continue;
        }
        committed.insert(key, version.clone());
        changed.insert(key, version);
    }
    changed
}

/// Relay writes to all peers without waiting for acks
async fn gossip(
    ctx: &Context,
    writes: HashMap<i64, Versioned>,
    except: Option<&Arc<str>>,
) -> Result<()> {
    let data = serialize_kv_store(&writes);
    for peer in ctx.peers() {
        if except.is_some_and(|e| e.as_ref() == peer.as_ref()) {
            continue;
        }
        let msg = ctx.message(
            peer.clone(),
            Type::GossipTxn,
            vec![("writes", data.clone())],
        )?;
        ctx.send(msg).await?;
    }
    Ok(())
}

fn serialize_kv_store(writes: &HashMap<i64, Versioned>) -> Value {
    Value::Object(
        writes
            .iter()
            .map(|(k, v)| {
                let version = Value::Array(vec![
                    Value::Number(Number::from(v.value)),
                    Value::Number(Number::from(v.ts)),
                    Value::String(v.node.to_string()),
                ]);
                (k.to_string(), version)
            })
            .collect(),
    )
}

fn parse_kv_store(v: &Value) -> Result<HashMap<i64, Versioned>> {
    let obj = v
        .as_object()
        .ok_or_else(|| anyhow!("writes is not an object"))?;
    obj.iter()
        .map(|(k, v)| {
            let key = k
                .parse::<i64>()
                .map_err(|_| anyhow!("write key {:?} is not an integer", k))?;
            let Some([value, ts, node]) = v.as_array().map(Vec::as_slice) else {
                bail!("write for key {k} is not a [value, ts, node] triple");
            };
            let value = value
                .as_i64()
                .ok_or_else(|| anyhow!("write value for key {k} is not an integer"))?;
            let ts = ts
                .as_u64()
                .ok_or_else(|| anyhow!("write ts for key {k} is not an integer"))?;
            let node = node
                .as_str()
                .ok_or_else(|| anyhow!("write node for key {k} is not a string"))?;
            let version = Versioned {
                value,
                ts,
                node: Arc::from(node),
            };
            Ok((key, version))
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    run::<Transactions>().await
}
