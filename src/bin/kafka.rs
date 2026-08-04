use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use gossip::constants::{LinKv, SeqKv};
use gossip::kv::{CasStep, KvClient};
use gossip::{App, Context, Message, RetryPolicy, Type, run};
use serde_json::{Map, Number, Value};
use tokio::sync::Notify;

/// Max buffered sends for a key before a flush is forced.
const FLUSH_SIZE: usize = 32;
/// Max time a send can sit buffered before being forced out.
const FLUSH_LINGER: Duration = Duration::from_millis(50);
/// Max age of a key's cached poll data before a poll must read through to
/// lin-kv again.
const POLL_CACHE_TTL: Duration = Duration::from_millis(200);
/// Max age of a key's cached committed offset before `list_committed_offsets`
/// must read through to seq-kv again.
const COMMIT_CACHE_TTL: Duration = Duration::from_millis(200);

/// This node's cached view of a key's log. Populated upon flush or reads.
/// Source of truth is in lin-kv
struct CachedLog {
    /// Format stored in lin-kv store
    raw: Value,
    /// Format for answering queries
    by_offset: BTreeMap<u64, Value>,
    /// When this snapshot was taken
    last_fetched: Instant,
}

/// This node's cached view of a key's committed offset. Source of truth is
/// in seq-kv. `offset` is `None` when a real read confirmed nothing has been
/// committed yet — that's just as cacheable/TTL-bounded as a real offset, no
/// need to treat "not committed" as an uncached special case.
struct CachedCommit {
    offset: Option<u64>,
    last_fetched: Instant,
}

/// Entries that are pending to be flushed; have not been assigned an offset yet
struct PendingEntry {
    payload: Value,
    msg: Message,
}

/// A live flush worker for a key.
struct FlushWorker {
    /// Sends buffered for this key, not yet assigned an offset or flushed.
    buffer: Vec<PendingEntry>,
    /// Wakes the worker early once `buffer` hits `FLUSH_SIZE`.
    notify_should_flush: Rc<Notify>,
}

#[derive(Default)]
struct LogState {
    /// Absent until the first successful fetch or flush for this key
    cached_log: Option<CachedLog>,
    /// Absent until the first commit or `list_committed_offsets` read for
    /// this key.
    committed_cache: Option<CachedCommit>,
    flush_worker: Option<FlushWorker>,
}

#[derive(Default)]
struct Kafka {
    logs: Rc<RefCell<HashMap<String, LogState>>>,
}

impl App for Kafka {
    type Timer = ();

    fn init(_ctx: &Context) -> Self {
        Kafka::default()
    }

    async fn handle(&mut self, ctx: Rc<Context>, msg: Message) -> Result<()> {
        match msg.type_ {
            Type::Send => self.handle_send(ctx.clone(), msg).await,
            Type::Poll => self.handle_poll(ctx.clone(), msg).await,
            Type::CommitOffsets => self.handle_commit_offsets(ctx.clone(), msg).await,
            Type::ListCommittedOffsets => {
                self.handle_list_committed_offsets(ctx.clone(), msg).await
            }
            other => bail!("unexpected message {:?}", other),
        }
    }
}

impl Kafka {
    /// Buffer the send and kick off (or wake) a flush worker for its key. The client ack
    /// is handled by the flush worker
    async fn handle_send(&mut self, ctx: Rc<Context>, msg: Message) -> Result<()> {
        let key = msg
            .get("key")?
            .as_str()
            .ok_or_else(|| anyhow!("key is not a string"))?
            .to_string();
        let payload = msg.get("msg")?.clone();

        let mut logs = self.logs.borrow_mut();
        let state = logs.entry(key.clone()).or_default();

        let should_spawn = state.flush_worker.is_none();
        let worker = state.flush_worker.get_or_insert_with(|| FlushWorker {
            buffer: Vec::new(),
            notify_should_flush: Rc::new(Notify::new()),
        });
        worker.buffer.push(PendingEntry { payload, msg });
        let notify_should_flush = worker.notify_should_flush.clone();

        if !should_spawn && worker.buffer.len() >= FLUSH_SIZE {
            notify_should_flush.notify_one();
        }
        drop(logs);

        if should_spawn {
            spawn_flush_worker(ctx, self.logs.clone(), key, notify_should_flush);
        }

        Ok(())
    }

    /// Ensures the local cache is not stale for each given key, then uses the
    /// cache to answer the query
    async fn handle_poll(&mut self, ctx: Rc<Context>, msg: Message) -> Result<()> {
        let offsets = msg
            .get("offsets")?
            .as_object()
            .ok_or_else(|| anyhow!("offsets is not an object"))?
            .clone();

        let mut result = Map::new();
        for (key, start_offset) in &offsets {
            let start_offset = start_offset
                .as_u64()
                .ok_or_else(|| anyhow!("start offset is not a u64"))?;
            ensure_cached_not_stale(ctx.clone(), &self.logs, key).await?;

            let entries: Vec<Value> = {
                let logs = self.logs.borrow();
                logs.get(key)
                    .and_then(|state| state.cached_log.as_ref())
                    .map(|cached| {
                        cached
                            .by_offset
                            .range(start_offset..)
                            .map(|(offset, msg)| {
                                Value::Array(vec![
                                    Value::Number(Number::from(*offset)),
                                    msg.clone(),
                                ])
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            };
            result.insert(key.clone(), Value::Array(entries));
        }

        ctx.reply(&msg, Type::PollOk, vec![("msgs", Value::Object(result))])
            .await
    }

    /// Update the offset in the cache + seq-kv
    async fn handle_commit_offsets(&mut self, ctx: Rc<Context>, msg: Message) -> Result<()> {
        let offsets = msg
            .get("offsets")?
            .as_object()
            .ok_or_else(|| anyhow!("offsets is not an object"))?
            .clone();

        let kv = KvClient::<SeqKv>::new(ctx.clone());
        for (key, offset) in &offsets {
            let offset = offset
                .as_u64()
                .ok_or_else(|| anyhow!("offset is not a u64"))?;
            kv.write(&committed_key(key), Value::Number(Number::from(offset)))
                .await?;
            let mut logs = self.logs.borrow_mut();
            let state = logs.entry(key.clone()).or_default();
            update_cached_committed(state, Some(offset));
        }

        ctx.reply(&msg, Type::CommitOffsetsOk, vec![]).await
    }

    /// If the cache isn't too far out of date, use it. Otherwise query seq-kv
    async fn handle_list_committed_offsets(
        &mut self,
        ctx: Rc<Context>,
        msg: Message,
    ) -> Result<()> {
        let keys = msg
            .get("keys")?
            .as_array()
            .ok_or_else(|| anyhow!("keys is not an array"))?
            .iter()
            .map(|v| {
                v.as_str()
                    .ok_or_else(|| anyhow!("key is not a string"))
                    .map(str::to_string)
            })
            .collect::<Result<Vec<String>>>()?;

        let kv = KvClient::<SeqKv>::new(ctx.clone());
        let mut result = Map::new();
        for key in keys {
            let fresh_enough = {
                let logs = self.logs.borrow();
                logs.get(&key)
                    .and_then(|state| state.committed_cache.as_ref())
                    .is_some_and(|cached| cached.last_fetched.elapsed() < COMMIT_CACHE_TTL)
            };
            if !fresh_enough {
                let offset = kv
                    .read(&committed_key(&key))
                    .await?
                    .and_then(|v| v.as_u64());
                let mut logs = self.logs.borrow_mut();
                let state = logs.entry(key.clone()).or_default();
                update_cached_committed(state, offset);
            }

            let offset = self
                .logs
                .borrow()
                .get(&key)
                .and_then(|state| state.committed_cache.as_ref())
                .and_then(|cached| cached.offset);
            if let Some(offset) = offset {
                result.insert(key, Value::Number(Number::from(offset)));
            }
        }

        ctx.reply(
            &msg,
            Type::ListCommittedOffsetsOk,
            vec![("offsets", Value::Object(result))],
        )
        .await
    }
}

fn committed_key(key: &str) -> String {
    let str = format!("committed-{key}");
    str
}

fn log_key(key: &str) -> String {
    let str = format!("log-{key}");
    str
}

fn empty_log_value() -> Value {
    Value::Object(Map::from_iter([
        ("next_offset".to_string(), Value::Number(Number::from(0u64))),
        ("entries".to_string(), Value::Object(Map::new())),
    ]))
}

/// Ensures the currently cached data for `key` is not stale
async fn ensure_cached_not_stale(
    ctx: Rc<Context>,
    logs: &Rc<RefCell<HashMap<String, LogState>>>,
    key: &str,
) -> Result<()> {
    {
        let logs = logs.borrow();
        if let Some(cached) = logs.get(key).and_then(|s| s.cached_log.as_ref())
            && cached.last_fetched.elapsed() < POLL_CACHE_TTL
        {
            return Ok(());
        }
    }

    let kv = KvClient::<LinKv>::new(ctx.clone());
    let fetched = kv
        .read(&log_key(key))
        .await?
        .unwrap_or_else(empty_log_value);

    let mut logs = logs.borrow_mut();
    let state = logs.entry(key.to_string()).or_default();
    replace_cached_log(state, fetched);

    Ok(())
}

/// Waits for either a notification that the buffer has filled or for the
/// timeout. Then flushes to lin-kv via `flush_key`
fn spawn_flush_worker(
    ctx: Rc<Context>,
    logs: Rc<RefCell<HashMap<String, LogState>>>,
    key: String,
    notify: Rc<Notify>,
) {
    tokio::task::spawn_local(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(FLUSH_LINGER) => {},
                _ = notify.notified() => {},
            }

            let entries = {
                let mut logs = logs.borrow_mut();
                let state = logs.entry(key.clone()).or_default();
                state
                    .flush_worker
                    .as_mut()
                    .map(|worker| std::mem::take(&mut worker.buffer))
                    .unwrap_or_default()
            };

            if entries.is_empty() {
                let mut logs = logs.borrow_mut();
                if let Some(state) = logs.get_mut(&key) {
                    state.flush_worker = None;
                }
                break;
            }

            if let Err(err) = flush_key(ctx.clone(), logs.clone(), &key, entries).await {
                eprintln!("flush of {key} failed permanently: {err}");
            }
        }
        Ok::<(), anyhow::Error>(())
    });
}

/// Assigns offsets to `entries` and durably stores them in lin-kv, all as a
/// single CAS
async fn flush_key(
    ctx: Rc<Context>,
    logs: Rc<RefCell<HashMap<String, LogState>>>,
    key: &str,
    entries: Vec<PendingEntry>,
) -> Result<()> {
    let kv = KvClient::<LinKv>::new(ctx.clone()).with_retry(RetryPolicy {
        max_attempts: u32::MAX,
        per_attempt_timeout: Duration::from_millis(500),
        backoff: Duration::from_millis(200),
    });
    let log_key = log_key(key);

    // Start from our last known value to try to skip an upfront read in the
    // common case where noone else has written
    let guess: Option<Value> = {
        let logs = logs.borrow();
        logs.get(key)
            .and_then(|s| s.cached_log.as_ref())
            .map(|c| c.raw.clone())
    };

    let (assigned_offsets, new_value) = kv
        .cas_loop(&log_key, true, guess, empty_log_value, |from_value| {
            let (next_offset, existing_entries) = match from_value {
                Value::Object(obj) => {
                    let next_offset = obj.get("next_offset").and_then(|v| v.as_u64()).unwrap_or(0);
                    let existing = match obj.get("entries") {
                        Some(Value::Object(m)) => m.clone(),
                        _ => Map::new(),
                    };
                    (next_offset, existing)
                }
                _ => (0, Map::new()),
            };

            let mut new_entries = existing_entries;
            let mut offset = next_offset;
            let mut offsets = Vec::with_capacity(entries.len());
            for entry in &entries {
                new_entries.insert(offset.to_string(), entry.payload.clone());
                offsets.push(offset);
                offset += 1;
            }
            let to_value = Value::Object(Map::from_iter([
                (
                    "next_offset".to_string(),
                    Value::Number(Number::from(offset)),
                ),
                ("entries".to_string(), Value::Object(new_entries)),
            ]));

            Ok(CasStep::Apply(to_value.clone(), (offsets, to_value)))
        })
        .await
        .map_err(|e| anyhow!("failed to flush {key}: {e}"))?;

    {
        let mut logs = logs.borrow_mut();
        let state = logs.entry(key.to_string()).or_default();
        replace_cached_log(state, new_value);
    }

    for (entry, offset) in entries.into_iter().zip(assigned_offsets.into_iter()) {
        ctx.reply(
            &entry.msg,
            Type::SendOk,
            vec![("offset", Value::Number(Number::from(offset)))],
        )
        .await?;
    }

    Ok(())
}

/// After performing a read or write to lin-kv, replace our cached version
fn replace_cached_log(state: &mut LogState, fetched: Value) {
    let by_offset = match fetched.get("entries") {
        Some(Value::Object(entries)) => entries
            .iter()
            .filter_map(|(offset_str, payload)| {
                offset_str.parse::<u64>().ok().map(|o| (o, payload.clone()))
            })
            .collect(),
        _ => BTreeMap::new(),
    };

    state.cached_log = Some(CachedLog {
        raw: fetched,
        by_offset,
        last_fetched: Instant::now(),
    });
}

fn update_cached_committed(state: &mut LogState, offset: Option<u64>) {
    let existing = state.committed_cache.as_ref().and_then(|c| c.offset);
    let new_cached = match (offset, existing) {
        (Some(new), Some(existing)) => Some(new.max(existing)),
        (Some(new), None) => Some(new),
        (None, existing) => existing,
    };
    state.committed_cache = Some(CachedCommit {
        offset: new_cached,
        last_fetched: Instant::now(),
    });
}

#[tokio::main]
async fn main() -> Result<()> {
    run::<Kafka>().await
}
