use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use futures::future::try_join_all;
use gossip::constants::{LinKv, SeqKv};
use gossip::kv::{CasStep, KvClient, KvError};
use gossip::{App, Context, Message, RetryPolicy, Type, run};
use serde_json::{Map, Number, Value};
use tokio::sync::Notify;

/// Number of consecutive offsets stored together in a single segment. Also
/// the max number of values that can be buffered before being flushed.
const SEGMENT_SIZE: u64 = 32;
/// Max segments a single poll will read for one key to limit RPC size
const MAX_POLL_SEGMENTS: u64 = 64;
/// Max time a send can sit buffered before being flushed
const FLUSH_TIMEOUT: Duration = Duration::from_millis(50);
/// Max age of a key's cached next offset before a poll must read through to
/// lin-kv again.
const OFFSET_CACHE_TTL: Duration = Duration::from_millis(200);
/// Max age of a cached partial segment before a poll must read through to
/// lin-kv again.
const SEGMENT_CACHE_TTL: Duration = Duration::from_millis(200);
/// Max age of a key's cached committed offset before `list_committed_offsets`
/// must read through to seq-kv again.
const COMMIT_CACHE_TTL: Duration = Duration::from_millis(200);
/// How long a reserved segment may stay unwritten before a poll fences its
/// owner out and steps over it.
const FLUSH_DEADLINE: Duration = Duration::from_secs(2);

/// This node's cached view of one segment of a key's log. Source of truth is
/// in lin-kv
enum Segment {
    /// Reserved, but no write has landed yet
    Pending {
        /// When we first saw it unwritten, for measuring when we should consider
        /// it stalled / abandoned
        first_seen: Instant,
        /// The last time we re-fetched this segment, for measuring when we should
        /// retry
        last_fetched: Instant,
    },
    /// Has data written to it
    Written(BTreeMap<u64, Value>),
    /// A poller waited too long for this and skipped it, so we mark it as never
    /// coming back
    Abandoned,
}

impl Segment {
    /// Whether a Pending segment should be re-fetched from lin-kv
    fn is_stale(&self) -> bool {
        let is_stale = matches!(self, Segment::Pending { last_fetched, .. }
            if last_fetched.elapsed() >= SEGMENT_CACHE_TTL);
        is_stale
    }

    fn is_pending(&self) -> bool {
        let is_pending = matches!(self, Segment::Pending { .. });
        is_pending
    }

    /// Whether a Pending segment should be marked as Abandoned
    fn is_stalled(&self) -> bool {
        let is_stalled = matches!(self, Segment::Pending { first_seen, .. }
            if first_seen.elapsed() >= FLUSH_DEADLINE);
        is_stalled
    }
}

/// This node's cached view of the next unassigned offset for a key. Source of
/// truth is in lin-kv
struct CachedNextOffset {
    offset: u64,
    /// Verbatim value for CAS
    raw: Value,
    last_fetched: Instant,
}

/// This node's cached view of a key's committed offset. Source of truth is
/// in seq-kv
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
    /// Wakes the worker early once `buffer` holds a full segment. Otherwise
    /// the worker waits for the flush timeout
    notify_should_flush: Rc<Notify>,
}

/// Per-key log state
#[derive(Default)]
struct LogState {
    /// Cached segments by segment index. Only covers segments this node has
    /// read or written. If a segment is requested but missing, this cache
    /// is populated
    segments: BTreeMap<u64, Segment>,
    /// Absent until the first reservation or poll for this key.
    next_offset: Option<CachedNextOffset>,
    /// Absent until the first commit or `list_committed_offsets` read for
    /// this key.
    committed_cache: Option<CachedCommit>,
    flush_worker: Option<FlushWorker>,
}

type Logs = Rc<RefCell<HashMap<String, LogState>>>;

#[derive(Default)]
struct Kafka {
    logs: Logs,
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

        if !should_spawn && worker.buffer.len() >= SEGMENT_SIZE as usize {
            notify_should_flush.notify_one();
        }
        drop(logs);

        if should_spawn {
            spawn_flush_worker(ctx, self.logs.clone(), key, notify_should_flush);
        }

        Ok(())
    }

    /// Answers each requested key from its own cache, in parallel
    async fn handle_poll(&mut self, ctx: Rc<Context>, msg: Message) -> Result<()> {
        let offsets = msg
            .get("offsets")?
            .as_object()
            .ok_or_else(|| anyhow!("offsets is not an object"))?
            .clone();

        let logs = self.logs.clone();
        let per_key = offsets.iter().map(|(key, start_offset)| {
            let ctx = ctx.clone();
            let logs = logs.clone();
            async move {
                let start_offset = start_offset
                    .as_u64()
                    .ok_or_else(|| anyhow!("start offset is not a u64"))?;
                let entries = poll_key(ctx, &logs, key, start_offset).await?;
                Ok::<_, anyhow::Error>((key.clone(), Value::Array(entries)))
            }
        });
        let result: Map<String, Value> = try_join_all(per_key).await?.into_iter().collect();

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
        let logs = self.logs.clone();
        let per_key = offsets.iter().map(|(key, offset)| {
            let kv = &kv;
            let logs = logs.clone();
            async move {
                let offset = offset
                    .as_u64()
                    .ok_or_else(|| anyhow!("offset is not a u64"))?;
                kv.write(&committed_key(key), Value::Number(Number::from(offset)))
                    .await?;
                let mut logs = logs.borrow_mut();
                let state = logs.entry(key.clone()).or_default();
                update_cached_committed(state, Some(offset));
                Ok::<(), anyhow::Error>(())
            }
        });
        try_join_all(per_key).await?;

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
        let logs = self.logs.clone();
        let per_key = keys.into_iter().map(|key| {
            let kv = &kv;
            let logs = logs.clone();
            async move {
                let fresh_enough = {
                    let logs = logs.borrow();
                    logs.get(&key)
                        .and_then(|state| state.committed_cache.as_ref())
                        .is_some_and(|cached| cached.last_fetched.elapsed() < COMMIT_CACHE_TTL)
                };
                if !fresh_enough {
                    let offset = kv
                        .read(&committed_key(&key))
                        .await?
                        .and_then(|v| v.as_u64());
                    let mut logs = logs.borrow_mut();
                    let state = logs.entry(key.clone()).or_default();
                    update_cached_committed(state, offset);
                }

                let offset = logs
                    .borrow()
                    .get(&key)
                    .and_then(|state| state.committed_cache.as_ref())
                    .and_then(|cached| cached.offset);
                Ok::<_, anyhow::Error>((key, offset))
            }
        });

        let mut result = Map::new();
        for (key, offset) in try_join_all(per_key).await? {
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

fn next_offset_key(key: &str) -> String {
    let str = format!("next-offset-{key}");
    str
}

fn segment_key(key: &str, segment: u64) -> String {
    let str = format!("seg-{key}-{segment}");
    str
}

fn segment_of(offset: u64) -> u64 {
    offset / SEGMENT_SIZE
}

/// Stored form of a key's offset counter: the offset, plus the node that
/// last claimed a range from it.
///
/// The owner tag exists to make every value a node proposes unique to that
/// node, preventing issues where two nodes attempt to reserve the same offset
/// and mistakenly believe they both successfully reserved
fn offset_value(offset: u64, owner: &str) -> Value {
    Value::Object(Map::from_iter([
        ("offset".to_string(), Value::Number(Number::from(offset))),
        ("owner".to_string(), Value::String(owner.to_string())),
    ]))
}

/// For a counter that doesn't exist yet
fn unclaimed_offset() -> Value {
    offset_value(0, "")
}

fn parse_offset(value: &Value) -> u64 {
    value.get("offset").and_then(Value::as_u64).unwrap_or(0)
}

/// Marker stored in place of a segment whose owner missed the flush deadline
const ABANDONED_SEGMENT: &str = "abandoned";

fn abandoned_segment_value() -> Value {
    Value::String(ABANDONED_SEGMENT.to_string())
}

/// Used for pending segments
fn unwritten_segment_value() -> Value {
    Value::Object(Map::new())
}

fn lin_kv_client(ctx: Rc<Context>) -> KvClient<LinKv> {
    KvClient::<LinKv>::new(ctx).with_retry(RetryPolicy {
        max_attempts: u32::MAX,
        per_attempt_timeout: Duration::from_millis(500),
        backoff: Duration::from_millis(200),
    })
}

/// Waits for either a notification that the buffer has filled or for the
/// timeout, then flushes one segment's worth of entries via `flush_key`.
///
/// A flush never spans more than a segment, so a burst drains over several
/// iterations. Those skip the wait - the entries are already buffered, and
/// nothing further will arrive to notify us
fn spawn_flush_worker(ctx: Rc<Context>, logs: Logs, key: String, notify: Rc<Notify>) {
    tokio::task::spawn_local(async move {
        loop {
            let segment_ready = {
                let logs = logs.borrow();
                logs.get(&key)
                    .and_then(|state| state.flush_worker.as_ref())
                    .is_some_and(|worker| worker.buffer.len() >= SEGMENT_SIZE as usize)
            };
            if !segment_ready {
                tokio::select! {
                    _ = tokio::time::sleep(FLUSH_TIMEOUT) => {},
                    _ = notify.notified() => {},
                }
            }

            let entries = {
                let mut logs = logs.borrow_mut();
                let state = logs.entry(key.clone()).or_default();
                state
                    .flush_worker
                    .as_mut()
                    .map(|worker| {
                        let take = worker.buffer.len().min(SEGMENT_SIZE as usize);
                        worker.buffer.drain(..take).collect::<Vec<_>>()
                    })
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

/// Claims a segment, durably stores the entries in it, and only then acks the
/// clients. If the segment write fails, then we retry, reserving a new segment
async fn flush_key(
    ctx: Rc<Context>,
    logs: Logs,
    key: &str,
    entries: Vec<PendingEntry>,
) -> Result<()> {
    let kv = lin_kv_client(ctx.clone());
    let base = loop {
        let base = reserve_segment(&ctx, &kv, &logs, key).await?;
        match write_segment(&kv, &logs, key, base, &entries).await? {
            WriteOutcome::Written => break base,
            WriteOutcome::Abandoned => {}
        }
    };

    for (index, entry) in entries.into_iter().enumerate() {
        let offset = base + index as u64;
        ctx.reply(
            &entry.msg,
            Type::SendOk,
            vec![("offset", Value::Number(Number::from(offset)))],
        )
        .await?;
    }

    Ok(())
}

/// Atomically claims one whole segment by bumping the key's counter, returning
/// the first offset in it.
async fn reserve_segment(
    ctx: &Context,
    kv: &KvClient<LinKv>,
    logs: &Logs,
    key: &str,
) -> Result<u64> {
    // Start from our last known value to try to skip an upfront read in the
    // common case where noone else has written
    let guess = cached_offset_value(logs, key);
    let owner = ctx.node_id.clone();

    let (base, claimed) = kv
        .cas_loop(
            &next_offset_key(key),
            true,
            guess,
            unclaimed_offset,
            |from| {
                let base = parse_offset(from);
                let to = offset_value(base + SEGMENT_SIZE, &owner);
                Ok(CasStep::Apply(to.clone(), (base, to)))
            },
        )
        .await
        .map_err(|e| anyhow!("failed to reserve a segment for {key}: {e}"))?;

    let mut logs = logs.borrow_mut();
    let state = logs.entry(key.to_string()).or_default();
    update_cached_offset(state, base + SEGMENT_SIZE, claimed);

    Ok(base)
}

/// Whether a flush managed to write the segment it reserved
enum WriteOutcome {
    Written,
    Abandoned,
}

/// Writes the flush's entries as its reserved segment in one CAS.
async fn write_segment(
    kv: &KvClient<LinKv>,
    logs: &Logs,
    key: &str,
    base: u64,
    entries: &[PendingEntry],
) -> Result<WriteOutcome> {
    let by_offset: BTreeMap<u64, Value> = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| (base + index as u64, entry.payload.clone()))
        .collect();
    let value = segment_value(&by_offset);
    let segment = segment_of(base);

    let outcome = match kv
        .cas(
            &segment_key(key, segment),
            unwritten_segment_value(),
            value.clone(),
            true,
        )
        .await
    {
        Ok(()) => WriteOutcome::Written,
        Err(KvError::PreconditionFailed) => {
            let stored = kv
                .read(&segment_key(key, segment))
                .await
                .map_err(|e| anyhow!("failed to read back segment {segment} of {key}: {e}"))?;
            // If we find our value in here, then one of our previous attempts might have succeeded
            // This is safe because this is marked by node id and the flush worker has sole access
            // to the node in this key
            if stored.as_ref() == Some(&value) {
                WriteOutcome::Written
            } else {
                WriteOutcome::Abandoned
            }
        }
        Err(e) => bail!("failed to write segment {segment} of {key}: {e}"),
    };

    if matches!(outcome, WriteOutcome::Written) {
        let mut logs = logs.borrow_mut();
        let state = logs.entry(key.to_string()).or_default();
        replace_cached_segment(state, segment, Some(&value));
    }

    Ok(outcome)
}

async fn poll_key(
    ctx: Rc<Context>,
    logs: &Logs,
    key: &str,
    start_offset: u64,
) -> Result<Vec<Value>> {
    // Do the async work to populate the cache with segments
    ensure_segments_not_stale(ctx, logs, key, start_offset).await?;

    // Do the sync work to gather from the populated cache
    let logs = logs.borrow();
    let entries = logs
        .get(key)
        .map(|state| collect_from_cache(state, start_offset))
        .unwrap_or_default();

    Ok(entries)
}

/// 1. Refreshes the offset so we know how much we need to read.
/// 2. Refresh any stale segments covering the range this poll will answer from.
/// 3. Find the first Pending segment and abandon it if it has stalled for too long
async fn ensure_segments_not_stale(
    ctx: Rc<Context>,
    logs: &Logs,
    key: &str,
    start_offset: u64,
) -> Result<()> {
    let kv = KvClient::<LinKv>::new(ctx.clone());
    let next_offset = read_offset(&kv, logs, key).await?;
    if next_offset <= start_offset {
        return Ok(());
    }

    let first_segment = segment_of(start_offset);
    let last_segment = segment_of(next_offset - 1).min(first_segment + MAX_POLL_SEGMENTS - 1);

    let stale: Vec<u64> = {
        let logs = logs.borrow();
        let state = logs.get(key);
        (first_segment..=last_segment)
            .filter(|segment| {
                state
                    .and_then(|state| state.segments.get(segment))
                    .is_none_or(Segment::is_stale)
            })
            .collect()
    };

    let fetched = try_join_all(stale.into_iter().map(|segment| {
        let kv = &kv;
        async move {
            let value = kv.read(&segment_key(key, segment)).await?;
            Ok::<_, anyhow::Error>((segment, value))
        }
    }))
    .await?;

    {
        let mut logs = logs.borrow_mut();
        let state = logs.entry(key.to_string()).or_default();
        for (segment, value) in fetched {
            replace_cached_segment(state, segment, value.as_ref());
        }
    }

    abandon_stalled_segments(&kv, logs, key, first_segment, last_segment).await
}

/// Returns the key's offset, reading through to lin-kv if our cached value has aged out
async fn read_offset(kv: &KvClient<LinKv>, logs: &Logs, key: &str) -> Result<u64> {
    {
        let logs = logs.borrow();
        if let Some(cached) = logs.get(key).and_then(|state| state.next_offset.as_ref())
            && cached.last_fetched.elapsed() < OFFSET_CACHE_TTL
        {
            return Ok(cached.offset);
        }
    }

    let fetched = kv
        .read(&next_offset_key(key))
        .await?
        .unwrap_or_else(unclaimed_offset);
    let offset = parse_offset(&fetched);

    let mut logs = logs.borrow_mut();
    let state = logs.entry(key.to_string()).or_default();
    update_cached_offset(state, offset, fetched);

    Ok(offset)
}

/// Abandons the first segment holding up this key's poll if it has been stalled for
/// long enough. If we lose the CAS, then we reread since this mean the owner has
/// written the data.
async fn abandon_stalled_segments(
    kv: &KvClient<LinKv>,
    logs: &Logs,
    key: &str,
    first_segment: u64,
    last_segment: u64,
) -> Result<()> {
    let blocking = {
        let logs = logs.borrow();
        let state = logs.get(key);
        (first_segment..=last_segment).find(|segment| {
            state
                .and_then(|state| state.segments.get(segment))
                .is_some_and(Segment::is_stalled)
        })
    };
    let Some(segment) = blocking else {
        return Ok(());
    };

    let fenced = match kv
        .cas(
            &segment_key(key, segment),
            unwritten_segment_value(),
            abandoned_segment_value(),
            true,
        )
        .await
    {
        Ok(()) => Some(abandoned_segment_value()),
        // The owner got its write in first, so use the newly populated value
        Err(KvError::PreconditionFailed) => kv
            .read(&segment_key(key, segment))
            .await
            .map_err(|e| anyhow!("failed to read back segment {segment} of {key}: {e}"))?,
        Err(e) => bail!("failed to abandon segment {segment} of {key}: {e}"),
    };

    let mut logs = logs.borrow_mut();
    let state = logs.entry(key.to_string()).or_default();
    replace_cached_segment(state, segment, fenced.as_ref());

    Ok(())
}

/// Reads cached messages at or after `start_offset`, stopping at the first
/// segment that has not been written yet. This should be called after a
/// `ensure_segments_not_stale` call to make sure the cache has an up-to-date
/// view of the world.
fn collect_from_cache(state: &LogState, start_offset: u64) -> Vec<Value> {
    let first = segment_of(start_offset);
    let mut msgs = Vec::new();

    for segment in first..first + MAX_POLL_SEGMENTS {
        match state.segments.get(&segment) {
            Some(Segment::Written(by_offset)) => {
                msgs.extend(by_offset.range(start_offset..).map(|(offset, msg)| {
                    Value::Array(vec![Value::Number(Number::from(*offset)), msg.clone()])
                }));
            }
            Some(Segment::Abandoned) => continue,
            Some(Segment::Pending { .. }) | None => break,
        }
    }

    msgs
}

fn parse_segment(fetched: Option<&Value>) -> Segment {
    let now = Instant::now();
    let pending = Segment::Pending {
        first_seen: now,
        last_fetched: now,
    };

    match fetched {
        None => pending,
        Some(Value::String(marker)) if marker == ABANDONED_SEGMENT => Segment::Abandoned,
        Some(Value::Object(entries)) => {
            let by_offset: BTreeMap<u64, Value> = entries
                .iter()
                .filter_map(|(offset_str, payload)| {
                    offset_str.parse::<u64>().ok().map(|o| (o, payload.clone()))
                })
                .collect();
            // A written segment always holds at least one entry, so an empty
            // object is not something a flush produced
            if by_offset.is_empty() {
                pending
            } else {
                Segment::Written(by_offset)
            }
        }
        Some(_) => pending,
    }
}

fn segment_value(by_offset: &BTreeMap<u64, Value>) -> Value {
    Value::Object(Map::from_iter(
        by_offset
            .iter()
            .map(|(offset, payload)| (offset.to_string(), payload.clone())),
    ))
}

/// After performing a read or write to lin-kv, replace our cached view.
fn replace_cached_segment(state: &mut LogState, segment: u64, fetched: Option<&Value>) {
    let existing = state.segments.get(&segment);
    if existing.is_some_and(Segment::is_pending) {
        return;
    }

    let mut parsed = parse_segment(fetched);
    if let (
        Segment::Pending { first_seen, .. },
        Some(Segment::Pending {
            first_seen: seen_before,
            ..
        }),
    ) = (&mut parsed, existing)
    {
        *first_seen = *seen_before;
    }

    state.segments.insert(segment, parsed);
}

fn cached_offset_value(logs: &Logs, key: &str) -> Option<Value> {
    let logs = logs.borrow();
    logs.get(key)
        .and_then(|state| state.next_offset.as_ref())
        .map(|cached| cached.raw.clone())
}

fn update_cached_offset(state: &mut LogState, offset: u64, raw: Value) {
    match state.next_offset.as_mut() {
        Some(cached) if cached.offset > offset => cached.last_fetched = Instant::now(),
        Some(cached) => {
            cached.offset = offset;
            cached.raw = raw;
            cached.last_fetched = Instant::now();
        }
        None => {
            state.next_offset = Some(CachedNextOffset {
                offset,
                raw,
                last_fetched: Instant::now(),
            })
        }
    }
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
