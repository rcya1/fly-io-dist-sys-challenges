use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Interval, MissedTickBehavior};

use crate::constants::ErrorCode;
use crate::message::{Message, Type};
use crate::serde_ext::SerdeJsonExt;

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub per_attempt_timeout: Duration,
    pub backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 10,
            per_attempt_timeout: Duration::from_millis(500),
            backoff: Duration::from_millis(100),
        }
    }
}

#[derive(Debug)]
pub enum RpcError {
    TimedOut,
    Remote(Message),
    ChannelClosed,
}

impl Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcError::TimedOut => write!(f, "rpc timed out after retries"),
            RpcError::Remote(msg) => write!(f, "rpc failed: {:?}", msg),
            RpcError::ChannelClosed => write!(f, "rpc outbound channel closed"),
        }
    }
}

pub struct Context {
    pub node_id: Arc<str>,
    pub node_ids: Vec<Arc<str>>,
    message_id: Cell<u64>,
    tx: mpsc::Sender<Message>,
    pending: RefCell<HashMap<u64, oneshot::Sender<Message>>>,
}

impl Context {
    fn new(node_id: Arc<str>, node_ids: Vec<Arc<str>>, tx: mpsc::Sender<Message>) -> Self {
        Context {
            node_id,
            node_ids,
            message_id: Cell::new(1),
            tx,
            pending: RefCell::new(HashMap::new()),
        }
    }

    pub fn new_message_id(&self) -> u64 {
        let id = self.message_id.get();
        self.message_id.set(id + 1);
        id
    }

    pub fn peers(&self) -> impl Iterator<Item = &Arc<str>> {
        self.node_ids
            .iter()
            .filter(|id| id.as_ref() != self.node_id.as_ref())
    }

    pub fn message(
        &self,
        dest: Arc<str>,
        type_: Type,
        data: Vec<(&str, serde_json::Value)>,
    ) -> Result<Message> {
        Message::create(
            self.node_id.clone(),
            dest,
            self.new_message_id(),
            type_,
            data,
        )
    }

    pub async fn send(&self, msg: Message) -> Result<()> {
        self.tx.send(msg).await?;
        Ok(())
    }

    pub async fn reply(
        &self,
        msg: &Message,
        type_: Type,
        data: Vec<(&str, serde_json::Value)>,
    ) -> Result<()> {
        let reply = msg.build_reply(self.new_message_id(), type_, data)?;
        self.send(reply).await
    }

    pub async fn rpc(
        &self,
        dest: Arc<str>,
        type_: Type,
        data: Vec<(&str, serde_json::Value)>,
        retry: RetryPolicy,
    ) -> Result<Message, RpcError> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let id = self.new_message_id();
            let msg = Message::create(self.node_id.clone(), dest.clone(), id, type_, data.clone())
                .map_err(|_| RpcError::ChannelClosed)?;

            let (tx, rx) = oneshot::channel();
            self.pending.borrow_mut().insert(id, tx);

            if self.tx.send(msg).await.is_err() {
                return Err(RpcError::ChannelClosed);
            }

            match tokio::time::timeout(retry.per_attempt_timeout, rx).await {
                Ok(Ok(reply)) => match self.classify_reply(reply) {
                    ReplyOutcome::Ok(reply) => return Ok(reply),
                    ReplyOutcome::Retryable(_reply) if attempt < retry.max_attempts => {
                        tokio::time::sleep(retry.backoff).await;
                    }
                    ReplyOutcome::Retryable(reply) => return Err(RpcError::Remote(reply)),
                    ReplyOutcome::Fatal(reply) => return Err(RpcError::Remote(reply)),
                },
                Ok(Err(_)) => return Err(RpcError::ChannelClosed),
                Err(_elapsed) if attempt < retry.max_attempts => {
                    tokio::time::sleep(retry.backoff).await;
                }
                Err(_elapsed) => return Err(RpcError::TimedOut),
            }
        }
    }

    fn classify_reply(&self, reply: Message) -> ReplyOutcome {
        if reply.type_ != Type::Error {
            return ReplyOutcome::Ok(reply);
        }
        let retryable = reply
            .get("code")
            .ok()
            .and_then(|v| v.as_num().ok())
            .and_then(|code| ErrorCode::from_code(code).ok())
            .map(|code| code.is_retryable())
            .unwrap_or(false);
        if retryable {
            ReplyOutcome::Retryable(reply)
        } else {
            ReplyOutcome::Fatal(reply)
        }
    }

    fn complete_pending(&self, msg: Message) -> Option<Message> {
        let Some(reply_to) = msg.in_reply_to else {
            return Some(msg);
        };
        let pending_tx = self.pending.borrow_mut().remove(&reply_to);
        match pending_tx {
            Some(tx) => {
                let _ = tx.send(msg);
                None
            }
            None => Some(msg),
        }
    }
}

enum ReplyOutcome {
    Ok(Message),
    Retryable(Message),
    Fatal(Message),
}

#[allow(async_fn_in_trait)]
pub trait App {
    type Timer: Copy;

    fn init(ctx: &Context) -> Self;

    fn timers() -> Vec<(Self::Timer, Duration)> {
        Vec::new()
    }

    async fn on_start(&mut self, _ctx: Rc<Context>) -> Result<()> {
        Ok(())
    }

    async fn handle(&mut self, ctx: Rc<Context>, msg: Message) -> Result<()>;

    async fn on_timer(&mut self, _ctx: Rc<Context>, _timer: Self::Timer) -> Result<()> {
        Ok(())
    }
}

pub async fn run<A: App + 'static>() -> Result<()> {
    let (input_tx, input_rx) = mpsc::channel::<Message>(1024);
    let (output_tx, output_rx) = mpsc::channel::<Message>(1024);
    let stdin_handle = tokio::spawn(read_stdin_task(input_tx));
    let stdout_handle = tokio::spawn(write_stdout_task(output_rx));

    let local = tokio::task::LocalSet::new();
    let node_res = local.run_until(run_node::<A>(input_rx, output_tx)).await;

    let (stdin_res, stdout_res) = tokio::join!(stdin_handle, stdout_handle);
    node_res?;
    stdin_res??;
    stdout_res??;
    Ok(())
}

enum AppEvent<T> {
    Message(Message),
    Timer(T),
}

async fn run_node<A: App + 'static>(
    mut rx: mpsc::Receiver<Message>,
    tx: mpsc::Sender<Message>,
) -> Result<()> {
    let init_msg = rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("failed to receive init message"))?;

    let (node_id, node_ids) = match init_msg.type_ {
        Type::Init => {
            let node_id: Arc<str> = Arc::from(init_msg.get("node_id")?.as_string()?);
            let node_ids = init_msg
                .get("node_ids")?
                .as_string_array()?
                .into_iter()
                .map(Arc::from)
                .collect();
            (node_id, node_ids)
        }
        other => bail!("received non-init message {:?}", other),
    };

    let ctx = Rc::new(Context::new(node_id, node_ids, tx));
    let init_reply = init_msg.build_reply(ctx.new_message_id(), Type::InitOk, vec![])?;
    ctx.send(init_reply).await?;

    let (timer_kinds, durations): (Vec<A::Timer>, Vec<Duration>) = A::timers().into_iter().unzip();
    let mut intervals: Vec<Interval> = durations
        .into_iter()
        .map(|duration| {
            let mut interval = tokio::time::interval(duration);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            interval
        })
        .collect();

    let (app_tx, mut app_rx) = mpsc::unbounded_channel::<AppEvent<A::Timer>>();
    let app_ctx = ctx.clone();
    let app_task = tokio::task::spawn_local(async move {
        let mut app = A::init(&app_ctx);
        app.on_start(app_ctx.clone()).await?;
        while let Some(event) = app_rx.recv().await {
            match event {
                AppEvent::Message(msg) => {
                    if let Err(err) = app.handle(app_ctx.clone(), msg).await {
                        eprintln!("message handler failed: {err}");
                    }
                }
                AppEvent::Timer(timer) => {
                    if let Err(err) = app.on_timer(app_ctx.clone(), timer).await {
                        eprintln!("timer handler failed: {err}");
                    }
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                if let Some(msg) = ctx.complete_pending(msg)
                    && app_tx.send(AppEvent::Message(msg)).is_err()
                {
                    break;
                }
            }
            index = next_tick(&mut intervals) => {
                if app_tx.send(AppEvent::Timer(timer_kinds[index])).is_err() {
                    break;
                }
            }
        }
    }

    drop(app_tx);
    app_task.await??;
    Ok(())
}

async fn next_tick(intervals: &mut [Interval]) -> usize {
    if intervals.is_empty() {
        std::future::pending::<usize>().await
    } else {
        let (_, index, _) =
            futures::future::select_all(intervals.iter_mut().map(|i| Box::pin(i.tick()))).await;
        index
    }
}

async fn read_stdin_task(tx: mpsc::Sender<Message>) -> Result<()> {
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        tx.send(Message::parse(&line)?).await?;
    }
    Ok(())
}

async fn write_stdout_task(mut rx: mpsc::Receiver<Message>) -> Result<()> {
    let mut stdout = tokio::io::stdout();
    while let Some(msg) = rx.recv().await {
        let msg_str = serde_json::to_string(&msg)?;
        stdout.write_all(msg_str.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    Ok(())
}
