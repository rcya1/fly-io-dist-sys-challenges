use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::time::{Interval, MissedTickBehavior};

use crate::message::{Message, Type};
use crate::serde_ext::SerdeJsonExt;

pub struct Context {
    pub node_id: Arc<str>,
    pub node_ids: Vec<Arc<str>>,
    message_id: Cell<u64>,
    tx: mpsc::Sender<Message>,
}

impl Context {
    fn new(node_id: Arc<str>, node_ids: Vec<Arc<str>>, tx: mpsc::Sender<Message>) -> Self {
        Context {
            node_id,
            node_ids,
            message_id: Cell::new(1),
            tx,
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
}

#[allow(async_fn_in_trait)]
pub trait App {
    type Timer: Copy;

    fn init(ctx: &Context) -> Self;

    fn timers() -> Vec<(Self::Timer, Duration)> {
        Vec::new()
    }

    async fn handle(&mut self, ctx: &Context, msg: Message) -> Result<()>;

    async fn on_timer(&mut self, _ctx: &Context, _timer: Self::Timer) -> Result<()> {
        Ok(())
    }
}

pub async fn run<A: App>() -> Result<()> {
    let (input_tx, input_rx) = mpsc::channel::<Message>(1024);
    let (output_tx, output_rx) = mpsc::channel::<Message>(1024);
    let stdin_handle = tokio::spawn(read_stdin_task(input_tx));
    let stdout_handle = tokio::spawn(write_stdout_task(output_rx));

    let node_res = run_node::<A>(input_rx, output_tx).await;

    let (stdin_res, stdout_res) = tokio::join!(stdin_handle, stdout_handle);
    node_res?;
    stdin_res??;
    stdout_res??;
    Ok(())
}

async fn run_node<A: App>(
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

    let ctx = Context::new(node_id, node_ids, tx);
    let init_reply = init_msg.build_reply(ctx.new_message_id(), Type::InitOk, vec![])?;
    ctx.send(init_reply).await?;

    let mut app = A::init(&ctx);
    let (timer_kinds, durations): (Vec<A::Timer>, Vec<Duration>) =
        A::timers().into_iter().unzip();
    let mut intervals: Vec<Interval> = durations
        .into_iter()
        .map(|duration| {
            let mut interval = tokio::time::interval(duration);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            interval
        })
        .collect();

    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                app.handle(&ctx, msg).await?;
            }
            index = next_tick(&mut intervals) => {
                app.on_timer(&ctx, timer_kinds[index]).await?;
            }
        }
    }

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
