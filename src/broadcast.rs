use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use serde_json::Value;
use tokio::time::Instant;

use crate::framework::{App, Context};
use crate::message::{Message, Type};
use crate::serde_ext::SerdeJsonExt;

const GOSSIP_RESEND_INTERVAL: Duration = Duration::from_millis(200);
const BUFFER_TIME: Duration = Duration::from_millis(300);

#[derive(Clone, Copy)]
pub enum Timer {
    Retry,
    Flush,
}

struct Unacked {
    dest: Arc<str>,
    values: Vec<u64>,
    last_send: Instant,
}

pub struct Broadcast<const BUFFERED: bool> {
    values: HashSet<u64>,
    pending: Vec<u64>,
    unacked: HashMap<u64, Unacked>,
}

impl<const BUFFERED: bool> App for Broadcast<BUFFERED> {
    type Timer = Timer;

    fn init(_ctx: &Context) -> Self {
        Broadcast {
            values: HashSet::new(),
            pending: Vec::new(),
            unacked: HashMap::new(),
        }
    }

    fn timers() -> Vec<(Timer, Duration)> {
        let mut timers = vec![(Timer::Retry, GOSSIP_RESEND_INTERVAL)];
        if BUFFERED {
            timers.push((Timer::Flush, BUFFER_TIME));
        }
        timers
    }

    async fn handle(&mut self, ctx: &Context, msg: Message) -> Result<()> {
        match msg.type_ {
            Type::Broadcast => self.on_broadcast(ctx, msg).await,
            Type::GossipBroadcast => self.on_gossip(ctx, msg).await,
            Type::GossipBroadcastOk => self.on_gossip_ok(msg),
            Type::Read => self.on_read(ctx, msg).await,
            Type::Topology => ctx.reply(&msg, Type::TopologyOk, vec![]).await,
            other => bail!("unexpected message {:?}", other),
        }
    }

    async fn on_timer(&mut self, ctx: &Context, timer: Timer) -> Result<()> {
        match timer {
            Timer::Retry => self.retry(ctx).await,
            Timer::Flush => self.flush(ctx).await,
        }
    }
}

impl<const BUFFERED: bool> Broadcast<BUFFERED> {
    async fn on_broadcast(&mut self, ctx: &Context, msg: Message) -> Result<()> {
        let value = msg.get("message")?.as_num()?;
        ctx.reply(&msg, Type::BroadcastOk, vec![]).await?;

        if self.values.insert(value) {
            if BUFFERED {
                self.pending.push(value);
            } else {
                self.gossip(ctx, vec![value]).await?;
            }
        }
        Ok(())
    }

    async fn on_gossip(&mut self, ctx: &Context, msg: Message) -> Result<()> {
        for value in msg.get("messages")?.as_num_array()? {
            self.values.insert(value);
        }
        ctx.reply(&msg, Type::GossipBroadcastOk, vec![]).await
    }

    fn on_gossip_ok(&mut self, msg: Message) -> Result<()> {
        let in_reply_to = msg
            .in_reply_to
            .ok_or_else(|| anyhow!("gossip_broadcast_ok without in_reply_to: {:?}", msg))?;
        self.unacked.remove(&in_reply_to);
        Ok(())
    }

    async fn on_read(&mut self, ctx: &Context, msg: Message) -> Result<()> {
        let values = num_array(self.values.iter().copied());
        ctx.reply(&msg, Type::ReadOk, vec![("messages", values)]).await
    }

    async fn gossip(&mut self, ctx: &Context, values: Vec<u64>) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let peers: Vec<Arc<str>> = ctx.peers().cloned().collect();
        for peer in peers {
            let id = ctx.new_message_id();
            let msg = Message::create(
                ctx.node_id.clone(),
                peer.clone(),
                id,
                Type::GossipBroadcast,
                vec![("messages", num_array(values.iter().copied()))],
            )?;
            self.unacked.insert(
                id,
                Unacked {
                    dest: peer,
                    values: values.clone(),
                    last_send: Instant::now(),
                },
            );
            ctx.send(msg).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, ctx: &Context) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let values = std::mem::take(&mut self.pending);
        self.gossip(ctx, values).await
    }

    async fn retry(&mut self, ctx: &Context) -> Result<()> {
        let now = Instant::now();
        let stale: Vec<u64> = self
            .unacked
            .iter()
            .filter(|(_, u)| now.duration_since(u.last_send) >= GOSSIP_RESEND_INTERVAL)
            .map(|(&id, _)| id)
            .collect();

        for id in stale {
            let (dest, values) = match self.unacked.get_mut(&id) {
                Some(u) => {
                    u.last_send = now;
                    (u.dest.clone(), u.values.clone())
                }
                None => continue,
            };
            let msg = Message::create(
                ctx.node_id.clone(),
                dest,
                id,
                Type::GossipBroadcast,
                vec![("messages", num_array(values.into_iter()))],
            )?;
            ctx.send(msg).await?;
        }
        Ok(())
    }
}

fn num_array(values: impl Iterator<Item = u64>) -> Value {
    Value::Array(values.map(Value::from).collect())
}
