use std::rc::Rc;

use anyhow::{Result, anyhow, bail};
use futures::future::try_join_all;
use gossip::constants::SeqKv;
use gossip::kv::KvClient;
use gossip::{App, Context, Message, Type, run};
use serde_json::{Number, Value};

fn key(ctx: &Context) -> String {
    let key = format!("counter-{}", ctx.node_id);
    key
}

#[derive(Default)]
struct Counter {
    counter: i64,
}

impl App for Counter {
    type Timer = ();

    fn init(_ctx: &Context) -> Self {
        Counter::default()
    }

    async fn on_start(&mut self, ctx: Rc<Context>) -> Result<()> {
        let kv = KvClient::<SeqKv>::new(ctx.clone());
        self.counter = kv
            .read(&key(&ctx))
            .await?
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        Ok(())
    }

    async fn handle(&mut self, ctx: Rc<Context>, msg: Message) -> Result<()> {
        let kv = KvClient::<SeqKv>::new(ctx.clone());
        match msg.type_ {
            Type::Add => {
                let delta = msg
                    .get("delta")?
                    .as_i64()
                    .ok_or_else(|| anyhow!("delta is not an integer"))?;

                let updated = self.counter + delta;
                let key = key(&ctx);
                kv.write(&key, Value::Number(Number::from(updated)))
                    .await
                    .map_err(|e| anyhow!("write on {key} failed: {e}"))?;
                self.counter = updated;

                ctx.reply(&msg, Type::AddOk, vec![]).await
            }
            Type::Read => {
                let reads = ctx.node_ids.iter().map(|node_id| {
                    let kv = &kv;
                    async move {
                        let key = format!("counter-{node_id}");
                        Ok::<i64, anyhow::Error>(
                            kv.read(&key).await?.and_then(|v| v.as_i64()).unwrap_or(0),
                        )
                    }
                });
                let total: i64 = try_join_all(reads).await?.into_iter().sum();

                ctx.reply(
                    &msg,
                    Type::ReadOk,
                    vec![("value", Value::Number(Number::from(total)))],
                )
                .await
            }
            other => bail!("unexpected message {:?}", other),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    run::<Counter>().await
}
