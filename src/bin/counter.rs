use std::rc::Rc;

use anyhow::{Result, anyhow, bail};
use gossip::constants::SeqKv;
use gossip::kv::KvClient;
use gossip::{App, Context, Message, Type, run};
use serde_json::{Number, Value};

struct Counter;

impl App for Counter {
    type Timer = ();

    fn init(_ctx: &Context) -> Self {
        Counter
    }

    async fn handle(&mut self, ctx: Rc<Context>, msg: Message) -> Result<()> {
        let kv = KvClient::<SeqKv>::new(ctx.clone());
        match msg.type_ {
            Type::Add => {
                let delta = msg
                    .get("delta")?
                    .as_i64()
                    .ok_or_else(|| anyhow!("delta is not an integer"))?;

                let key = format!("counter-{}", ctx.node_id);
                let current = kv.read(&key).await?.and_then(|v| v.as_i64()).unwrap_or(0);
                kv.write(&key, Value::Number(Number::from(current + delta)))
                    .await
                    .map_err(|e| anyhow!("write on {key} failed: {e}"))?;

                ctx.reply(&msg, Type::AddOk, vec![]).await
            }
            Type::Read => {
                let mut total = 0i64;
                for node_id in &ctx.node_ids {
                    let key = format!("counter-{node_id}");
                    total += kv.read(&key).await?.and_then(|v| v.as_i64()).unwrap_or(0);
                }
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
