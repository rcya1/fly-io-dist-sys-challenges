use std::rc::Rc;

use anyhow::{Result, anyhow, bail};
use gossip::constants::LinKv;
use gossip::kv::{CasStep, KvClient};
use gossip::{App, Context, Message, Type, run};
use serde_json::{Number, Value};

struct Counter;

impl App for Counter {
    type Timer = ();

    fn init(_ctx: &Context) -> Self {
        Counter
    }

    async fn handle(&mut self, ctx: Rc<Context>, msg: Message) -> Result<()> {
        let kv = KvClient::<LinKv>::new(ctx.clone());
        match msg.type_ {
            Type::Add => {
                let delta = msg
                    .get("delta")?
                    .as_i64()
                    .ok_or_else(|| anyhow!("delta is not an integer"))?;

                kv.cas_loop("counter", true, None, || Value::Number(Number::from(0)), |current| {
                    let current = current
                        .as_i64()
                        .ok_or_else(|| anyhow!("counter value is not an integer"))?;
                    let updated = Value::Number(Number::from(current + delta));
                    Ok(CasStep::Apply(updated, ()))
                })
                .await
                .map_err(|e| anyhow!("cas on counter failed: {e}"))?;

                ctx.reply(&msg, Type::AddOk, vec![]).await
            }
            Type::Read => {
                let value = kv
                    .read("counter")
                    .await?
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                ctx.reply(
                    &msg,
                    Type::ReadOk,
                    vec![("value", Value::Number(Number::from(value)))],
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
