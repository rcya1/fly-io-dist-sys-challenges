use std::rc::Rc;

use anyhow::{Result, bail};
use gossip::{App, Context, Message, Type, run};
use serde_json::Value;

struct UniqueIds {
    counter: u64,
}

impl App for UniqueIds {
    type Timer = ();

    fn init(_ctx: &Context) -> Self {
        UniqueIds { counter: 0 }
    }

    async fn handle(&mut self, ctx: Rc<Context>, msg: Message) -> Result<()> {
        match msg.type_ {
            Type::Generate => {
                self.counter += 1;
                let id = format!("{}-{}", ctx.node_id, self.counter);
                ctx.reply(&msg, Type::GenerateOk, vec![("id", Value::String(id))])
                    .await
            }
            other => bail!("unexpected message {:?}", other),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    run::<UniqueIds>().await
}
