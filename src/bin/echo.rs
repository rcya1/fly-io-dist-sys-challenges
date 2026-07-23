use anyhow::{Result, bail};
use gossip::{App, Context, Message, Type, run};

struct Echo;

impl App for Echo {
    type Timer = ();

    fn init(_ctx: &Context) -> Self {
        Echo
    }

    async fn handle(&mut self, ctx: &Context, msg: Message) -> Result<()> {
        match msg.type_ {
            Type::Echo => {
                let echo = msg.get("echo")?.clone();
                ctx.reply(&msg, Type::EchoOk, vec![("echo", echo)]).await
            }
            other => bail!("unexpected message {:?}", other),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    run::<Echo>().await
}
