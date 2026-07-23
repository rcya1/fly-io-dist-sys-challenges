use anyhow::Result;
use gossip::broadcast::Broadcast;
use gossip::run;

#[tokio::main]
async fn main() -> Result<()> {
    run::<Broadcast<true>>().await
}
