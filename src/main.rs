mod message;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use crate::message::Message;

struct Node {}

#[tokio::main]
async fn main() {
    let (input_tx, input_rx) = mpsc::channel::<Message>(1024);
    let (output_tx, output_rx) = mpsc::channel::<Message>(1024);

    tokio::spawn(read_stdin_task(input_tx));
    tokio::spawn(write_stdout_task(output_rx));
}

async fn read_stdin_task(tx: mpsc::Sender<Message>) -> Result<()> {
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        match Message::parse(&line) {
            Ok(msg) => tx.send(msg).await?,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

async fn write_stdout_task(mut rx: mpsc::Receiver<Message>) -> Result<()> {
    let mut stdout = tokio::io::stdout();
    while let Some(msg) = rx.recv().await {
        let msg_str = msg.to_string();
        stdout.write_all(msg_str.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    Ok(())
}
