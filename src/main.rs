mod message;

use std::collections::HashMap;

use anyhow::{Result, anyhow, bail};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::select;
use tokio::sync::mpsc;

use crate::message::Message;

#[derive(Debug)]
struct Node {
    node_id: String,
    message_id: u64,
}

#[tokio::main]
async fn main() {
    let code = match run().await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("Error: {e:?}");
            1
        }
    };
    std::process::exit(code);
}

async fn run() -> Result<()> {
    let (input_tx, input_rx) = mpsc::channel::<Message>(1024);
    let (output_tx, output_rx) = mpsc::channel::<Message>(1024);

    let stdin_handle = tokio::spawn(read_stdin_task(input_tx));
    let stdout_handle = tokio::spawn(write_stdout_task(output_rx));
    let node_handle = tokio::spawn(run_node(input_rx, output_tx));

    tokio::select! {
        res = stdin_handle => {
            res??;
        }
        res = stdout_handle => {
            res??;
        }
        res = node_handle => {
            res??;
        }
    }

    Ok(())
}

async fn run_node(mut rx: mpsc::Receiver<Message>, tx: mpsc::Sender<Message>) -> Result<()> {
    let init_msg = rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("failed to receive init message"))?;

    let node = match init_msg.type_ {
        message::Type::Init => {
            let node_id = init_msg
                .data
                .get("node_id")
                .ok_or_else(|| anyhow!("init msg didn't have node id"))?
                .clone();
            let reply = init_msg.build_reply(1, message::Type::InitOk, HashMap::new())?;
            tx.clone().send(reply).await?;
            Node {
                node_id,
                message_id: 2,
            }
        }
        other => bail!("received non-init message {:?}", other),
    };

    while let Some(msg) = rx.recv().await {
        eprintln!("handling {:?} on {:?}", msg, node);
    }

    Ok(())
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
