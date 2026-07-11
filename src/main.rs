mod message;

use std::collections::HashSet;

use anyhow::{Result, anyhow, bail};
use futures::future::try_join_all;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use crate::message::Message;

#[derive(Debug)]
struct Node {
    node_id: String,
    node_ids: Vec<String>,
    neighbors: Vec<String>,
    message_id: u64,
    generate_counter: u64,
    broadcasted_values: HashSet<u64>,
}

impl Node {
    fn create(node_id: String, node_ids: Vec<String>) -> Self {
        Node {
            node_id,
            node_ids,
            neighbors: Vec::new(),
            message_id: 1,
            generate_counter: 0,
            broadcasted_values: HashSet::new(),
        }
    }

    fn new_message_id(&mut self) -> u64 {
        let ret = self.message_id;
        self.message_id += 1;
        ret
    }
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

    let (stdin_res, stdout_res, node_res) = tokio::join!(stdin_handle, stdout_handle, node_handle);

    if let Err(e) = &node_res {
        eprintln!("node task panicked: {e:?}");
    }
    if let Ok(Err(e)) = &node_res {
        eprintln!("node task returned error: {e:?}");
    }
    if let Err(e) = &stdin_res {
        eprintln!("stdin task panicked: {e:?}");
    }
    if let Ok(Err(e)) = &stdin_res {
        eprintln!("stdin task returned error: {e:?}");
    }
    if let Err(e) = &stdout_res {
        eprintln!("stdout task panicked: {e:?}");
    }
    if let Ok(Err(e)) = &stdout_res {
        eprintln!("stdout task returned error: {e:?}");
    }

    stdin_res??;
    stdout_res??;
    node_res??;

    Ok(())
}

async fn handle_echo(
    node: &mut Node,
    tx: mpsc::Sender<Message>,
    msg: message::Message,
) -> Result<()> {
    let echo_msg = msg.data["echo"].clone();
    let reply = msg.build_reply(
        node.new_message_id(),
        message::Type::EchoOk,
        vec![("echo".to_string(), echo_msg)],
    )?;
    tx.send(reply).await?;
    Ok(())
}

async fn handle_generate(
    node: &mut Node,
    tx: mpsc::Sender<Message>,
    msg: message::Message,
) -> Result<()> {
    node.generate_counter += 1;
    let id = format!("{}-{}", node.node_id, node.generate_counter);
    let reply = msg.build_reply(
        node.new_message_id(),
        message::Type::GenerateOk,
        vec![("id".to_string(), serde_json::Value::String(id))],
    )?;
    tx.send(reply).await?;
    Ok(())
}

async fn propagate_gossip(
    node: &mut Node,
    tx: mpsc::Sender<Message>,
    msg: u64,
    exclude_id: Option<String>,
) -> Result<()> {
    let mut futures = Vec::with_capacity(node.neighbors.len());
    for i in 0..node.neighbors.len() {
        if let Some(ref exclude_id) = exclude_id
            && exclude_id == &node.neighbors[i]
        {
            continue;
        }
        let gossip = Message::create(
            node.node_id.clone(),
            node.neighbors[i].clone(),
            node.new_message_id(),
            message::Type::GossipBroadcast,
            vec![("message".to_string(), message::Message::serde_num(msg))],
        )
        .unwrap();
        futures.push(tx.send(gossip));
    }
    try_join_all(futures).await?;
    Ok(())
}

async fn handle_broadcast(
    node: &mut Node,
    tx: mpsc::Sender<Message>,
    msg: message::Message,
) -> Result<()> {
    let message = msg.get_u64("message")?;
    node.broadcasted_values.insert(message);

    let reply = msg.build_reply(node.new_message_id(), message::Type::BroadcastOk, vec![])?;
    tx.send(reply).await?;

    propagate_gossip(node, tx.clone(), message, None).await?;
    Ok(())
}

async fn handle_gossip_broadcast(
    node: &mut Node,
    tx: mpsc::Sender<Message>,
    msg: message::Message,
) -> Result<()> {
    let message = msg.get_u64("message")?;
    let is_new = node.broadcasted_values.insert(message);
    if is_new {
        propagate_gossip(node, tx.clone(), message, Some(node.node_id.clone())).await?;
    }
    Ok(())
}

async fn handle_read(
    node: &mut Node,
    tx: mpsc::Sender<Message>,
    msg: message::Message,
) -> Result<()> {
    let resp_vec = node
        .broadcasted_values
        .iter()
        .map(|v| serde_json::Value::Number(serde_json::Number::from_u128(u128::from(*v)).unwrap()))
        .collect();
    let reply = msg.build_reply(
        node.new_message_id(),
        message::Type::ReadOk,
        vec![("messages".to_string(), serde_json::Value::Array(resp_vec))],
    )?;
    tx.send(reply).await?;
    Ok(())
}

async fn handle_topology(
    node: &mut Node,
    tx: mpsc::Sender<Message>,
    msg: message::Message,
) -> Result<()> {
    let reply = msg.build_reply(node.new_message_id(), message::Type::TopologyOk, vec![])?;
    tx.send(reply).await?;
    let neighbors = msg
        .data
        .get("topology")
        .ok_or_else(|| anyhow!("topology msg didn't have topology"))?
        .as_object()
        .ok_or_else(|| anyhow!("could not parse topology as map"))?
        .get(&node.node_id)
        .ok_or_else(|| anyhow!("did not find node in topology"))?
        .as_array()
        .ok_or_else(|| anyhow!("could not parse node ids as array"))?
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    node.neighbors = neighbors;
    Ok(())
}

async fn run_node(mut rx: mpsc::Receiver<Message>, tx: mpsc::Sender<Message>) -> Result<()> {
    let init_msg = rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("failed to receive init message"))?;

    let mut node = match init_msg.type_ {
        message::Type::Init => {
            let node_id = init_msg.get_string("node_id")?;
            let node_ids = init_msg.get_string_array("node_ids")?;
            let mut node = Node::create(node_id.to_string(), node_ids);
            let reply =
                init_msg.build_reply(node.new_message_id(), message::Type::InitOk, vec![])?;
            tx.send(reply).await?;
            node
        }
        other => bail!("received non-init message {:?}", other),
    };

    while let Some(msg) = rx.recv().await {
        match msg.type_ {
            message::Type::Echo => {
                handle_echo(&mut node, tx.clone(), msg).await?;
            }
            message::Type::Generate => {
                handle_generate(&mut node, tx.clone(), msg).await?;
            }
            message::Type::Broadcast => {
                handle_broadcast(&mut node, tx.clone(), msg).await?;
            }
            message::Type::GossipBroadcast => {
                handle_gossip_broadcast(&mut node, tx.clone(), msg).await?;
            }
            message::Type::Read => {
                handle_read(&mut node, tx.clone(), msg).await?;
            }
            message::Type::Topology => handle_topology(&mut node, tx.clone(), msg).await?,
            other => bail!("received unimplemented message {:?}", other),
        }
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
