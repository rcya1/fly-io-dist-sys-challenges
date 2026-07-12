mod message;
mod serde_ext;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::{Result, anyhow, bail};
use futures::future::try_join_all;
use serde_ext::SerdeJsonExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use crate::message::Message;

#[derive(Debug)]
struct Node {
    node_id: Arc<str>,
    node_ids: Vec<String>,
    topology: HashMap<Arc<str>, Vec<Arc<str>>>,
    message_id: u64,
    generate_counter: u64,
    broadcasted_values: HashSet<u64>,
}

impl Node {
    fn create(node_id: Arc<str>, node_ids: Vec<String>) -> Self {
        Node {
            node_id,
            node_ids,
            topology: HashMap::new(),
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

fn bfs_distances(
    topology: &HashMap<Arc<str>, Vec<Arc<str>>>,
    origin: &str,
) -> HashMap<Arc<str>, u64> {
    let mut distances = HashMap::new();
    let mut queue = VecDeque::new();

    if let Some((origin_key, _)) = topology.get_key_value(origin) {
        distances.insert(origin_key.clone(), 0u64);
        queue.push_back(origin_key.clone());
    }

    while let Some(current) = queue.pop_front() {
        let current_dist = distances[&current];
        for neighbor in topology.get(current.as_ref()).into_iter().flatten() {
            if !distances.contains_key(neighbor) {
                distances.insert(neighbor.clone(), current_dist + 1);
                queue.push_back(neighbor.clone());
            }
        }
    }

    distances
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
    fn process_res(res: &Result<Result<()>, tokio::task::JoinError>, tag: &str) {
        if let Err(e) = &res {
            eprintln!("{tag} task panicked: {e:?}");
        }
        if let Ok(Err(e)) = &res {
            eprintln!("{tag} task returned error: {e:?}");
        }
    }

    process_res(&node_res, "node");
    process_res(&stdin_res, "stdin");
    process_res(&stdout_res, "stdout");

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
        vec![("echo", echo_msg)],
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
        vec![("id", serde_json::Value::String(id))],
    )?;
    tx.send(reply).await?;
    Ok(())
}

/// Forwards `msg` only to neighbors strictly farther (in hop count) from `origin`
/// than this node is, so gossip flows outward from the origin instead of flooding
/// every edge in both directions.
async fn propagate_gossip(
    node: &mut Node,
    tx: mpsc::Sender<Message>,
    msg: u64,
    origin: &str,
) -> Result<()> {
    let distances = bfs_distances(&node.topology, origin);
    let Some(&my_dist) = distances.get(node.node_id.as_ref()) else {
        // Topology doesn't (yet) know how to reach us from this origin; nothing to do.
        return Ok(());
    };

    let neighbors = node
        .topology
        .get(node.node_id.as_ref())
        .cloned()
        .unwrap_or_default();

    let mut futures = Vec::with_capacity(neighbors.len());
    for neighbor in &neighbors {
        let farther = distances
            .get(neighbor.as_ref())
            .is_some_and(|&d| d > my_dist);
        if !farther {
            continue;
        }
        let message_id = node.new_message_id();
        let gossip = Message::create(
            node.node_id.clone(),
            neighbor.clone(),
            message_id,
            message::Type::GossipBroadcast,
            vec![
                ("message", serde_json::Value::from(msg)),
                ("origin", serde_json::Value::String(origin.to_string())),
            ],
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
    let message = msg.get("message")?.as_num()?;
    node.broadcasted_values.insert(message);

    let reply = msg.build_reply(node.new_message_id(), message::Type::BroadcastOk, vec![])?;
    tx.send(reply).await?;

    let origin = node.node_id.clone();
    propagate_gossip(node, tx.clone(), message, &origin).await?;
    Ok(())
}

async fn handle_gossip_broadcast(
    node: &mut Node,
    tx: mpsc::Sender<Message>,
    msg: message::Message,
) -> Result<()> {
    let message = msg.get("message")?.as_num()?;
    let origin = msg.get("origin")?.as_string()?;
    let is_new = node.broadcasted_values.insert(message);
    if is_new {
        propagate_gossip(node, tx.clone(), message, origin).await?;
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
        .map(|v| serde_json::Value::from(*v))
        .collect();
    let reply = msg.build_reply(
        node.new_message_id(),
        message::Type::ReadOk,
        vec![("messages", serde_json::Value::Array(resp_vec))],
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
    node.topology = msg
        .get("topology")?
        .as_obj()?
        .iter()
        .map(
            |(node_id, neighbors)| -> Result<(Arc<str>, Vec<Arc<str>>)> {
                let neighbors = neighbors
                    .as_string_array()?
                    .into_iter()
                    .map(Arc::from)
                    .collect();
                Ok((Arc::from(node_id.as_str()), neighbors))
            },
        )
        .collect::<Result<_>>()?;
    Ok(())
}

async fn run_node(mut rx: mpsc::Receiver<Message>, tx: mpsc::Sender<Message>) -> Result<()> {
    let init_msg = rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("failed to receive init message"))?;

    let mut node = match init_msg.type_ {
        message::Type::Init => {
            let node_id = init_msg.get("node_id")?.as_string()?;
            let node_ids = init_msg
                .get("node_ids")?
                .as_string_array()?
                .iter()
                .map(|v| v.to_string())
                .collect();
            let mut node = Node::create(Arc::from(node_id), node_ids);
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
        let msg_str = serde_json::to_string(&msg)?;
        stdout.write_all(msg_str.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    Ok(())
}
