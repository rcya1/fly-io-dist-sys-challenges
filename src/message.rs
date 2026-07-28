use anyhow::{Result, anyhow, bail};
use serde::ser::{Serialize, SerializeMap, Serializer};
use std::{
    collections::HashMap,
    fmt::{self, Display},
    sync::Arc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Init,
    InitOk,
    Echo,
    EchoOk,
    Generate,
    GenerateOk,
    Broadcast,
    BroadcastOk,
    GossipBroadcast,
    GossipBroadcastOk,
    Read,
    ReadOk,
    Write,
    WriteOk,
    Cas,
    CasOk,
    Topology,
    TopologyOk,
    Add,
    AddOk,
    Send,
    SendOk,
    Poll,
    PollOk,
    CommitOffsets,
    CommitOffsetsOk,
    ListCommittedOffsets,
    ListCommittedOffsetsOk,
    Error,
}

impl Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for Type {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl Type {
    fn as_str(&self) -> &'static str {
        match self {
            Type::Init => "init",
            Type::InitOk => "init_ok",
            Type::Echo => "echo",
            Type::EchoOk => "echo_ok",
            Type::Generate => "generate",
            Type::GenerateOk => "generate_ok",
            Type::Broadcast => "broadcast",
            Type::BroadcastOk => "broadcast_ok",
            Type::GossipBroadcast => "gossip_broadcast",
            Type::GossipBroadcastOk => "gossip_broadcast_ok",
            Type::Read => "read",
            Type::ReadOk => "read_ok",
            Type::Write => "write",
            Type::WriteOk => "write_ok",
            Type::Cas => "cas",
            Type::CasOk => "cas_ok",
            Type::Topology => "topology",
            Type::TopologyOk => "topology_ok",
            Type::Add => "add",
            Type::AddOk => "add_ok",
            Type::Send => "send",
            Type::SendOk => "send_ok",
            Type::Poll => "poll",
            Type::PollOk => "poll_ok",
            Type::CommitOffsets => "commit_offsets",
            Type::CommitOffsetsOk => "commit_offsets_ok",
            Type::ListCommittedOffsets => "list_committed_offsets",
            Type::ListCommittedOffsetsOk => "list_committed_offsets_ok",
            Type::Error => "error",
        }
    }

    fn of_string(str: &str) -> Result<Self> {
        let type_ = match str {
            "init" => Type::Init,
            "init_ok" => Type::InitOk,
            "echo" => Type::Echo,
            "echo_ok" => Type::EchoOk,
            "error" => Type::Error,
            "generate" => Type::Generate,
            "generate_ok" => Type::GenerateOk,
            "broadcast" => Type::Broadcast,
            "broadcast_ok" => Type::BroadcastOk,
            "gossip_broadcast" => Type::GossipBroadcast,
            "gossip_broadcast_ok" => Type::GossipBroadcastOk,
            "read" => Type::Read,
            "read_ok" => Type::ReadOk,
            "write" => Type::Write,
            "write_ok" => Type::WriteOk,
            "cas" => Type::Cas,
            "cas_ok" => Type::CasOk,
            "topology" => Type::Topology,
            "topology_ok" => Type::TopologyOk,
            "add" => Type::Add,
            "add_ok" => Type::AddOk,
            "send" => Type::Send,
            "send_ok" => Type::SendOk,
            "poll" => Type::Poll,
            "poll_ok" => Type::PollOk,
            "commit_offsets" => Type::CommitOffsets,
            "commit_offsets_ok" => Type::CommitOffsetsOk,
            "list_committed_offsets" => Type::ListCommittedOffsets,
            "list_committed_offsets_ok" => Type::ListCommittedOffsetsOk,
            s => bail!("received unknown type {:?}", s),
        };
        Ok(type_)
    }
}

#[derive(Debug)]
pub struct Message {
    pub src: Arc<str>,
    pub dest: Arc<str>,
    pub message_id: Option<u64>,
    pub in_reply_to: Option<u64>,
    pub type_: Type,
    pub data: HashMap<String, serde_json::Value>,
}

impl Message {
    pub fn parse(input: &str) -> Result<Message> {
        let mut json: serde_json::Value = serde_json::from_str(input)?;
        let obj = json
            .as_object_mut()
            .ok_or_else(|| anyhow!("message is not an object"))?;

        let src: Arc<str> = match obj.remove("src") {
            Some(serde_json::Value::String(s)) => Arc::from(s),
            _ => bail!("src field not found"),
        };
        let dest: Arc<str> = match obj.remove("dest") {
            Some(serde_json::Value::String(s)) => Arc::from(s),
            _ => bail!("dest field not found"),
        };

        let mut body = match obj.remove("body") {
            Some(serde_json::Value::Object(m)) => m,
            _ => bail!("body field not found"),
        };

        let message_id = body.remove("msg_id").and_then(|v| v.as_u64());
        let in_reply_to = body.remove("in_reply_to").and_then(|v| v.as_u64());

        let type_ = match body.remove("type") {
            Some(serde_json::Value::String(s)) => Type::of_string(&s)?,
            _ => bail!("type field not found"),
        };

        let data: HashMap<String, serde_json::Value> = body.into_iter().collect();

        Ok(Message {
            src,
            dest,
            message_id,
            in_reply_to,
            type_,
            data,
        })
    }

    pub fn create(
        src: Arc<str>,
        dest: Arc<str>,
        message_id: u64,
        type_: Type,
        data: Vec<(&str, serde_json::Value)>,
    ) -> Result<Message> {
        Ok(Message {
            src,
            dest,
            message_id: Some(message_id),
            in_reply_to: None,
            type_,
            data: data.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        })
    }

    pub fn build_reply(
        &self,
        message_id: u64,
        type_: Type,
        data: Vec<(&str, serde_json::Value)>,
    ) -> Result<Message> {
        let in_reply_to = self
            .message_id
            .ok_or_else(|| anyhow!("attempted to build reply to a message with no id"))?;
        Ok(Message {
            src: self.dest.clone(),
            dest: self.src.clone(),
            message_id: Some(message_id),
            in_reply_to: Some(in_reply_to),
            type_,
            data: data.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        })
    }

    pub fn get(&self, key: &str) -> Result<&serde_json::Value> {
        let value = self
            .data
            .get(key)
            .ok_or_else(|| anyhow!("did not find key {:?}", key))?;
        Ok(value)
    }
}

impl Serialize for Message {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("src", self.src.as_ref())?;
        map.serialize_entry("dest", self.dest.as_ref())?;
        map.serialize_entry("body", &MessageBody(self))?;
        map.end()
    }
}

struct MessageBody<'a>(&'a Message);

impl<'a> Serialize for MessageBody<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let msg = self.0;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("type", &msg.type_)?;
        if let Some(message_id) = msg.message_id {
            map.serialize_entry("msg_id", &message_id)?;
        }
        if let Some(in_reply_to) = msg.in_reply_to {
            map.serialize_entry("in_reply_to", &in_reply_to)?;
        }
        for (key, value) in &msg.data {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}
