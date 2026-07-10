use anyhow::{Result, anyhow, bail};
use std::{
    collections::HashMap,
    fmt::{self, Display},
};

#[derive(Debug)]
pub enum Type {
    Init,
    InitOk,
    Echo,
    EchoOk,
    Generate,
    GenerateOk,
    Error,
}

impl Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Type::Init => "init",
            Type::InitOk => "init_ok",
            Type::Echo => "echo",
            Type::EchoOk => "echo_ok",
            Type::Generate => "generate",
            Type::GenerateOk => "generate_ok",
            Type::Error => "error",
        };
        f.write_str(str)
    }
}

impl Type {
    fn of_string(str: &str) -> Result<Self> {
        let type_ = match str {
            "init" => Type::Init,
            "init_ok" => Type::InitOk,
            "echo" => Type::Echo,
            "echo_ok" => Type::EchoOk,
            "error" => Type::Error,
            "generate" => Type::Generate,
            "generate_ok" => Type::GenerateOk,
            s => bail!("received unknown type {:?}", s),
        };
        Ok(type_)
    }
}

#[derive(Debug)]
pub struct Message {
    src: String,
    dest: String,
    message_id: Option<u64>,
    in_reply_to: Option<u64>,
    pub type_: Type,
    pub data: HashMap<String, String>,
}

impl Message {
    pub fn parse(input: &str) -> Result<Message> {
        let json: serde_json::Value = serde_json::from_str(input)?;
        let src = json["src"]
            .as_str()
            .ok_or_else(|| anyhow!("src field not found"))?
            .to_string();
        let dest = json["dest"]
            .as_str()
            .ok_or_else(|| anyhow!("dest field not found"))?
            .to_string();

        let body = json["body"]
            .as_object()
            .ok_or_else(|| anyhow!("body field not found"))?;

        let message_id = body.get("msg_id").and_then(|v| v.as_u64());
        let in_reply_to = body.get("in_reply_to").and_then(|v| v.as_u64());

        let type_ = body["type"]
            .as_str()
            .ok_or_else(|| anyhow!("type field not found"))?;
        let type_ = Type::of_string(type_)?;

        let data: HashMap<String, String> = body
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "type" | "msg_id" | "in_reply_to"))
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect();

        Ok(Message {
            src,
            dest,
            message_id,
            in_reply_to,
            type_,
            data,
        })
    }

    pub fn build_reply(
        &self,
        message_id: u64,
        type_: Type,
        data: HashMap<String, String>,
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
            data,
        })
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use serde_json::*;
        let mut map = Map::new();
        map.insert("src".into(), Value::String(self.src.clone()));
        map.insert("dest".into(), Value::String(self.dest.clone()));

        let mut body = Map::new();
        body.insert("type".into(), Value::String(self.type_.to_string()));
        if let Some(message_id) = self.message_id {
            body.insert("msg_id".into(), Value::Number(message_id.into()));
        }
        if let Some(in_reply_to) = self.in_reply_to {
            body.insert("in_reply_to".into(), Value::Number(in_reply_to.into()));
        }
        self.data.iter().for_each(|(key, value)| {
            body.insert(key.clone(), Value::String(value.to_string()));
        });
        map.insert("body".into(), Value::Object(body));

        let str = Value::to_string(&Value::Object(map));
        f.write_str(&str)
    }
}
