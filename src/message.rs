use anyhow::{Result, anyhow, bail};
use std::collections::HashMap;

pub enum Type {
    Init,
    InitOk,
    Echo,
    Error,
}

impl Type {
    fn to_string(self: &Self) -> String {
        match self {
            Type::Init => "init".to_string(),
            Type::InitOk => "init_ok".to_string(),
            Type::Echo => "echo".to_string(),
            Type::Error => "error".to_string(),
        }
    }

    fn of_string(str: &str) -> Result<Self> {
        let type_ = match str {
            "init" => Type::Init,
            "init_ok" => Type::InitOk,
            "echo" => Type::Echo,
            "error" => Type::Error,
            s => bail!("received unknown type {:?}", s),
        };
        Ok(type_)
    }
}

pub struct Message {
    src: String,
    dest: String,
    type_: Type,
    message_id: Option<u64>,
    in_reply_to: Option<u64>,
    data: HashMap<String, String>,
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

        let type_ = body["type"]
            .as_str()
            .ok_or_else(|| anyhow!("type field not found"))?;
        let type_ = match type_ {
            "init" => Type::Init,
            "echo" => Type::Echo,
            other => bail!("unknown type: {other}"),
        };

        let message_id = body.get("message_id").and_then(|v| v.as_u64());
        let in_reply_to = body.get("in_reply_to").and_then(|v| v.as_u64());

        let data: HashMap<String, String> = body
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "type" | "message_id" | "in_reply_to"))
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect();

        Ok(Message {
            src,
            dest,
            type_,
            message_id,
            in_reply_to,
            data,
        })
    }

    pub fn to_string(self: Self) -> String {
        use serde_json::*;
        let mut map = Map::new();
        map["src"] = Value::String(self.src);
        map["dest"] = Value::String(self.dest);

        let mut body = Map::new();
        body["type"] = Value::String(self.type_.to_string());
        if let Some(message_id) = self.message_id {
            body["message_id"] = Value::Number(message_id.into());
        }
        if let Some(in_reply_to) = self.in_reply_to {
            body["in_reply_to"] = Value::Number(in_reply_to.into());
        }
        self.data.iter().for_each(|(key, value)| {
            body[key] = Value::String(value.to_string());
        });
        map["body"] = Value::Object(body);

        Value::to_string(&Value::Object(map))
    }
}
