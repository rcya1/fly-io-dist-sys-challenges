use anyhow::{Result, anyhow};

pub trait SerdeJsonExt {
    fn as_string(&self) -> Result<String>;
    fn as_string_array(&self) -> Result<Vec<String>>;
    fn as_num(&self) -> Result<u64>;
    fn as_obj(&self) -> Result<serde_json::Map<String, serde_json::Value>>;
}

impl SerdeJsonExt for serde_json::Value {
    fn as_string(&self) -> Result<String> {
        let value = self
            .as_str()
            .ok_or_else(|| anyhow!("could not parse {:?} as string", self))?
            .to_string();
        Ok(value)
    }

    fn as_string_array(&self) -> Result<Vec<String>> {
        let value = self
            .as_array()
            .ok_or_else(|| anyhow!("could not parse {:?} as array", self))?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| anyhow!("array element {:?} is not a string", v))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(value)
    }

    fn as_num(&self) -> Result<u64> {
        let value = self
            .as_u64()
            .ok_or_else(|| anyhow!("could not parse {:?} as u64", self))?;
        Ok(value)
    }

    fn as_obj(&self) -> Result<serde_json::Map<String, serde_json::Value>> {
        let value = self
            .as_object()
            .ok_or_else(|| anyhow!("could not parse {:?} as object", self))?
            .clone();
        Ok(value)
    }
}

pub trait SerdeMapExt {
    fn get_key(&self, key: &str) -> Result<serde_json::Value>;
}

impl SerdeMapExt for serde_json::Map<String, serde_json::Value> {
    fn get_key(&self, key: &str) -> Result<serde_json::Value> {
        let value = self
            .get(key)
            .ok_or_else(|| anyhow!("did not find key {:?} in object", key))?
            .clone();
        Ok(value)
    }
}
