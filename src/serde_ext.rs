use anyhow::{Result, anyhow};

pub trait SerdeJsonExt {
    fn as_string(&self) -> Result<&str>;
    fn as_string_array(&self) -> Result<Vec<&str>>;
    fn as_num(&self) -> Result<u64>;
    fn as_num_array(&self) -> Result<Vec<u64>>;
}

impl SerdeJsonExt for serde_json::Value {
    fn as_string(&self) -> Result<&str> {
        let value = self
            .as_str()
            .ok_or_else(|| anyhow!("could not parse {:?} as string", self))?;
        Ok(value)
    }

    fn as_string_array(&self) -> Result<Vec<&str>> {
        let value = self
            .as_array()
            .ok_or_else(|| anyhow!("could not parse {:?} as array", self))?
            .iter()
            .map(|v| {
                v.as_str()
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

    fn as_num_array(&self) -> Result<Vec<u64>> {
        let value = self
            .as_array()
            .ok_or_else(|| anyhow!("could not parse {:?} as array", self))?
            .iter()
            .map(|v| {
                v.as_u64()
                    .ok_or_else(|| anyhow!("array element {:?} is not a u64", v))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(value)
    }
}
