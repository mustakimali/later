use serde::{de::DeserializeOwned, Serialize};

pub fn encode(input: impl Serialize) -> anyhow::Result<Vec<u8>> {
    Ok(rmp_serde::to_vec(&input)?)
}

pub fn decode<T: DeserializeOwned>(input: &[u8]) -> anyhow::Result<T> {
    Ok(rmp_serde::from_slice(input)?)
}
