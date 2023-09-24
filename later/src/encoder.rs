use serde::{de::DeserializeOwned, Serialize};

pub fn encode(input: impl Serialize) -> anyhow::Result<Vec<u8>> {
    Ok(rmp_serde::to_vec(&input)?)
}

pub fn encode_json(input: impl Serialize) -> anyhow::Result<String> {
    Ok(serde_json::to_string_pretty(&input)?)
}

pub fn decode<T: DeserializeOwned>(input: &[u8]) -> anyhow::Result<T> {
    Ok(rmp_serde::from_slice(input)?)
}

pub fn hash(input: &[u8]) -> String {
    blake3::hash(input).to_hex().to_lowercase()
}
