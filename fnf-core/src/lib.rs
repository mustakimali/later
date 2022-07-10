use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// A message that can be used to enqueue a background job
pub trait JobParameter
where
    Self: Sized + Serialize + DeserializeOwned,
{
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>>;
    fn from_bytes(payload: &[u8]) -> Self;
}

// Marker trait used to decorate handles
pub trait BgJobHandler {}