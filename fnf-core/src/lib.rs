use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub trait HangfireMessage // where
//     Self: Sized + Serialize + DeserializeOwned,
{
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>>;
}

pub trait HangfireMessageO
where
    Self: Sized + Serialize + DeserializeOwned + Default,
{
    fn from_bytes(payload: &[u8]) -> Self;
}
