use serde::{de::DeserializeOwned, Serialize};

/// A message that can be used to enqueue a background job
pub trait JobParameter
where
    Self: Sized + Serialize + DeserializeOwned,
{
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>>;
    fn from_bytes(payload: &[u8]) -> Self;
    fn get_ptype(&self) -> String;
}

// Marker trait used to decorate handles
pub trait BgJobHandler<C> {
    fn get_ctx(&self) -> &C;
    fn dispatch(&self, ptype: String, payload: &[u8]) -> anyhow::Result<()>;
}
