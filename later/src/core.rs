use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::{encoder, BackgroundJobServerPublisher};

/// A message that can be used to enqueue a background job
pub trait JobParameter
where
    Self: Serialize + DeserializeOwned,
{
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>>;
    fn from_bytes(payload: &[u8]) -> Self;
    fn get_ptype(&self) -> String;
}

#[async_trait]
// Marker trait used to decorate handles
pub trait BgJobHandler<C> {
    fn get_ctx(&self) -> &C;
    fn get_publisher(&self) -> &BackgroundJobServerPublisher;
    async fn dispatch(&self, ptype: String, payload: &[u8]) -> anyhow::Result<()>;
}

pub(crate) trait ToJson {
    fn to_json(&self) -> anyhow::Result<String>;
}

impl<T: JobParameter> ToJson for T {
    fn to_json(&self) -> anyhow::Result<String> {
        encoder::encode_json(self)
    }
}
