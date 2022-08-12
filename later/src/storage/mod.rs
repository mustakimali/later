pub mod memory;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;
pub(crate) mod storage_iter;

pub use storage_iter::StorageIter;
pub use storage_iter::StorageIterator;

#[async_trait::async_trait]
pub trait Storage: Sync + Send {
    async fn get(&self, key: &str) -> Option<Vec<u8>>;
    async fn set(&self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    async fn del(&self, key: &str) -> anyhow::Result<()>;

    async fn expire(&self, key: &str, ttl_sec: usize) -> anyhow::Result<()>;
}
