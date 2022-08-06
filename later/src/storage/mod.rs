#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;

pub mod memory;

#[async_trait::async_trait]
pub trait Storage: Sync + Send {
    async fn get(&self, key: &str) -> Option<Vec<u8>>;
    async fn set(&self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    async fn del(&self, key: &str) -> anyhow::Result<()>;

    async fn expire(&self, key: &str, ttl_sec: usize) -> anyhow::Result<()>;

    // hashset
    async fn push(&self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    async fn trim(&self, range: &Box<dyn StorageIter>) -> anyhow::Result<()>;
    async fn scan_range(&self, key: &str) -> Box<dyn StorageIter>;
    async fn del_range(&self, key: &str) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
pub trait StorageIter: Sync + Send {
    fn get_key(&self) -> String;
    fn get_start(&self) -> usize;
    fn get_index(&self) -> usize;

    async fn next(&mut self) -> Option<Vec<u8>>;
    async fn count(&mut self) -> usize;
}
