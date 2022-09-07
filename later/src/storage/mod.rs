pub mod memory;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;

pub(crate) mod storage_iter;

#[cfg(feature = "postgres")]
pub use crate::storage::postgres::Postgres;

#[cfg(feature = "redis")]
pub use crate::storage::redis::Redis;

#[cfg(test)]
mod tests;

pub(crate) use storage_iter::StorageEx;
pub(crate) use storage_iter::StorageIter;

#[async_trait::async_trait]
pub trait Storage: Sync + Send {
    async fn get(&self, key: &str) -> Option<Vec<u8>>;
    async fn set(&self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    async fn del(&self, key: &str) -> anyhow::Result<()>;
    async fn exist(&self, key: &str) -> anyhow::Result<bool>;

    async fn expire(&self, key: &str, ttl_sec: usize) -> anyhow::Result<()>;

    async fn lock(&self, key: &str) -> anyhow::Result<LockHandle>;
    async fn atomic_incr(&self, key: &str, delta: usize) -> anyhow::Result<usize>;
}

pub struct LockHandle {
    inner: Box<dyn LockHandler + Send + Sync>,
}

impl LockHandle {
    pub fn new<H: LockHandler + Send + Sync + 'static>(handler: H) -> Self {
        Self {
            inner: Box::new(handler),
        }
    }
}

pub trait LockHandler {
    fn release(&mut self);
}

impl Drop for LockHandle {
    fn drop(&mut self) {
        self.inner.release();
    }
}
