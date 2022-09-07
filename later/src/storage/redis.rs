use std::sync::Arc;

use super::{LockHandle, LockHandler, Storage};
use redis::{aio::Connection, AsyncCommands, Client};
use relock::Relock;

#[derive(Clone)]
pub struct Redis {
    _client: Client,
    relock: Relock,
    connection: Arc<async_mutex::Mutex<Connection>>,
}

impl Redis {
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        let conn = client.get_async_connection().await?;

        let relock = Relock::new(url)?;

        Ok(Self {
            relock,
            _client: client,
            connection: Arc::new(async_mutex::Mutex::new(conn)),
        })
    }
}

#[async_trait::async_trait]
impl Storage for Redis {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        match self.connection.lock().await.get::<_, Vec<u8>>(key).await {
            Ok(data) => {
                if data.len() == 0 {
                    None
                } else {
                    Some(data)
                }
            }
            _ => None,
        }
    }

    async fn set(&self, key: &str, value: &[u8]) -> anyhow::Result<()> {
        Ok(self.connection.lock().await.set(key, value).await?)
    }

    async fn del(&self, key: &str) -> anyhow::Result<()> {
        Ok(self.connection.lock().await.del(key).await?)
    }

    async fn expire(&self, key: &str, ttl_sec: usize) -> anyhow::Result<()> {
        self.connection
            .lock()
            .await
            .expire(key, ttl_sec)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn exist(&self, key: &str) -> anyhow::Result<bool> {
        self.connection
            .lock()
            .await
            .exists(key)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn lock(&self, key: &str) -> anyhow::Result<LockHandle> {
        let ttl = 10_000;
        let retry_count = ttl as u32;
        let retry_delay = 10;
        let key = format!("lock-{}", key);

        let lock = self
            .relock
            .lock(key.clone(), ttl, retry_count, retry_delay)
            .await?;
        let lock = ReLockHandle(lock, self.relock.clone(), key);
        Ok(LockHandle::new(lock))
    }

    async fn atomic_incr(&self, key: &str, delta: usize) -> anyhow::Result<usize> {
        let value = self
            .connection
            .lock()
            .await
            .incr::<_, _, usize>(key, delta)
            .await?;

        Ok(value)
    }
}

struct ReLockHandle(relock::Lock, relock::Relock, String);
impl LockHandler for ReLockHandle {
    fn release(&mut self) {
        let _ = self.1.unlock(self.2.clone(), self.0.id.clone());
    }
}
