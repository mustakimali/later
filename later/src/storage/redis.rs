use std::sync::Arc;

use super::Storage;
use redis::{aio::Connection, AsyncCommands, Client};

#[derive(Clone)]
pub struct Redis {
    _client: Client,
    connection: Arc<async_mutex::Mutex<Connection>>,
}

impl Redis {
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        let conn = client.get_async_connection().await?;

        Ok(Self {
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
}
