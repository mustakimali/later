use std::sync::{Arc, Mutex, MutexGuard};

use super::{Storage, StorageIter};
use crate::encoder;
use redis::{Client, Commands, Connection};
use serde::de::DeserializeOwned;

#[derive(Clone)]
pub struct Redis {
    _client: Client,
    connection: Arc<Mutex<Connection>>,
}

struct ScanRange {
    key: String,
    count: usize,
    start: usize,
    index: usize,
    parent: Redis,
}

impl Redis {
    pub fn new(url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        let conn = client.get_connection()?;

        Ok(Self {
            _client: client,
            connection: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn new_cleared(url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        let mut conn = client.get_connection()?;

        redis::cmd("FLUSHDB").query(&mut conn)?;

        Ok(Self {
            _client: client,
            connection: Arc::new(Mutex::new(conn)),
        })
    }

    fn get_of_type<T>(&self, key: &str) -> Option<T>
    where
        T: DeserializeOwned,
    {
        match self
            .get_connection()
            .ok()
            .map(|mut c| c.get::<_, Vec<u8>>(key))
        {
            Some(Ok(bytes)) => encoder::decode::<T>(&bytes).ok(),
            _ => None,
        }
    }

    fn get_connection(&self) -> anyhow::Result<MutexGuard<'_, Connection>> {
        self.connection.lock().map_err(|e| anyhow::anyhow!("{}", e))
    }
}

#[async_trait::async_trait]
impl Storage for Redis {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        match self
            .get_connection()
            .ok()
            .map(|mut c| c.get::<_, Vec<u8>>(key))
        {
            Some(Ok(data)) => {
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
        Ok(self.get_connection()?.set(key, value)?)
    }

    async fn del(&self, key: &str) -> anyhow::Result<()> {
        Ok(self.get_connection()?.del(key)?)
    }

    async fn push(&self, key: &str, value: &[u8]) -> anyhow::Result<()> {
        let count_key = format!("{}-count", key);
        let count = self.get_of_type::<i32>(&count_key).unwrap_or_else(|| 0);

        let key = format!("{}-{}", key, count);

        match self.set(&key, value).await {
            Ok(_) => {
                // store the count
                self.set(&count_key, &encoder::encode(&count + 1)?).await?;

                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn trim(&self, range: &Box<dyn StorageIter>) -> anyhow::Result<()> {
        let key = range.get_key();
        let start_key = format!("{}-start", key);
        let idx = range.get_index();

        if idx == range.get_start() {
            return Ok(());
        }

        self.set(&start_key, &encoder::encode(idx)?).await?;

        let start = range.get_start();
        let end = range.get_index();
        for i in start..end {
            let key = get_scan_item_key(&key, i);
            let _ = self.del(&key);
        }

        Ok(())
    }

    async fn scan_range(&self, key: &str) -> Box<dyn StorageIter> {
        let start_key = format!("{}-start", key);
        let count_key = format!("{}-count", key);
        let start_from_idx = self.get_of_type::<usize>(&start_key).unwrap_or_else(|| 0);
        let item_in_range = self.get_of_type::<usize>(&count_key).unwrap_or_else(|| 0);

        let scan = ScanRange {
            key: key.to_string(),
            count: item_in_range,
            start: start_from_idx,
            index: start_from_idx,
            parent: self.clone(),
        };

        Box::new(scan)
    }
}

#[async_trait::async_trait]
impl StorageIter for ScanRange {
    fn get_index(&self) -> usize {
        self.index
    }

    fn get_start(&self) -> usize {
        self.start
    }

    fn get_key(&self) -> String {
        self.key.clone()
    }

    async fn next(&mut self) -> Option<Vec<u8>> {
        if self.count == 0 {
            return None;
        }

        let key = get_scan_item_key(&self.key, self.index);

        let item = self.parent.get(&key).await;

        if item.is_some() {
            self.index += 1;
        }

        item
    }

    async fn count(&mut self) -> usize {
        let mut count = 0;
        while self.next().await.is_some() {
            count += 1;
        }

        count
    }
}

fn get_scan_item_key(range_key: &str, idx: usize) -> String {
    format!("{}-{}", range_key, idx)
}

#[cfg(test)]
mod test {

    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn basic() {
        let data = uuid::Uuid::new_v4().to_string();
        let my_data = data.as_bytes();
        let storage = Redis::new("redis://127.0.0.1/").expect("connect to redis");
        storage.set("key", my_data).await.unwrap();

        let result = storage.get("key").await.unwrap();
        assert_eq!(my_data, result);
    }

    #[tokio::test]
    async fn range_basic() {
        let key = format!("key-{}", Uuid::new_v4().to_string());

        let storage = Redis::new("redis://127.0.0.1/").expect("connect to redis");

        for _ in 0..10 {
            storage
                .push(&key, uuid::Uuid::new_v4().to_string().as_bytes())
                .await
                .unwrap();
        }

        let mut scan_result = storage.scan_range(&key).await;
        let count = scan_result.count().await;

        assert_eq!(10, count);
    }

    #[tokio::test]
    async fn range_trim() {
        let key = format!("key-{}", Uuid::new_v4().to_string());

        let storage = Redis::new("redis://127.0.0.1/").expect("connect to redis");

        for idx in 0..100 {
            storage
                .push(&key, idx.to_string().as_bytes())
                .await
                .unwrap();
        }

        assert_eq!(100, storage.scan_range(&key).await.count().await);

        // scan first 50
        let mut range = storage.scan_range(&key).await;
        let mut counter = 0;
        while counter < 50 {
            let next_item = range.next().await;
            assert!(next_item.is_some());

            counter += 1;
        }

        // trim
        let _ = storage.trim(&range).await;

        // should have only 50
        let mut range = storage.scan_range(&key).await;
        assert_eq!(50, range.count().await);

        // should be empty
        let mut range = storage.scan_range(&key).await;
        while range.next().await.is_some() {}
        let _ = storage.trim(&range);

        assert_eq!(0, storage.scan_range(&key).await.count().await); // should be empty
    }
}
