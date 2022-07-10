use redis::{Client, Commands, Connection};
use serde::{de::DeserializeOwned, Deserialize};

use super::Storage;

pub struct Redis {
    _client: Client,
    connection: Connection,
    scan: Option<ScanRange>,
}

#[derive(Clone)]
struct ScanRange {
    key: String,
    count: i32,
    index: i32,
}

impl Redis {
    pub fn new(url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        let conn = client.get_connection()?;

        Ok(Self {
            _client: client,
            connection: conn,
            scan: None,
        })
    }

    fn get_of_type<T>(&mut self, key: &str) -> Option<T>
    where
        T: DeserializeOwned,
    {
        match self.connection.get::<_, Vec<u8>>(key).ok() {
            Some(bytes) => serde_json::from_slice::<T>(&bytes).ok(),
            None => todo!(),
        }
    }
}

impl Storage for Redis {
    fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        match self.connection.get::<_, Vec<u8>>(key) {
            Ok(data) => {
                if data.len() == 0 {
                    None
                } else {
                    Some(data)
                }
            }
            Err(_) => None,
        }
    }

    fn push(&mut self, key: &str, value: &[u8]) -> anyhow::Result<()> {
        let count_key = format!("{}-count", key);
        let item_in_range = self.get_of_type::<i32>(&count_key).unwrap_or_else(|| -1) + 1;

        let key = format!("{}-{}", key, item_in_range);

        match self.set(&key, value) {
            Ok(_) => {
                // store the count
                self.set(&count_key, &serde_json::to_vec(&item_in_range)?)?;

                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn set(&mut self, key: &str, value: &[u8]) -> anyhow::Result<()> {
        Ok(self.connection.set(key, value)?)
    }

    fn scan_range(mut self, key: &str) -> Self {
        let count_key = format!("{}-count", key);
        let item_in_range = self.get_of_type::<i32>(&count_key).unwrap_or_else(|| 0);

        self.scan = Some(ScanRange {
            key: key.to_string(),
            count: item_in_range,
            index: 0,
        });

        self
    }
}

impl Iterator for Redis {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(scan) = &self.scan {
            let key = format!("{}-{}", scan.key, scan.index);

            self.scan = Some(ScanRange {
                index: scan.index + 1,
                ..scan.clone()
            });

            self.get(&key)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::SystemTime;

    use super::*;

    #[test]
    fn basic() {
        let data = uuid::Uuid::new_v4().to_string();
        let my_data = data.as_bytes();
        let mut storage = Redis::new("redis://127.0.0.1/").expect("connect to redis");
        storage.set("key", my_data).unwrap();

        let result = storage.get("key").unwrap();
        assert_eq!(my_data, result);
    }

    #[test]
    fn basic_range() {
        let key = format!(
            "key-{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string()
        );

        let mut storage = Redis::new("redis://127.0.0.1/").expect("connect to redis");

        for _ in 0..10 {
            storage
                .push(&key, uuid::Uuid::new_v4().to_string().as_bytes())
                .unwrap();
        }

        let scan_result = storage.scan_range(&key);
        let count = scan_result.count();

        assert_eq!(10, count);
    }
}
