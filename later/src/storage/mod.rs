use std::collections::{HashMap, HashSet};

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;

#[async_trait::async_trait]
pub trait Storage: Sync + Send {
    async fn get(&self, key: &str) -> Option<Vec<u8>>;
    async fn set(&self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    async fn del(&self, key: &str) -> anyhow::Result<()>;

    async fn push(&self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    async fn trim(&self, range: &Box<dyn StorageIter>) -> anyhow::Result<()>;
    async fn scan_range(&self, key: &str) -> Box<dyn StorageIter>;
}

#[async_trait::async_trait]
pub trait StorageIter : Sync + Send {
    fn get_key(&self) -> String;
    fn get_start(&self) -> usize;
    fn get_index(&self) -> usize;
    
    async fn next(&mut self) -> Option<Vec<u8>>;
    async fn count(&mut self) -> usize;
}

pub struct MemoryStorage {
    _storage: HashMap<String, String>,
    _jobs: HashSet<String>,
}

// impl Storage for MemoryStorage {
//     fn get(&'_ self, key: &str) -> Option<&'_ str> {
//         let r = self.storage.get(key).map(|x| x.as_str());

//         r
//     }

//     fn set(&mut self, key: String, value: String) {
//         self.storage.insert(key, value);
//     }
// }

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            _storage: Default::default(),
            _jobs: Default::default(),
        }
    }
}
