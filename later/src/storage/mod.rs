use std::collections::{HashMap, HashSet};

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;

pub trait Storage: Sync + Send {
    fn get(&self, key: &str) -> Option<Vec<u8>>;
    fn set(&self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    fn del(&self, key: &str) -> anyhow::Result<()>;

    fn push(&self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    fn trim(&self, key: &str, range: Box<dyn StorageIter>) -> anyhow::Result<()>;
    fn scan_range(&self, key: &str) -> Box<dyn StorageIter>;
}

pub trait StorageIter: Iterator<Item = Vec<u8>> {
    fn get_start(&self) -> usize;
    fn get_index(&self) -> usize;
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
