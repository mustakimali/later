use std::collections::{HashMap, HashSet};

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;

pub trait Storage {
    fn get(&mut self, key: &str) -> Option<Vec<u8>>;
    fn set(&mut self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    fn push(&mut self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    fn scan_range(self, key: &str) -> Self
    where
        Self: Iterator; // Box<dyn Iterator<Item = Vec<u8>>>;
}

pub struct MemoryStorage {
    storage: HashMap<String, String>,
    jobs: HashSet<String>,
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
            storage: Default::default(),
            jobs: Default::default(),
        }
    }
}
