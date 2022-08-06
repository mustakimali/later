use std::collections::HashMap;

pub struct MemoryStorage {
    _storage: HashMap<String, String>,
}
impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            _storage: Default::default(),
        }
    }
}
