use storage::Storage;

pub struct BackgroundJob;

pub struct BackgroundJobServer<S: Storage> {
    storage: S,
}

impl<S: Storage> BackgroundJobServer<S> {
    pub fn start(storage: S) -> Self {
        Self { storage: storage }
    }

    pub fn enqueue(&self, job: String) {
        let s = self.storage.get("".to_string());
    }
}

pub mod storage {
    pub trait Storage {
        fn get(&self, key: String) -> String;
    }

    pub struct MemoryStorage;

    impl Storage for MemoryStorage {
        fn get(&self, key: String) -> String {
            todo!()
        }
    }

    impl MemoryStorage {
        pub fn new() -> Self {
            Self {}
        }
    }
}
