use storage::Storage;

pub type HandlerFunc = Box<dyn Fn(String) -> anyhow::Result<()> + Sync + Send>;

pub struct BackgroundJobServer<S: Storage> {
    storage: S,
    handler_fn: HandlerFunc,
}

impl<S: Storage> BackgroundJobServer<S> {
    pub fn start(storage: S, handler_fn: HandlerFunc) -> Self {
        Self {
            storage: storage,
            handler_fn,
        }
    }

    pub fn enqueue(&self, job: String) {
        let r = (self.handler_fn)(job);
        //let s = self.storage.get("".to_string());
    }
}

macro_rules! job {
    ($ex: expr) => {
        ex
    };
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
