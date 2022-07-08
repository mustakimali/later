use std::sync::{Arc, Mutex};

use storage::Storage;

pub type HandlerFunc<C> = Box<dyn Fn(C, String) -> anyhow::Result<()> + Sync + Send>;

pub struct BackgroundJobServer<S, C>
where
    S: Storage + Send + Sync,
    C: Send + Sync + Clone,
{
    storage: S,
    ctx: C,
    handler_fn: Arc<HandlerFunc<C>>,
}

impl<S, C> BackgroundJobServer<S, C>
where
    S: Storage + Sync + Send,
    C: Sync + Send + Clone,
{
    pub fn start(storage: S, context: C, handler_fn: HandlerFunc<C>) -> Self {
        Self {
            storage: storage,
            ctx: context,
            handler_fn: Arc::new(handler_fn),
        }
    }

    pub fn enqueue(&mut self, job: String) {
        let id = uuid::Uuid::new_v4().to_string();
        let handler = self.handler_fn.clone();
        let ctx = self.ctx.clone();

        self.storage.set(format!("job-{}", id), job);
        self.storage.push_job_id(id);
        
        // std::thread::spawn(move || {
        //     let r = (handler)(ctx, job);
        // });

        //let s = self.storage.get("".to_string());
    }
}

fn worker() {}

pub mod storage {
    use std::collections::{HashMap, HashSet};

    pub trait Storage {
        fn get(&'_ self, key: &str) -> Option<&'_ str>;
        fn push_job_id(&mut self, id: String);
        fn set(&mut self, key: String, value: String);
    }

    pub struct MemoryStorage {
        storage: HashMap<String, String>,
        jobs: HashSet<String>,
    }

    impl Storage for MemoryStorage {
        fn get(&'_ self, key: &str) -> Option<&'_ str> {
            let r = self.storage.get(key).map(|x| x.as_str());

            r
        }

        fn set(&mut self, key: String, value: String) {
            self.storage.insert(key, value);
        }

        fn push_job_id(&mut self, id: String) {
            self.jobs.insert(id);
        }
    }

    impl MemoryStorage {
        pub fn new() -> Self {
            Self {
                storage: Default::default(),
                jobs: Default::default(),
            }
        }
    }
}
