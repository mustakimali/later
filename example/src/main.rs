use std::{sync::{Arc, Mutex, MutexGuard}, ops::{Deref, DerefMut}};

use fnf_rs::{storage::MemoryStorage, BackgroundJobServer};

#[macro_use]
extern crate rocket;
use rocket::State;

#[derive(Clone)]
pub struct ArcMtx<T> {
    inner: Arc<Mutex<T>>,
}

impl<T> ArcMtx<T> {
    pub fn new(item: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(item)),
        }
    }

    pub fn get(&self) -> MutexGuard<'_, T> {
        self.inner.lock().unwrap()
    }
}

// fnf_derive::background_job! {
//     impl None for NoneImpl {
//         name: AppContext -> String;
//     }
// }

#[get("/")]
fn hello(state: &State<Arc<Mutex<AppContext>>>) -> String {
    state
        .lock()
        .unwrap()
        .jobs
        .enqueue("Enqueued job 1".to_string());
    "Hello, world!".to_string()
}

#[get("/next")]
fn next(state: &State<Arc<Mutex<AppContext>>>) -> String {
    state
        .lock()
        .unwrap()
        .jobs
        .enqueue("Enqueued job 2".to_string());
    "Hello, mo!!".to_string()
}

#[derive(Clone)]
pub struct JobContext {}

struct AppContext {
    jobs: BackgroundJobServer<MemoryStorage, ArcMtx<JobContext>>,
}

pub fn handler(ctx: ArcMtx<JobContext>, input: String) -> anyhow::Result<()> {
    println!("On Handle: {}", input);

    Ok(())
}

#[launch]
fn rocket() -> _ {
    let job_ctx = ArcMtx::new(JobContext {});
    let ms = fnf_rs::storage::MemoryStorage::new();
    let bjs = BackgroundJobServer::start(ms, job_ctx, Box::new(handler));

    let ctx = Arc::new(Mutex::new(AppContext { jobs: bjs }));

    rocket::build().mount("/", routes![hello, next]).manage(ctx)
}
