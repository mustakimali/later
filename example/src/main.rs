use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, MutexGuard},
};

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
    let id = uuid::Uuid::new_v4().to_string();
    state
        .lock()
        .unwrap()
        .jobs
        .enqueue(format!("Message: {}", id))
        .expect("Enqueue Job");
    "Hello, world!".to_string()
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
    let bjs = BackgroundJobServer::start(
        "fnf-example",
        "amqp://guest:guest@localhost:5672".into(),
        ms,
        job_ctx,
        Box::new(handler),
    )
    .expect("start bg server");

    let ctx = Arc::new(Mutex::new(AppContext { jobs: bjs }));

    let port = std::option_env!("PORT").unwrap_or_else(|| "8000");
    rocket::build().mount("/", routes![hello]).manage(ctx)
}
