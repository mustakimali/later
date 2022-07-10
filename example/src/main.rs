#[macro_use]
extern crate rocket;

use bg::*;
use fnf_rs::{storage::MemoryStorage, BackgroundJobServer};
use rocket::State;
use std::sync::{Arc, Mutex, MutexGuard};

mod bg;

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

pub fn handler(ctx: ArcMtx<JobContext>, input: String) -> anyhow::Result<()> {
    println!("On Handle: {}", input);

    Ok(())
}

fn handle_sample_message(ctx: &JobContext, payload: SampleMessage) -> anyhow::Result<()> {
    println!("On Handle handle_sample_message: {:?}", payload);

    Ok(())
}
fn handle_another_sample_message(ctx: &JobContext, payload: AnotherSampleMessage) -> anyhow::Result<()> {
    println!("On Handle handle_another_sample_message: {:?}", payload);

    Ok(())
}

struct AppContext {
    jobs: BackgroundJobServer<MemoryStorage, JobContext, DeriveHandler<JobContext>>,
}

#[get("/")]
fn hello(state: &State<Arc<Mutex<AppContext>>>) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let msg = SampleMessage { txt: id };
    state
        .lock()
        .unwrap()
        .jobs
        .enqueue(msg)
        .expect("Enqueue Job");
    "Hello, world!".to_string()
}

#[launch]
fn rocket() -> _ {
    let handles = DeriveHandler {
        sample_message: Box::new(handle_sample_message),
        another_sample_message: Box::new(handle_another_sample_message),
    };
    let job_ctx = JobContext {};
    let ms = fnf_rs::storage::MemoryStorage::new();
    let bjs = BackgroundJobServer::start(
        "fnf-example",
        "amqp://guest:guest@localhost:5672".into(),
        ms,
        job_ctx,
        handles
    )
    .expect("start bg server");

    let ctx = Arc::new(Mutex::new(AppContext { jobs: bjs }));

    rocket::build().mount("/", routes![hello]).manage(ctx)
}
