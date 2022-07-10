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

fn handle_sample_message(ctx: &JobContext, payload: SampleMessage) -> anyhow::Result<()> {
    println!("On Handle handle_sample_message: {:?}", payload);

    Ok(())
}
fn handle_another_sample_message(
    ctx: &JobContext,
    payload: AnotherSampleMessage,
) -> anyhow::Result<()> {
    println!("On Handle handle_another_sample_message: {:?}", payload);

    Ok(())
}

struct AppContext {
    jobs: Arc<Mutex<BackgroundJobServer<JobContext, DeriveHandler<JobContext>>>>,
}

impl AppContext {
    pub fn enqueue<T: fnf_rs::JobParameter>(&self, msg: T) -> anyhow::Result<()> {
        let x = self.jobs.lock().unwrap();
        x.enqueue(msg)
    }
}

#[get("/")]
fn hello(state: &State<AppContext>) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let msg = AnotherSampleMessage { txt: id };
    state
        .enqueue(msg)
        .expect("Enqueue Job");
    "Hello, world!".to_string()
}

#[launch]
fn rocket() -> _ {
    // let handles = DeriveHandler {
    //     sample_message: Some(Box::new(handle_sample_message)),
    //     another_sample_message: Some(Box::new(handle_another_sample_message)),
    // };
    let job_ctx = JobContext {};
    let handles = DeriveHandlerBuilder::new(job_ctx)
        .with_sample_message_handler(handle_sample_message)
        .with_another_sample_message_handler(handle_another_sample_message)
        .build();

    //let ms = fnf_rs::storage::MemoryStorage::new();
    let bjs = BackgroundJobServer::start(
        "fnf-example",
        "amqp://guest:guest@localhost:5672".into(),
        handles,
    )
    .expect("start bg server");

    let ctx = AppContext {
        jobs: Arc::new(Mutex::new(bjs)),
    };

    rocket::build().mount("/", routes![hello]).manage(ctx)
}
