use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, MutexGuard},
};

use fnf_rs::{
    serde::Serialize, storage::MemoryStorage, BackgroundJobServer, HangfireMessage,
    HangfireMessageO,
};

#[macro_use]
extern crate rocket;
use rocket::State;
use serde::{de::DeserializeOwned, Deserialize};

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

fnf_derive::background_job! {
    struct DeriveHandler<JobContext> {
        sample_message: SampleMessage,
        another_sample_message: AnotherSampleMessage,
    }
}


pub fn handler(ctx: ArcMtx<JobContext>, input: String) -> anyhow::Result<()> {
    println!("On Handle: {}", input);

    Ok(())
}

struct Handlers {
    simple_message: Box<dyn Fn(&JobContext, SampleMessage) -> anyhow::Result<()>>,
    another_simple_message: Box<dyn Fn(&JobContext, AnotherSampleMessage) -> anyhow::Result<()>>,
}

#[derive(Serialize, Deserialize, Default)]
struct SampleMessage {
    txt: String,
}

#[derive(Serialize, Deserialize, Default)]
struct AnotherSampleMessage {
    txt: String,
}

// -- Generated --

impl ::fnf_rs::HangfireMessage for SampleMessage {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let result = ::fnf_rs::serde_json::to_vec(&self);
        let result = ::fnf_rs::anyhow::Context::context(result, "unable to serialize");
        Ok(result?)
    }
}

impl ::fnf_rs::HangfireMessage for AnotherSampleMessage {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let result = ::fnf_rs::serde_json::to_vec(&self);
        let result = ::fnf_rs::anyhow::Context::context(result, "unable to serialize");
        Ok(result?)
    }
}

impl ::fnf_rs::HangfireMessageO for SampleMessage {
    fn from_bytes(payload: &[u8]) -> Self {
        ::fnf_rs::serde_json::from_slice(payload).unwrap()
    }
}
impl ::fnf_rs::HangfireMessageO for AnotherSampleMessage {
    fn from_bytes(payload: &[u8]) -> Self {
        ::fnf_rs::serde_json::from_slice(payload).unwrap()
    }
}

fn parse(ctx: JobContext, ptype: String, payload: &[u8]) -> anyhow::Result<()> {
    let handles = Handlers {
        simple_message: todo!(),
        another_simple_message: todo!(),
    };

    //-> Box<dyn ::fnf_rs::HangfireMessage> {
    match ptype.as_str() {
        "sample_message" => {
            let payload = SampleMessage::from_bytes(payload);
            (handles.simple_message)(&ctx, payload)
        },
        "another_sample_message" => {
            let payload = AnotherSampleMessage::from_bytes(payload);
            (handles.another_simple_message)(&ctx, payload)
        }
        _ => unreachable!(),
    }
}

// -- Generated --

#[derive(Clone)]
pub struct JobContext {}

struct AppContext {
    jobs: BackgroundJobServer<MemoryStorage, ArcMtx<JobContext>>,
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
