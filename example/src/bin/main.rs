#[macro_use]
extern crate rocket;

use bg::*;
use later::{storage::redis::Redis, BackgroundJobServer};
use rocket::State;

mod bg {
    use serde::{Deserialize, Serialize};

    later::background_job! {
        struct DeriveHandler {
            sample_message: SampleMessage,
            another_sample_message: AnotherSampleMessage,
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct SampleMessage {
        pub txt: String,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct AnotherSampleMessage {
        pub txt: String,
    }

    #[derive(Clone)]
    pub struct JobContext {}
}

async fn handle_sample_message(
    _ctx: DeriveHandlerContext<JobContext>,
    payload: SampleMessage,
) -> anyhow::Result<()> {
    println!("On Handle handle_sample_message: {:?}", payload);

    Ok(())
}

async fn handle_another_sample_message(
    _ctx: DeriveHandlerContext<JobContext>,
    payload: AnotherSampleMessage,
) -> anyhow::Result<()> {
    let id = _ctx
        .enqueue(SampleMessage {
            txt: "test".to_string(),
        })
        .await?;
    let id2 = _ctx
        .enqueue_continue(
            id.clone(),
            SampleMessage {
                txt: format!("Continuation of job {}", id),
            },
        )
        .await?;

    println!(
        "On Handle handle_another_sample_message: {:?}, enqueued: {}, {}",
        payload, id, id2
    );

    Ok(())
}

struct AppContext {
    jobs: BackgroundJobServer<JobContext, DeriveHandler<JobContext>>,
}

#[get("/")]
async fn hello(state: &State<AppContext>) -> String {
    let id = later::generate_id();
    let msg = AnotherSampleMessage { txt: id };
    state.jobs.enqueue(msg).await.expect("Enqueue Job");
    "Hello, world!".to_string()
}

#[get("/metrics")]
async fn metrics(state: &State<AppContext>) -> String {
    state.jobs.get_metrics().expect("metrics")
}

#[launch]
async fn rocket() -> _ {
    let job_ctx = JobContext {};
    let storage = Redis::new("redis://127.0.0.1/")
        .await
        .expect("connect to redis");
    let bjs = DeriveHandlerBuilder::new(
        job_ctx,
        "fnf-example".into(),
        "amqp://guest:guest@localhost:5672".into(),
        Box::new(storage),
    )
    .with_sample_message_handler(handle_sample_message)
    .with_another_sample_message_handler(handle_another_sample_message)
    .build()
    .expect("start bg server");

    let ctx = AppContext { jobs: bjs };

    rocket::build()
        .mount("/", routes![hello, metrics])
        .manage(ctx)
}
