#[macro_use]
extern crate rocket;

use bg::*;
use later::{storage::redis::Redis, BackgroundJobServer};
use rocket::State;

mod bg;
#[allow(dead_code)]
mod non_generated;

fn handle_sample_message(
    _ctx: &DeriveHandlerContext<JobContext>,
    payload: SampleMessage,
) -> anyhow::Result<()> {
    println!("On Handle handle_sample_message: {:?}", payload);

    Ok(())
}
fn handle_another_sample_message(
    _ctx: &DeriveHandlerContext<JobContext>,
    payload: AnotherSampleMessage,
) -> anyhow::Result<()> {
    let id = _ctx.enqueue(SampleMessage {
        txt: "test".to_string(),
    })?;

    println!(
        "On Handle handle_another_sample_message: {:?}, enqueued: {}",
        payload, id
    );

    Ok(())
}

struct AppContext {
    jobs: BackgroundJobServer<JobContext, DeriveHandler<JobContext>>,
}

#[get("/")]
fn hello(state: &State<AppContext>) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let msg = AnotherSampleMessage { txt: id };
    state.jobs.enqueue(msg).expect("Enqueue Job");
    "Hello, world!".to_string()
}

#[launch]
fn rocket() -> _ {
    let job_ctx = JobContext {};
    let storage = Redis::new("redis://127.0.0.1/").expect("connect to redis");
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

    #[cfg(debug_assertions)]
    non_generated::test_non_generated();

    rocket::build().mount("/", routes![hello]).manage(ctx)
}
