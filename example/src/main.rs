use fnf_rs::{storage::MemoryStorage, BackgroundJobServer};

#[macro_use]
extern crate rocket;
use rocket::State;

trait Nonee {
    fn check(&self) -> ();
}

struct NoneImpl;

// fnf_derive::background_job! {
//     impl Nonee for NoneImpl {
//         name: AppContext -> String;
//     }
// }

#[get("/")]
fn hello(state: &State<AppContext>) -> String {
    state.jobs.enqueue("Enqueued job 1".to_string());
    "Hello, world!".to_string()
}

#[get("/next")]
fn next(state: &State<AppContext>) -> String {
    state.jobs.enqueue("Enqueued job 2".to_string());
    "Hello, mo!!".to_string()
}

#[derive(Clone)]
pub struct JobContext {}

struct AppContext {
    jobs: BackgroundJobServer<MemoryStorage, JobContext>,
}

pub fn handler(ctx: JobContext, input: String) -> anyhow::Result<()> {
    println!("On Handle: {}", input);

    Ok(())
}

#[launch]
fn rocket() -> _ {
    let job_ctx = JobContext {};
    let ms = fnf_rs::storage::MemoryStorage::new();
    let bjs = BackgroundJobServer::start(ms, job_ctx, Box::new(handler));

    let ctx = AppContext { jobs: bjs };

    rocket::build().mount("/", routes![hello, next]).manage(ctx)
}
