use fnf_rs::{storage::MemoryStorage, BackgroundJobServer};

#[macro_use]
extern crate rocket;
use rocket::State;

#[fnf_derive::background_job]
fn job_print_time() {
    let epoch = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    println!("{}", epoch.as_millis());
}

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

struct AppContext {
    jobs: BackgroundJobServer<MemoryStorage>,
}

pub fn handler(input: String) -> anyhow::Result<()> {
    println!("On Handle: {}", input);

    Ok(())
}

#[launch]
fn rocket() -> _ {
    let ms = fnf_rs::storage::MemoryStorage::new();
    let bjs = BackgroundJobServer::start(ms, Box::new(handler));
    let ctx = AppContext { jobs: bjs };
    let ctx = ctx;

    rocket::build().mount("/", routes![hello, next]).manage(ctx)
}
