use fnf_rs::{storage::MemoryStorage, BackgroundJobServer};

#[macro_use]
extern crate rocket;
use rocket::State;

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

#[launch]
fn rocket() -> _ {
    let ms = fnf_rs::storage::MemoryStorage::new();
    let bjs = BackgroundJobServer::start(ms);
    let ctx = AppContext { jobs: bjs };
    let ctx = ctx;

    rocket::build().mount("/", routes![hello, next]).manage(ctx)
}
