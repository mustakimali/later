use std::sync::{Arc, Mutex};

use fnf_rs::{BackgroundJob, BackgroundJobServer};

#[macro_use]
extern crate rocket;

#[get("/")]
fn hello() -> String {
    "Hello, world!".to_string()
}

#[launch]
fn rocket() -> _ {
    let ms = fnf_rs::storage::MemoryStorage::new();
    let bjs = BackgroundJobServer::start(ms);
    let bjs = Arc::new(Mutex::new(bjs));

    rocket::build().mount("/", routes![hello]).manage(vec![bjs])
}
