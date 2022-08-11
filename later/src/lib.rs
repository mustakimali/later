//! # later
//!
//! A distributed background job manager and runner for Rust. This is currently in PoC stage.
//!
//! ## Set up
//!
//! <details>
//!   <summary>Click to expand: One-time setup during application startup!</summary>
//!
//! ### 1. Import `later` and required dependencies
//!
//! ```toml
//! later = { version = "0.0.6", features = ["redis"] }
//! serde = "1.0"
//! ```
//!
//! ### 2. Define some types to use as a payload to the background jobs
//!
//! ```
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)] // <- Required derives
//! pub struct SendEmail {
//!     pub address: String,
//!     pub body: String,
//! }
//!
//! // ... more as required
//! ```
//!
//! ### 3. Generate the stub
//!
//! ```
//! # use serde::{Deserialize, Serialize};
//! #
//! # #[derive(Serialize, Deserialize)] // <- Required derives
//! # pub struct SendEmail {
//! #     pub address: String,
//! #     pub body: String,
//! # }
//! later::background_job! {
//!     struct Jobs {
//!         send_email: SendEmail,
//!     }
//! }
//! ```
//!
//! This generates two types
//! * `JobsBuilder` - used to bootstrap the background job server - which can be used to enqueue jobs,
//! * `JobContext<T>` - used to pass application context (`T`) in the handler as well as enqueue jobs,
//!
//! ### 4. Use the generated code to bootstrap the background job server
//!
//! For `struct Jobs` a type `JobsBuilder` will be generated. Use this to bootstrap the server.
//!
//! ```no_run
//! # use serde::{Deserialize, Serialize};
//! #
//! # #[derive(Serialize, Deserialize)]
//! # pub struct SendEmail { pub address: String, pub body: String }
//! # pub struct MyContext {}
//! # later::background_job! {
//! #     struct Jobs {
//! #         send_email: SendEmail,
//! #     }
//! # }
//! use later::{storage::redis::Redis, BackgroundJobServer, mq::amqp, Config};
//!
//! # #[tokio::main]
//! # async fn main() {
//! // bootstrap the server
//! let ctx = MyContext{ /*..*/ };                  // Any context to pass onto the handlers
//! let storage = Redis::new("redis://127.0.0.1/")  // More storage option to be available later
//!     .await
//!     .expect("connect to redis");
//! let mq = amqp::RabbitMq::new("amqp://guest:guest@localhost:5672".into()); // RabbitMq instance
//! let ctx = JobsBuilder::new(
//!     later::Config::builder()
//!         .name("fnf-example".into())             // Unique name for this app
//!         .context(ctx)                       // Pass the context here
//!         .storage(Box::new(storage))             // Configure storage
//!         .message_queue_client(Box::new(mq))     // Configure mq
//!         // ...
//!         .build()
//!     )
//!     // for each payload defined in the `struct Jobs` above
//!     // the generated fn name uses the pattern "with_[name]_handler"
//!     .with_send_email_handler(handle_send_email)     // Pass the handler function
//!     // ..
//!     .build()
//!     .await
//!     .expect("start BG Jobs server");
//!
//! // use ctx.enqueue(SendEmail{ ... }) to enqueue jobs,
//! // or ctx.enqueue_continue(parent_job_id, SendEmail{ ... }) to chain jobs.
//! // this will only accept types defined inside the macro above
//! # }
//! // define handler
//! async fn handle_send_email(
//!         ctx: JobsContext<MyContext>, // JobContext is generated wrapper
//!         payload: SendEmail,
//!     ) -> anyhow::Result<()> {
//!     // handle `payload`
//!
//!     // ctx.app -> Access the MyContext passed during bootstrapping
//!     // ctx.enqueue(_).await to enqueue more jobs
//!     // ctx.enqueue_continue(_).await to chain jobs
//!
//!     Ok(()) // or Err(_) to retry this message
//! }
//! ```
//!
//! </details>
//!
//! ---
//!
//! ## Fire and forget jobs
//!
//! Fire and forget jobs are executed only once and executed by an available worker almost immediately.
//!
//! ```no_run
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # pub struct SendEmail { pub address: String, pub body: String }
//! # later::background_job! {
//! #     struct Jobs {
//! #         send_email: SendEmail,
//! #     }
//! # }
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()>{
//! # let ctx : later::BackgroundJobServerPublisher = todo!();
//! ctx.enqueue(SendEmail{
//!     address: "hello@rust-lang.org".to_string(),
//!     body: "You rock!".to_string()
//! }).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Continuations
//!
//! One or many jobs are chained together to create an workflow. Child jobs are executed **only when parent job has been finished**.
//!
//! ```no_run
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # pub struct SendEmail { pub address: String, pub body: String }
//! # later::background_job! {
//! #     struct Jobs {
//! #         send_email: SendEmail,
//! #         create_account: CreateAccount,
//! #     }
//! # }
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # pub struct CreateAccount { id: String }
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()>{
//! # let ctx : later::BackgroundJobServerPublisher = todo!();
//! let email_welcome = ctx.enqueue(SendEmail{
//!     address: "customer@example.com".to_string(),
//!     body: "Creating your account!".to_string()
//! }).await?;
//!
//! let create_account = ctx.enqueue_continue(email_welcome, CreateAccount { id: "accout-1".to_string() }).await?;
//!
//! let email_confirmation = ctx.enqueue_continue(create_account, SendEmail{
//!     address: "customer@example.com".to_string(),
//!     body: "Your account has been created!".to_string()
//! }).await;
//! # Ok(())
//! # }
//! ```
//!
//! ## Delayed jobs
//!
//! Just like fire and forget jobs that starts after a certain interval.
//!
//! ```no_run
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # pub struct SendEmail { pub address: String, pub body: String }
//! # later::background_job! {
//! #     struct Jobs {
//! #         send_email: SendEmail,
//! #     }
//! # }
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()>{
//! # let ctx : later::BackgroundJobServerPublisher = todo!();
//! // delay
//! ctx.enqueue_delayed(SendEmail{
//!     address: "hello@rust-lang.org".to_string(),
//!     body: "You rock!".to_string()
//! }, std::time::Duration::from_secs(60)).await?;
//!
//! // specific time
//! let run_job_at : chrono::DateTime<chrono::Utc> = todo!();
//! ctx.enqueue_delayed_at(SendEmail{
//!     address: "hello@rust-lang.org".to_string(),
//!     body: "You rock!".to_string()
//! }, run_job_at).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Recurring jobs
//!
//! _(Coming soon)_
use crate::core::BgJobHandler;

use mq::{MqClient, MqPublisher};
use persist::Persist;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, marker::PhantomData, sync::Arc};
use storage::Storage;
use tokio::task::JoinHandle;
use typed_builder::TypedBuilder;

pub use anyhow;
pub use async_trait;
pub use futures;
pub use later_derive::background_job;

mod bg_job_server;
mod bg_job_server_publisher;
mod commands;
pub mod core;
pub mod encoder;
mod id;
mod metrics;
mod models;
pub mod mq;
mod persist;
mod stats;
pub mod storage;

pub type UtcDateTime = chrono::DateTime<chrono::Utc>;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JobId(String);
impl Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RecurringJobId(String);
impl Display for RecurringJobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ToDo: Remove H - use Box<dyn BgJobHandler<C>>
pub struct BackgroundJobServer<C, H>
where
    H: BgJobHandler<C> + Sync + Send,
{
    ctx: PhantomData<C>,
    handler: Arc<H>,
    _workers: Vec<JoinHandle<anyhow::Result<()>>>,
}

pub struct BackgroundJobServerPublisher {
    publisher: Box<dyn MqPublisher>,
    routing_key: String,
    storage: Persist,
}

pub fn generate_id() -> String {
    rusty_ulid::generate_ulid_string()
}

#[derive(TypedBuilder)]
pub struct Config<C>
where
    C: Sync + Send + 'static,
{
    pub name: String,
    pub context: C,
    pub storage: Box<dyn Storage>,

    pub message_queue_client: Box<dyn MqClient>,

    #[builder(default = 6)]
    pub default_retry_count: u8,
}
