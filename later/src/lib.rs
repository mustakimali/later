#![doc = include_str!("../README.md")]
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

pub(crate) type UtcDateTime = chrono::DateTime<chrono::Utc>;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JobId(String);
impl Display for JobId {
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
