#![doc = include_str!("../README.md")]
use crate::core::BgJobHandler;

use lapin::Channel;
use persist::Persist;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use std::{
    fmt::Display,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

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
mod amqp;
mod metrics;
mod models;
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
    _amqp_address: String,
    channel: Channel,
    routing_key: String,
    storage: Persist,
    //_connection: Connection,
}

pub fn generate_id() -> String {
    rusty_ulid::generate_ulid_string()
}
