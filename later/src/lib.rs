#![doc = include_str!("../README.md")]
use crate::core::BgJobHandler;

use amiquip::{Channel, Connection};

use persist::Persist;
use serde::{Deserialize, Serialize};

use std::{
    fmt::Display,
    marker::PhantomData,
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

pub use anyhow;
pub use later_derive::background_job;

mod bg_job_server;
mod bg_job_server_publisher;
pub mod core;
pub mod encoder;
mod id;
mod models;
mod persist;
mod commands;
pub mod storage;
mod metrics;

pub(crate) type UtcDateTime = chrono::DateTime<chrono::Utc>;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JobId(String);
impl Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

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
    channel: Arc<Mutex<Channel>>,
    routing_key: String,
    storage: Persist,
    _connection: Connection,
}
