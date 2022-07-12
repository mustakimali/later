#![doc = include_str!("../README.md")]
use crate::core::{BgJobHandler, JobParameter};
use crate::models::EnqueuedJob;
use amiquip::{Channel, Connection, ConsumerOptions, Exchange, Publish, QueueDeclareOptions};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    marker::PhantomData,
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

pub use anyhow;
pub use later_derive::background_job;
pub use serde_json;

pub mod core;
mod models;
pub mod storage;

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
    handler: PhantomData<H>,
    publisher: BackgroundJobServerPublisher,
    _workers: Vec<JoinHandle<anyhow::Result<()>>>,
}

pub struct BackgroundJobServerPublisher {
    _amqp_address: String,
    channel: Mutex<Channel>,
    routing_key: String,
    _connection: Connection,
}

impl BackgroundJobServerPublisher {
    pub fn new(id: String, amqp_address: String) -> anyhow::Result<Self> {
        let mut connection = Connection::insecure_open(&amqp_address)?;
        let channel = connection.open_channel(None)?;
        let routing_key = format!("later-{}", id);

        Ok(Self {
            _amqp_address: amqp_address,
            _connection: connection,

            channel: Mutex::new(channel),
            routing_key: routing_key,
        })
    }

    pub fn enqueue_continue(&self, parent_job_id: JobId, message: impl JobParameter) -> anyhow::Result<JobId> {
        self.enqueue_internal(Some(parent_job_id), message)
    }

    pub fn enqueue(&self, message: impl JobParameter) -> anyhow::Result<JobId> {
        self.enqueue_internal(None, message)
    }

    fn enqueue_internal(&self, parent_job_id: Option<JobId>, message: impl JobParameter) -> anyhow::Result<JobId> {
        let id = uuid::Uuid::new_v4().to_string();

        // self.storage.set(format!("job-{}", id), job);
        // self.storage.push_job_id(id);

        let message = models::EnqueuedJob {
            id: JobId(id.clone()),
            payload_type: message.get_ptype(),
            payload: message
                .to_bytes()
                .context("Unable to serialize the message to bytes")?,
            parent_id: parent_job_id.clone(),
        };
        let message_bytes = serde_json::to_vec(&message)?;

        // save the job

        if let Some(_parent_job_id) = parent_job_id {
            // continuation
            // - enqueue if parent is already complete
            // - schedule self message to check an enqueue later (to prevent race)

        }


        let channel = self.channel.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
        let exchange = Exchange::direct(&channel);

        exchange.publish(Publish::new(&message_bytes, self.routing_key.clone()))?;

        Ok(JobId(id))
    }
}

impl<C, H> BackgroundJobServer<C, H>
where
    C: Sync + Send + Clone + 'static,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    pub fn start(handler: H, publisher: BackgroundJobServerPublisher) -> anyhow::Result<Self> {
        let mut workers = Vec::new();
        let handler = Arc::new(handler);
        for id in 1..5 {
            let amqp_address = publisher._amqp_address.clone();
            let routing_key = publisher.routing_key.clone();
            let handler = handler.clone();

            workers.push(std::thread::spawn(move || {
                start_worker(handler, id, &amqp_address, &routing_key)
            }));
        }

        Ok(Self {
            ctx: PhantomData,
            handler: PhantomData,
            publisher: publisher,
            _workers: workers,
        })
    }

    pub fn enqueue(&self, message: impl JobParameter) -> anyhow::Result<JobId> {
        self.publisher.enqueue(message)
    }
}

fn start_worker<C, H>(
    handler: Arc<H>,
    worker_id: i32,
    amqp_address: &str,
    routing_key: &str,
) -> anyhow::Result<()>
where
    C: Sync + Send + Clone,
    H: core::BgJobHandler<C> + Sync + Send,
{
    println!("[Worker#{}] Starting", worker_id);
    let mut connection = Connection::insecure_open(&amqp_address)?;
    let channel = connection.open_channel(None)?;
    let queue = channel.queue_declare(
        routing_key,
        QueueDeclareOptions {
            durable: true,
            auto_delete: false,
            ..Default::default()
        },
    )?;
    let consumer = queue.consume(ConsumerOptions {
        no_ack: false,
        ..Default::default()
    })?;

    for (i, message) in consumer.receiver().iter().enumerate() {
        match message {
            amiquip::ConsumerMessage::Delivery(delivery) => {
                match serde_json::from_slice::<EnqueuedJob>(&delivery.body) {
                    Ok(message) => {
                        // send to app and ack if successful
                        let id = message.id.clone();
                        let ptype = message.payload_type;
                        let payload = message.payload;

                        println!(
                            "[Worker#{}] ({:>3}) Message received [Id: {}]",
                            worker_id, i, id
                        );

                        if let Ok(_) = handler.dispatch(ptype, &payload) {
                            consumer.ack(delivery)?;
                        }
                    }
                    Err(_err) => {
                        println!(
                            "[Worker#{}] ({:>3}) Unknown message received [{} bytes]",
                            worker_id,
                            i,
                            delivery.body.len()
                        );
                        consumer.nack(delivery, false)?;
                    }
                }
            }
            other => {
                println!("[Worker#{}] Consumer ended: {:?}", worker_id, other);
                break;
            }
        }
    }

    println!("[Worker#{}] Ended", worker_id);
    Ok(())
}
