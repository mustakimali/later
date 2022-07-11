use crate::models::EnqueuedJob;
use amiquip::{Channel, Connection, ConsumerOptions, Exchange, Publish, QueueDeclareOptions};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, marker::PhantomData, sync::Arc, thread::JoinHandle};

pub use anyhow;
pub use fnf_core::{BgJobHandler, JobParameter};
pub use fnf_derive::background_job;
pub use serde_json;

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
    // channel: Channel,
    // _connection: Connection,
    // routing_key: String,
    publisher: BackgroundJobServerPublisher,
    _workers: Vec<JoinHandle<anyhow::Result<()>>>,
}

pub struct BackgroundJobServerPublisher {
    _amqp_address: String,
    channel: Channel,
    routing_key: String,
    _connection: Connection,
}

impl BackgroundJobServerPublisher {
    pub fn new(id: String, amqp_address: String) -> anyhow::Result<Self> {
        let mut connection = Connection::insecure_open(&amqp_address)?;
        let channel = connection.open_channel(None)?;
        let routing_key = format!("fnf-rs-{}", id);

        Ok(Self {
            _amqp_address: amqp_address,
            _connection: connection,

            channel: channel,
            routing_key: routing_key,
        })
    }

    pub fn enqueue(&self, message: impl JobParameter) -> anyhow::Result<JobId> {
        let id = uuid::Uuid::new_v4().to_string();

        // self.storage.set(format!("job-{}", id), job);
        // self.storage.push_job_id(id);

        let message = models::EnqueuedJob {
            id: JobId(id.clone()),
            payload_type: message.get_ptype(),
            payload: message
                .to_bytes()
                .context("Unable to serialize the message to bytes")?,
            parent_id: None,
        };
        let message_bytes = serde_json::to_vec(&message)?;
        let exchange = Exchange::direct(&self.channel);

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
            let context = handler.get_ctx().clone();
            let handler = handler.clone();

            workers.push(std::thread::spawn(move || {
                start_worker(context, handler, id, &amqp_address, &routing_key)
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
    ctx: C,
    handler: Arc<H>,
    worker_id: i32,
    amqp_address: &str,
    routing_key: &str,
) -> anyhow::Result<()>
where
    C: Sync + Send + Clone,
    H: BgJobHandler<C> + Sync + Send,
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
