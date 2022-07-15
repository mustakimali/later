#![doc = include_str!("../README.md")]
use crate::core::{BgJobHandler, JobParameter};
use crate::models::Job;
use amiquip::{Channel, Connection, ConsumerOptions, Exchange, Publish, QueueDeclareOptions};
use anyhow::Context;

use models::{DelayedStage, EnqueuedStage, Stage, WaitingStage};
use persist::Persist;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    marker::PhantomData,
    sync::{Arc, Mutex},
    thread::JoinHandle,
};
use storage::Storage;

pub use anyhow;
pub use later_derive::background_job;

pub mod core;
pub mod encoder;
mod id;
mod models;
mod persist;
pub mod storage;

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
    channel: Mutex<Channel>,
    routing_key: String,
    storage: Persist,
    _connection: Connection,
}

impl BackgroundJobServerPublisher {
    pub fn new(
        id: String,
        amqp_address: String,
        storage: Box<dyn Storage>,
    ) -> anyhow::Result<Self> {
        let mut connection = Connection::insecure_open(&amqp_address)?;
        let channel = connection.open_channel(None)?;
        let routing_key = format!("later-{}", id);

        Ok(Self {
            _amqp_address: amqp_address,
            _connection: connection,
            storage: Persist::new(storage),

            channel: Mutex::new(channel),
            routing_key: routing_key,
        })
    }

    pub fn enqueue_continue(
        &self,
        parent_job_id: JobId,
        message: impl JobParameter,
    ) -> anyhow::Result<JobId> {
        self.enqueue_internal(message, Some(parent_job_id), None)
    }

    pub fn enqueue_delayed(
        &self,
        message: impl JobParameter,
        delay: std::time::Duration,
    ) -> anyhow::Result<JobId> {
        let enqueue_time = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::from_std(delay)?)
            .ok_or(anyhow::anyhow!("Error calculating enqueue time"))?;

        self.enqueue_delayed_at(message, enqueue_time)
    }
    pub fn enqueue_delayed_at(
        &self,
        _message: impl JobParameter,
        time: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<JobId> {
        if time <= chrono::Utc::now() {
            return Err(anyhow::anyhow!("Time must be in the future"));
        }
        todo!()
    }

    pub fn enqueue(&self, message: impl JobParameter) -> anyhow::Result<JobId> {
        self.enqueue_internal(message, None, None)
    }

    fn enqueue_internal(
        &self,
        message: impl JobParameter,
        parent_job_id: Option<JobId>,
        delay_until: Option<UtcDateTime>,
    ) -> anyhow::Result<JobId> {
        let id = uuid::Uuid::new_v4().to_string();

        // self.storage.set(format!("job-{}", id), job);
        // self.storage.push_job_id(id);

        let job = models::Job {
            id: JobId(id.clone()),
            payload_type: message.get_ptype(),
            payload: message
                .to_bytes()
                .context("Unable to serialize the message to bytes")?,
            stages: {
                if let Some(parent_job_id) = parent_job_id {
                    Stage::Waiting(WaitingStage {
                        date: chrono::Utc::now(),
                        parent_id: parent_job_id,
                    })
                } else if let Some(delay_until) = delay_until {
                    Stage::Delayed(DelayedStage {
                        date: chrono::Utc::now(),
                        not_before: delay_until,
                    })
                } else {
                    Stage::Enqueued(EnqueuedStage {
                        date: chrono::Utc::now(),
                        previous_stages: Vec::default(),
                    })
                }
            },
        };

        // save the job
        self.save(&job)?;

        if let Stage::Delayed(_) = job.stages {
            // continuation
            // - enqueue if parent is already complete
            // - schedule self message to check an enqueue later (to prevent race)
        }

        self.handle_job(job)?;

        Ok(JobId(id))
    }

    fn save(&self, job: &Job) -> anyhow::Result<()> {
        self.storage.save_jobs(job.id.clone(), job)
    }

    fn handle_job(&self, job: Job) -> anyhow::Result<()> {
        match &job.stages {
            Stage::Delayed(delayed) => {
                if chrono::Utc::now() > delayed.date {
                    let job = job.transition();
                    self.save(&job)?;

                    self.handle_job(job)?;
                }
            }
            Stage::Waiting(_) => todo!(),
            Stage::Enqueued(_) => {
                let message_bytes = encoder::encode(&job)?;
                let channel = self.channel.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
                let exchange = Exchange::direct(&channel);

                exchange.publish(Publish::new(&message_bytes, self.routing_key.clone()))?;
            }
            Stage::Running(_) => todo!(),
            Stage::Requeued(_) => todo!(),
        }

        Ok(())
    }
}

impl<C, H> BackgroundJobServer<C, H>
where
    C: Sync + Send + 'static,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    pub fn start(handler: H) -> anyhow::Result<Self> {
        let mut workers = Vec::new();
        let handler = Arc::new(handler);
        let publisher = handler.get_publisher();
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
            handler: handler,
            _workers: workers,
        })
    }

    pub fn enqueue(&self, message: impl JobParameter) -> anyhow::Result<JobId> {
        self.handler.get_publisher().enqueue(message)
    }
}

fn start_worker<C, H>(
    handler: Arc<H>,
    worker_id: i32,
    amqp_address: &str,
    routing_key: &str,
) -> anyhow::Result<()>
where
    C: Sync + Send,
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
                match encoder::decode::<Job>(&delivery.body) {
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
