use crate::{
    core::{BgJobHandler, JobParameter},
    encoder,
    models::Job,
    BackgroundJobServer, JobId,
};
use amiquip::{Connection, ConsumerOptions, QueueDeclareOptions};
use std::{marker::PhantomData, sync::Arc, time::Duration};

impl<C, H> BackgroundJobServer<C, H>
where
    C: Sync + Send + 'static,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    pub fn start(handler: H) -> anyhow::Result<Self> {
        let mut workers = Vec::new();
        let handler = Arc::new(handler);
        let publisher = handler.get_publisher();

        // workers to process jobs (distributed)
        for id in 1..5 {
            let amqp_address = publisher._amqp_address.clone();
            let routing_key = publisher.routing_key.clone();
            let handler = handler.clone();

            workers.push(std::thread::spawn(move || {
                start_worker(handler, id, &amqp_address, &routing_key)
            }));
        }

        // allow some time for the workers to start up
        std::thread::sleep(Duration::from_millis(250));

        // workers to poll jobs

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
    H: BgJobHandler<C> + Sync + Send + 'static,
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
                    Ok(job) => handle_job(job, worker_id, i, handler.clone(), &consumer, delivery)?,
                    Err(err) => {
                        println!(
                            "[Worker#{}] ({:>3}) Unknown message received [{} bytes]: {}",
                            worker_id,
                            i,
                            delivery.body.len(),
                            err
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

fn handle_job<C, H>(
    job: Job,
    worker_id: i32,
    i: usize,
    handler: Arc<H>,
    consumer: &amiquip::Consumer,
    delivery: amiquip::Delivery,
) -> Result<(), anyhow::Error>
where
    C: Sync + Send,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    let id = job.id.clone();
    let ptype = job.payload_type.clone();
    let payload = job.payload.clone();
    println!(
        "[Worker#{}] ({:>3}) Message received [Id: {}]",
        worker_id, i, id
    );
    let publisher = handler.get_publisher();
    let running_job = job.transition();
    publisher.save(&running_job)?;

    match handler.dispatch(ptype, &payload) {
        Ok(_) => {
            // success
            let success_job = running_job.transition_success()?;
            publisher.save(&success_job)?;

            publisher.expire(&success_job, Duration::from_secs(3600))?;

            // enqueue waiting jobs
            if let Some(next_job) = handler
                .get_publisher()
                .storage
                .get_continuation_job(success_job)
            {
                let next_job = next_job.transition(); // Waiting -> Enqueued
                publisher.save(&next_job)?;

                publisher.handle_job_enqueue_initial(next_job)?;
            }
        }
        Err(e) => {
            println!("Failed job {}: {}", running_job.id, e);

            // failed, requeue
            let reqd_job = running_job.transition_req()?;
            handler.get_publisher().save(&reqd_job)?;
            // requeued jobs get polled later ...
        }
    }
    consumer.ack(delivery)?;
    Ok(())
}
