use crate::{
    core::BgJobHandler,
    encoder,
    models::{Job, RequeuedStage, Stage},
    BackgroundJobServer, BackgroundJobServerPublisher, JobId,
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
        let num_bg_workers = 5;

        // workers to process jobs (distributed)
        for id in 1..num_bg_workers {
            let amqp_address = publisher._amqp_address.clone();
            let routing_key = publisher.routing_key.clone();
            let handler = handler.clone();

            workers.push(std::thread::spawn(move || {
                start_distributed_job_worker(handler, id, &amqp_address, &routing_key)
            }));
        }

        // workers to poll jobs
        let handler_for_poller = handler.clone();
        workers.push(std::thread::spawn(move || {
            start_poller_reqd_jobs(handler_for_poller)
        }));

        let handler_for_poller = handler.clone();
        workers.push(std::thread::spawn(move || {
            start_poller_delayed_jobs(handler_for_poller)
        }));

        // allow some time for the workers to start up
        std::thread::sleep(Duration::from_millis(250));

        Ok(Self {
            ctx: PhantomData,
            handler: handler,
            _workers: workers,
        })
    }

    // `enqueue`, `enqueue_continue` etc. available as
    // self impl Deref to BackgroundJobServer
}

impl<C, H> std::ops::Deref for BackgroundJobServer<C, H>
where
    C: Sync + Send + 'static,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    type Target = BackgroundJobServerPublisher;

    fn deref(&self) -> &Self::Target {
        self.handler.get_publisher()
    }
}

fn start_poller_reqd_jobs<C, H>(handler: Arc<H>) -> anyhow::Result<()>
where
    C: Sync + Send,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    loop {
        tracing::debug!("Polling reqd jobs");

        let publisher = handler.get_publisher();
        let mut iter = rt.block_on(publisher.storage.get_reqd_jobs())?;
        while let Some(job_id_bytes) = rt.block_on(iter.next()) {
            let job_id = encoder::decode::<JobId>(&job_id_bytes)?;

            if let Some(job) = rt.block_on(publisher.storage.get_job(job_id)) {
                if let Stage::Requeued(RequeuedStage {
                    date: _,
                    requeue_count,
                }) = job.stage
                {
                    tracing::debug!("Job {}: Requeue #{}", job.id, requeue_count);

                    let enqueued = job.transition();
                    if let Err(_) = rt.block_on(publisher.save(&enqueued)) {
                        continue;
                    }

                    if let Err(_) = rt.block_on(publisher.handle_job_enqueue_initial(enqueued)) {
                        continue;
                    }
                }
            }
        }

        rt.block_on(publisher.storage.trim(iter))?;

        std::thread::sleep(Duration::from_secs(1));
    }
}

fn start_poller_delayed_jobs<C, H>(handler: Arc<H>) -> anyhow::Result<()>
where
    C: Sync + Send,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    loop {
        tracing::debug!("Polling delayed jobs");

        let publisher = handler.get_publisher();
        let mut iter = rt.block_on(publisher.storage.get_delayed_jobs())?;

        while let Some(bytes) = rt.block_on(iter.next()) {
            let job_id = encoder::decode::<JobId>(&bytes)?;

            if let Some(job) = rt.block_on(publisher.storage.get_job(job_id.clone())) {
                if let Stage::Delayed(delay) = &job.stage.clone() {
                    let mut requeue = false;

                    if delay.is_time() {
                        tracing::debug!("Job {}: Waiting is finished", job.id);

                        if let Err(_) = rt.block_on(publisher.handle_job_enqueue_initial(job)) {
                            requeue = true;
                        }
                    } else {
                        requeue = true;
                    }

                    if requeue {
                        rt.block_on(
                            publisher
                                .storage
                                .save_job_id(&job_id, &Stage::Delayed(delay.clone())),
                        )?;
                    }
                }
            }
        }

        rt.block_on(publisher.storage.trim(iter))?;
        std::thread::sleep(Duration::from_secs(2)); // ToDo: configure
    }
}

fn start_distributed_job_worker<C, H>(
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
            auto_delete: true,
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
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let id = job.id.clone();
    let ptype = job.payload_type.clone();
    let payload = job.payload.clone();
    println!(
        "[Worker#{}] ({:>3}) Message received [Id: {}]",
        worker_id, i, id
    );
    let publisher = handler.get_publisher();
    let running_job = job.transition();
    rt.block_on(publisher.save(&running_job))?;

    match handler.dispatch(ptype, &payload) {
        Ok(_) => {
            // success
            let success_job = running_job.transition_success()?;
            let success_job_id = success_job.id.clone();
            rt.block_on(publisher.save(&success_job))?;

            rt.block_on(publisher.expire(&success_job, Duration::from_secs(3600)))?;

            // enqueue waiting jobs
            if let Some(waiting_jobs) = rt.block_on(
                handler
                    .get_publisher()
                    .storage
                    .get_continuation_job(&success_job),
            ) {
                for next in waiting_jobs {
                    println!("Continuing {} -> {}", success_job_id, next.id);

                    let next_job = next.transition(); // Waiting -> Enqueued
                    rt.block_on(publisher.save(&next_job))?;

                    rt.block_on(publisher.handle_job_enqueue_initial(next_job))?;
                }

                rt.block_on(
                    handler
                        .get_publisher()
                        .storage
                        .del_get_continuation_job(&success_job),
                )?;
            }
        }
        Err(e) => {
            println!("Failed job {}: {}", running_job.id, e);

            // failed, requeue
            let reqd_job = running_job.transition_req()?;
            rt.block_on(handler.get_publisher().save(&reqd_job))?;
            // requeued jobs get polled later ...
        }
    }
    consumer.ack(delivery)?;
    Ok(())
}
