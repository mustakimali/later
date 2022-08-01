use crate::{
    commands,
    core::BgJobHandler,
    encoder,
    models::{AmqpCommand, ChannelCommand},
    BackgroundJobServer, BackgroundJobServerPublisher, UtcDateTime,
};
use amiquip::{Connection, ConsumerOptions, QueueDeclareOptions};
use async_std::channel::{Receiver, Sender};
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
        let (tx, rx) = async_std::channel::unbounded::<ChannelCommand>();

        // workers to process jobs (distributed)
        for id in 1..num_bg_workers {
            let amqp_address = publisher._amqp_address.clone();
            let routing_key = publisher.routing_key.clone();
            let handler = handler.clone();
            let inproc_tx = tx.clone();

            workers.push(std::thread::spawn(move || {
                start_distributed_job_worker(handler, id, &amqp_address, &routing_key, inproc_tx)
            }));
        }

        let handler_for_ensure_ops = handler.clone();
        workers.push(std::thread::spawn(move || {
            start_bg_worker_system_ops_ensure_ops_run_on_certain_interval(
                handler_for_ensure_ops,
                tx.clone(),
            )
        }));

        for _ in 1..2 {
            let rx_clone = rx.clone();
            let handler_for_check_ops = handler.clone();

            workers.push(std::thread::spawn(move || {
                start_bg_worker_system_ops_inproc_cmd_to_amqp_cmd(handler_for_check_ops, rx_clone)
            }));
        }

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

/// listens for commands on an in-process queue,
/// then waits a few moments and publish/enqueue an amqp
/// command to execute system operations (like polling for certain jobs etc.)
fn start_bg_worker_system_ops_inproc_cmd_to_amqp_cmd<C, H>(
    handler: Arc<H>,
    rx: Receiver<ChannelCommand>,
) -> anyhow::Result<()>
where
    C: Sync + Send,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    loop {
        let channel_command = rt
            .block_on(rx.recv())
            .expect("receive command from channel");
        std::thread::sleep(Duration::from_secs(2));

        match channel_command {
            ChannelCommand::PollDelayedJobs => {
                handler
                    .get_publisher()
                    .publish_amqp_command(AmqpCommand::PollDelayedJobs)?;
            }
            ChannelCommand::PollRequeuedJobs => {
                handler
                    .get_publisher()
                    .publish_amqp_command(AmqpCommand::PollRequeuedJobs)?;
            }
        }
    }
}

/// Every 10 secs, ensure some system operations are run - otherwise enqueue them
fn start_bg_worker_system_ops_ensure_ops_run_on_certain_interval<C, H>(
    handler: Arc<H>,
    tx: Sender<ChannelCommand>,
) -> anyhow::Result<()>
where
    C: Sync + Send,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    std::thread::sleep(Duration::from_secs(3)); // wait for existing commands to be processed

    loop {
        let config = handler.get_publisher().storage.config();

        enqueue_if(
            &tx,
            ChannelCommand::PollDelayedJobs,
            rt.block_on(config.poll_delayed_jobs_last_run()),
            10,
        );

        enqueue_if(
            &tx,
            ChannelCommand::PollRequeuedJobs,
            rt.block_on(config.poll_requeued_jobs_last_run()),
            10,
        );

        std::thread::sleep(Duration::from_secs(10));
    }

    // unreachable

    fn enqueue_if(
        tx: &Sender<ChannelCommand>,
        cmd: ChannelCommand,
        last_run: UtcDateTime,
        if_not_sec: i64,
    ) {
        let last_run_since = chrono::Utc::now() - last_run;
        if last_run_since.num_seconds() > if_not_sec {
            println!("Ops {} did not run for a while: Enqueuing", cmd);

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let _ = rt.block_on(tx.send(cmd));
        }
    }
}

fn start_distributed_job_worker<C, H>(
    handler: Arc<H>,
    worker_id: i32,
    amqp_address: &str,
    routing_key: &str,
    inproc_cmd_tx: Sender<ChannelCommand>,
) -> anyhow::Result<()>
where
    C: Sync + Send,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    println!("[Worker#{}] Starting", worker_id);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

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
                match encoder::decode::<AmqpCommand>(&delivery.body) {
                    Ok(command) => {
                        let _ = rt.block_on(commands::handle_amqp_command(
                            command,
                            worker_id,
                            &handler,
                            &inproc_cmd_tx,
                        ));
                        consumer.ack(delivery)?;
                    }
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
