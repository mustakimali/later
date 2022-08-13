use crate::{
    commands,
    core::BgJobHandler,
    encoder,
    models::{AmqpCommand, ChannelCommand},
    mq::MqClient,
    BackgroundJobServer, BackgroundJobServerPublisher, UtcDateTime,
};
use async_std::channel::{Receiver, Sender};
use std::{marker::PhantomData, sync::Arc, time::Duration};

impl<C, H> BackgroundJobServer<C, H>
where
    C: Sync + Send + 'static,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    pub async fn start(handler: H, mq_client: Arc<Box<dyn MqClient>>) -> anyhow::Result<Self> {
        let mut workers = Vec::new();
        let handler = Arc::new(handler);
        let publisher = handler.get_publisher();
        let num_bg_workers = 5;
        let (tx, rx) = async_std::channel::unbounded::<ChannelCommand>();

        // workers to process jobs (distributed)
        for id in 1..num_bg_workers {
            let mq_client = mq_client.clone();
            let routing_key = publisher.routing_key.clone();
            let handler = handler.clone();
            let inproc_tx = tx.clone();

            workers.push(tokio::spawn(async move {
                start_distributed_job_worker(handler, id, mq_client, &routing_key, inproc_tx).await
            }));
        }

        let handler_for_ensure_ops = handler.clone();
        workers.push(tokio::spawn(async move {
            start_bg_worker_system_ops_ensure_ops_run_on_certain_interval(
                handler_for_ensure_ops,
                tx.clone(),
            )
            .await
        }));

        for _ in 1..2 {
            let rx_clone = rx.clone();
            let handler_for_check_ops = handler.clone();

            workers.push(tokio::spawn(async move {
                start_bg_worker_system_ops_inproc_cmd_to_amqp_cmd(handler_for_check_ops, rx_clone)
                    .await
            }));
        }

        Ok(Self {
            ctx: PhantomData,
            handler: handler,
            _workers: workers,
        })
    }

    // `enqueue`, `enqueue_continue` etc. available as
    // self impl Deref to BackgroundJobServer
}

pub(crate) async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
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
async fn start_bg_worker_system_ops_inproc_cmd_to_amqp_cmd<C, H>(
    handler: Arc<H>,
    rx: Receiver<ChannelCommand>,
) -> anyhow::Result<()>
where
    C: Sync + Send,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    loop {
        let channel_command = rx.recv().await.expect("receive command from channel");
        sleep_ms(2000).await; // publish system ops after 2s delay

        match channel_command {
            ChannelCommand::PollDelayedJobs => {
                handler
                    .get_publisher()
                    .publish_amqp_command(AmqpCommand::PollDelayedJobs)
                    .await?;
            }
            ChannelCommand::PollRequeuedJobs => {
                handler
                    .get_publisher()
                    .publish_amqp_command(AmqpCommand::PollRequeuedJobs)
                    .await?;
            }
        }
    }
}

/// Every 10 secs, ensure some system operations are run - otherwise enqueue them
async fn start_bg_worker_system_ops_ensure_ops_run_on_certain_interval<C, H>(
    handler: Arc<H>,
    tx: Sender<ChannelCommand>,
) -> anyhow::Result<()>
where
    C: Sync + Send,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    sleep_ms(3_000).await; // wait for existing commands to be processed

    loop {
        let config = handler.get_publisher().storage.config();

        enqueue_if(
            &tx,
            ChannelCommand::PollDelayedJobs,
            config.poll_delayed_jobs_last_run().await,
            10,
        )
        .await;

        enqueue_if(
            &tx,
            ChannelCommand::PollRequeuedJobs,
            config.poll_requeued_jobs_last_run().await,
            10,
        )
        .await;

        sleep_ms(10_000).await; // check again 10s later
    }

    // unreachable

    async fn enqueue_if(
        tx: &Sender<ChannelCommand>,
        cmd: ChannelCommand,
        last_run: UtcDateTime,
        if_not_sec: i64,
    ) {
        let last_run_since = chrono::Utc::now() - last_run;
        if last_run_since.num_seconds() > if_not_sec {
            tracing::warn!("Ops {} did not run for a while: Enqueuing", cmd);

            let _ = tx.send(cmd).await;
        }
    }
}

async fn start_distributed_job_worker<C, H>(
    handler: Arc<H>,
    worker_id: i32,
    mq_client: Arc<Box<dyn MqClient>>,
    routing_key: &str,
    inproc_cmd_tx: Sender<ChannelCommand>,
) -> anyhow::Result<()>
where
    C: Sync + Send,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    tracing::info!("[Worker#{}] Starting", worker_id);

    let mut consumer = mq_client.new_consumer(routing_key, worker_id).await?;

    while let Some(message) = consumer.next().await {
        match message {
            Ok(delivery) => match encoder::decode::<AmqpCommand>(&delivery.data()) {
                Ok(command) => {
                    let _ =
                        commands::handle_amqp_command(command, worker_id, &handler, &inproc_cmd_tx)
                            .await;

                    delivery.ack().await?;
                }
                Err(err) => {
                    tracing::warn!(
                        "[Worker#{}] Unknown message received [{} bytes]: {}",
                        worker_id,
                        delivery.data().len(),
                        err
                    );

                    delivery.nack_requeue().await?;
                }
            },
            Err(e) => {
                tracing::warn!("[Worker#{}] Consumer ended: {:?}", worker_id, e);
                //break;
            }
        }
    }

    tracing::info!("[Worker#{}] Ended", worker_id);
    Ok(())
}
