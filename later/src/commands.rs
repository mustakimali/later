use std::{sync::Arc, time::Duration};

use async_std::channel::Sender;

use crate::{
    core::BgJobHandler,
    encoder, metrics,
    models::{AmqpCommand, ChannelCommand, Job, RequeuedStage, Stage},
    JobId,
};

pub(crate) async fn handle_amqp_command<C, H>(
    command: AmqpCommand,
    worker_id: i32,
    handler: &Arc<H>,
    inproc_cmd_tx: &Sender<ChannelCommand>,
) -> Result<(), anyhow::Error>
where
    C: Sync + Send,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    let ty = command.get_type();
    metrics::COUNTER.commands_all.with_label_values(&[ty.as_str()]).inc();

    Ok(match command {
        AmqpCommand::PollDelayedJobs => {
            tracing::debug!("[Worker#{}] amqp_command: PollDelayedJobs", worker_id);

            let _ = handle_poll_delayed_job_command(handler.clone()).await;
            let _ = handler
                .get_publisher()
                .storage
                .config()
                .poll_delayed_jobs_last_run_set()
                .await;
            let _ = inproc_cmd_tx.send(ChannelCommand::PollDelayedJobs).await;
        }
        AmqpCommand::PollRequeuedJobs => {
            tracing::debug!("[Worker#{}] amqp_command: PollRequeuedJobs", worker_id);

            let _ = handle_poll_requeued_job_command(handler.clone()).await;
            let _ = handler
                .get_publisher()
                .storage
                .config()
                .poll_requeued_jobs_last_run_set()
                .await;
            let _ = inproc_cmd_tx.send(ChannelCommand::PollRequeuedJobs).await;
        }
        AmqpCommand::ExecuteJob(job) => {
            metrics::COUNTER.jobs_all.with_label_values(&[ty.as_str()]).inc();

            tracing::debug!("[Worker#{}] amqp_command: Job [Id: {}]", worker_id, job.id);

            handle_job(job, handler.clone()).await?;
        }
    })
}

async fn handle_poll_delayed_job_command<C, H: BgJobHandler<C>>(
    handler: Arc<H>,
) -> anyhow::Result<()> {
    tracing::debug!("Polling delayed jobs");

    let publisher = handler.get_publisher();
    let mut iter = publisher.storage.get_delayed_jobs().await?;

    while let Some(bytes) = iter.next().await {
        let job_id = encoder::decode::<JobId>(&bytes)?;

        if let Some(job) = publisher.storage.get_job(job_id.clone()).await {
            if let Stage::Delayed(delay) = &job.stage.clone() {
                let mut requeue = false;

                if delay.is_time() {
                    tracing::debug!("Job {}: Waiting is finished", job.id);

                    if let Err(_) = publisher.handle_job_enqueue_initial(job).await {
                        requeue = true;
                    }
                } else {
                    requeue = true;
                }

                if requeue {
                    publisher
                        .storage
                        .save_job_id(&job_id, &Stage::Delayed(delay.clone()))
                        .await?;
                }
            }
        }
    }

    publisher.storage.trim(iter).await?;

    Ok(())
}

async fn handle_poll_requeued_job_command<C, H: BgJobHandler<C>>(
    handler: Arc<H>,
) -> anyhow::Result<()> {
    tracing::debug!("Polling reqd jobs");

    let publisher = handler.get_publisher();
    let mut iter = publisher.storage.get_reqd_jobs().await?;
    while let Some(job_id_bytes) = iter.next().await {
        let job_id = encoder::decode::<JobId>(&job_id_bytes)?;

        if let Some(job) = publisher.storage.get_job(job_id).await {
            if let Stage::Requeued(RequeuedStage {
                date: _,
                requeue_count,
            }) = job.stage
            {
                tracing::debug!("Job {}: Requeue #{}", job.id, requeue_count);

                let enqueued = job.transition();
                if let Err(_) = publisher.save(&enqueued).await {
                    continue;
                }

                if let Err(_) = publisher.handle_job_enqueue_initial(enqueued).await {
                    continue;
                }
            }
        }
    }

    publisher.storage.trim(iter).await?;

    Ok(())
}

async fn handle_job<C, H>(job: Job, handler: Arc<H>) -> Result<(), anyhow::Error>
where
    C: Sync + Send,
    H: BgJobHandler<C> + Sync + Send + 'static,
{
    let ptype = job.payload_type.clone();
    let payload = job.payload.clone();

    let publisher = handler.get_publisher();
    let running_job = job.transition();
    publisher.save(&running_job).await?;

    match handler.dispatch(ptype, &payload).await {
        Ok(_) => {
            // success
            let success_job = running_job.transition_success()?;
            let success_job_id = success_job.id.clone();
            publisher.save(&success_job).await?;

            publisher
                .expire(&success_job, Duration::from_secs(3600))
                .await?;

            // enqueue waiting jobs
            if let Some(waiting_jobs) = handler
                .get_publisher()
                .storage
                .get_continuation_job(&success_job)
                .await
            {
                for next in waiting_jobs {
                    println!("Continuing {} -> {}", success_job_id, next.id);

                    let next_job = next.transition(); // Waiting -> Enqueued
                    publisher.save(&next_job).await?;

                    publisher.handle_job_enqueue_initial(next_job).await?;
                }

                handler
                    .get_publisher()
                    .storage
                    .del_get_continuation_job(&success_job)
                    .await?;
            }
        }
        Err(e) => {
            println!("Failed job {}: {}", running_job.id, e);

            // failed, requeue
            let reqd_job = running_job.transition_req()?;
            handler.get_publisher().save(&reqd_job).await?;
            // requeued jobs get polled later ...
        }
    }

    Ok(())
}
