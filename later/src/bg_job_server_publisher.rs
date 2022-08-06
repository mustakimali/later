
use crate::core::JobParameter;
use crate::models::{AmqpCommand, Job};
use crate::models::{DelayedStage, EnqueuedStage, JobConfig, Stage, WaitingStage};
use crate::persist::Persist;
use crate::storage::Storage;
use crate::{amqp, metrics, BackgroundJobServerPublisher, JobId, UtcDateTime};
use anyhow::Context;
use std::str::FromStr;
use std::time::Duration;

impl BackgroundJobServerPublisher {
    pub async fn new(
        id: String,
        amqp_address: String,
        storage: Box<dyn Storage>,
    ) -> anyhow::Result<Self> {
        let routing_key = format!("later-{}", id);
        let channel = amqp::Client::new(&amqp_address, &routing_key)
            .new_publisher()
            .await?;

        Ok(Self {
            _amqp_address: amqp_address,
            //_connection: connection,
            storage: Persist::new(storage, routing_key.clone()),

            channel: channel,
            routing_key: routing_key,
        })
    }

    /// Blocks until there is at least worker available.
    /// This is used during startup to ensure readiness.
    pub async fn ensure_worker_ready(&self) -> anyhow::Result<()> {
        Ok(self.channel.ensure_consumer().await?)
    }

    pub fn get_metrics(&self) -> anyhow::Result<String> {
        metrics::COUNTER.output()
    }

    pub async fn enqueue_continue(
        &self,
        parent_job_id: JobId,
        message: impl JobParameter,
    ) -> anyhow::Result<JobId> {
        self.enqueue_internal(message, Some(parent_job_id), None, None)
            .await
    }

    pub async fn enqueue_delayed(
        &self,
        message: impl JobParameter,
        delay: std::time::Duration,
    ) -> anyhow::Result<JobId> {
        let enqueue_time = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::from_std(delay)?)
            .ok_or(anyhow::anyhow!("Error calculating enqueue time"))?;

        self.enqueue_delayed_at(message, enqueue_time).await
    }

    pub async fn enqueue_delayed_at(
        &self,
        message: impl JobParameter,
        time: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<JobId> {
        if time <= chrono::Utc::now() {
            return Err(anyhow::anyhow!("Time must be in the future"));
        }
        self.enqueue_internal(message, None, Some(time), None).await
    }

    pub async fn enqueue_recurring(
        &self,
        message: impl JobParameter,
        cron: String,
    ) -> anyhow::Result<JobId> {
        let cron_schedule =
            cron::Schedule::from_str(&cron).context("error parsing cron expression")?;
        self.enqueue_internal(message, None, None, Some(cron_schedule))
            .await
    }

    pub async fn enqueue(&self, message: impl JobParameter) -> anyhow::Result<JobId> {
        self.enqueue_internal(message, None, None, None).await
    }

    async fn enqueue_internal(
        &self,
        message: impl JobParameter,
        parent_job_id: Option<JobId>,
        delay_until: Option<UtcDateTime>,
        _cron_schedule: Option<cron::Schedule>,
    ) -> anyhow::Result<JobId> {
        let id = crate::generate_id();

        let job = Job {
            id: JobId(id.clone()),
            payload_type: message.get_ptype(),
            payload: message
                .to_bytes()
                .context("Unable to serialize the message to bytes")?,
            stage: {
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
                    })
                }
            },
            previous_stages: Vec::default(),
            config: JobConfig::default(),
        };

        // save the job
        self.save(&job).await?;

        self.handle_job_enqueue_initial(job).await?;

        Ok(JobId(id))
    }

    pub(crate) async fn save(&self, job: &Job) -> anyhow::Result<()> {
        if job.stage.is_polling_required() {
            self.storage.save_job_id(&job.id, &job.stage).await?;
        }
        if let Stage::Waiting(w) = &job.stage {
            self.storage
                .save_continuation(&job.id, w.parent_id.clone())
                .await?;
        }
        self.storage.save_jobs(job.id.clone(), job).await
    }

    pub(crate) async fn expire(&self, job: &Job, _duration: Duration) -> anyhow::Result<()> {
        // ToDo: expire properly
        self.storage.expire(job.id.clone()).await
    }

    #[async_recursion::async_recursion]
    pub(crate) async fn handle_job_enqueue_initial(&self, job: Job) -> anyhow::Result<()> {
        println!(
            "handle_job_enqueue_initial: Id: {}, Stage: {:?}",
            &job.id, &job.stage
        );

        match &job.stage {
            Stage::Delayed(delayed) => {
                // delayed job
                // should be polled

                if delayed.is_time() {
                    let job = job.transition(); // Delayed -> Enqueued
                    self.save(&job).await?;

                    self.handle_job_enqueue_initial(job).await?;
                }
            }
            Stage::Waiting(waiting) => {
                // continuation
                // - enqueue if parent is already complete
                // - schedule self message to check an enqueue later (to prevent race)

                if let Some(parent_job) = self.storage.get_job(waiting.parent_id.clone()).await {
                    if !parent_job.stage.is_success() {
                        return Ok(());
                    }

                    println!(
                        "Parent job {} is already completed, enqueuing this job immediately",
                        parent_job.id
                    );
                }

                // parent job is success or not found (means successful long time ago)
                let job = job.transition();
                self.save(&job).await?;

                self.handle_job_enqueue_initial(job).await?;
            }
            Stage::Enqueued(_) => {
                println!("Enqueue job {}", job.id);

                self.publish_amqp_command(AmqpCommand::ExecuteJob(job.into()))
                    .await?
            }
            Stage::Running(_) | Stage::Requeued(_) | Stage::Success(_) | Stage::Failed(_) => {
                println!("Invalid job here {}, Stage {:?}", job.id, &job.stage);
                //unreachable!("stage is handled in consumer")
            }
        }

        Ok(())
    }

    pub(crate) async fn publish_amqp_command(&self, cmd: AmqpCommand) -> anyhow::Result<()> {
        self.channel.publish(cmd).await?;

        Ok(())
    }
}
