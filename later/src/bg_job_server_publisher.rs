use crate::core::JobParameter;
use crate::models::{AmqpCommand, Job, RecurringJob};
use crate::models::{DelayedStage, EnqueuedStage, JobConfig, Stage, WaitingStage};
use crate::mq::MqClient;
use crate::persist::Persist;
use crate::storage::Storage;
use crate::{encoder, RecurringJobId};
use crate::{metrics, BackgroundJobServerPublisher, JobId, UtcDateTime};
use anyhow::Context;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

impl BackgroundJobServerPublisher {
    pub async fn new(
        id: String,
        mq_client: Arc<Box<dyn MqClient>>,
        storage: Box<dyn Storage>,
    ) -> anyhow::Result<Self> {
        let routing_key = format!("later-{}", id);
        let publisher = mq_client.new_publisher(&routing_key).await?;

        Ok(Self {
            storage: Persist::new(storage, routing_key.clone()),

            publisher: publisher,
            routing_key: routing_key,
        })
    }

    /// Blocks until there is at least worker available.
    /// This is used during startup to ensure readiness.
    pub async fn ensure_worker_ready(&self) -> anyhow::Result<()> {
        Ok(self.publisher.ensure_consumer().await?)
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

    pub async fn enqueue(&self, message: impl JobParameter) -> anyhow::Result<JobId> {
        self.enqueue_internal(message, None, None, None).await
    }

    async fn enqueue_internal(
        &self,
        message: impl JobParameter,
        parent_job_id: Option<JobId>,
        delay_until: Option<UtcDateTime>,
        recurring_job_id: Option<RecurringJobId>,
    ) -> anyhow::Result<JobId> {
        let job = create_job(message, parent_job_id, delay_until, recurring_job_id)?;

        Ok(self.enqueue_internal_job(job).await?)
    }

    pub(crate) async fn enqueue_internal_job(&self, job: Job) -> Result<JobId, anyhow::Error> {
        let id = job.id.clone();

        self.save(&job).await?;
        self.handle_job_enqueue_initial(job).await?;
        Ok(id)
    }

    pub async fn enqueue_recurring(
        &self,
        identifier: String,
        message: impl JobParameter,
        cron: String,
    ) -> anyhow::Result<JobId> {
        // validate
        let _ = cron::Schedule::from_str(&cron).context("error parsing cron expression")?;

        let recurring_job = RecurringJob {
            id: RecurringJobId(identifier),
            payload_type: message.get_ptype(),
            payload: message
                .to_bytes()
                .context("Unable to serialize the message to bytes")?,
            cron_schedule: cron,
            date_added: chrono::Utc::now(),
            config: JobConfig::default(),
        };

        self.storage.save_recurring_job(&recurring_job).await?;

        let first_job = recurring_job.try_into()?;
        let id = self.enqueue_internal_job(first_job).await?;

        Ok(id)
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
        self.storage.save_job(job).await
    }

    pub(crate) async fn expire(&self, job: &Job, _duration: Duration) -> anyhow::Result<()> {
        // ToDo: expire properly
        self.storage.expire(job.id.clone()).await
    }

    #[async_recursion::async_recursion]
    pub(crate) async fn handle_job_enqueue_initial(&self, job: Job) -> anyhow::Result<()> {
        tracing::debug!(
            "handle_job_enqueue_initial: Id: {}, Stage: {:?}",
            &job.id,
            &job.stage
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

                    tracing::info!(
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
                tracing::debug!("Enqueue job {}", job.id);

                self.publish_amqp_command(AmqpCommand::ExecuteJob(job.into()))
                    .await?
            }
            Stage::Running(_) | Stage::Requeued(_) | Stage::Success(_) | Stage::Failed(_) => {
                tracing::warn!("Invalid job here {}, Stage {:?}", job.id, &job.stage);
                //unreachable!("stage is handled in consumer")
            }
        }

        Ok(())
    }

    pub(crate) async fn publish_amqp_command(&self, cmd: AmqpCommand) -> anyhow::Result<()> {
        let message_bytes = encoder::encode(&cmd)?;

        self.publisher.publish(&message_bytes).await?;

        Ok(())
    }
}

fn create_job(
    message: impl JobParameter,
    parent_job_id: Option<JobId>,
    delay_until: Option<chrono::DateTime<chrono::Utc>>,
    recurring_job_id: Option<RecurringJobId>,
) -> anyhow::Result<Job> {
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
        recurring_job_id: recurring_job_id,
    };
    Ok(job)
}
