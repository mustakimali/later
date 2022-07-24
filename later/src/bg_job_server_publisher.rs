use crate::core::JobParameter;
use crate::models::Job;
use crate::models::{DelayedStage, EnqueuedStage, JobConfig, Stage, WaitingStage};
use crate::persist::Persist;
use crate::storage::Storage;
use crate::{encoder, BackgroundJobServerPublisher, JobId, UtcDateTime};
use amiquip::{Connection, Exchange, Publish};
use anyhow::Context;
use std::sync::Mutex;
use std::time::Duration;

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
        self.save(&job)?;

        if let Stage::Delayed(_) = job.stage {
            // continuation
            // - enqueue if parent is already complete
            // - schedule self message to check an enqueue later (to prevent race)
        }

        self.handle_job_enqueue_initial(job)?;

        Ok(JobId(id))
    }

    pub(crate) fn save(&self, job: &Job) -> anyhow::Result<()> {
        if job.stage.is_polling_required() {
            self.storage.save_job_id(&job.id, &job.stage)?;
        }
        if let Stage::Waiting(w) = &job.stage {
            self.storage
                .save_continuation(&job.id, w.parent_id.clone())?;
        }
        self.storage.save_jobs(job.id.clone(), job)
    }

    pub(crate) fn expire(&self, job: &Job, _duration: Duration) -> anyhow::Result<()> {
        // ToDo: expire properly
        self.storage.expire(job.id.clone())
    }

    pub(crate) fn handle_job_enqueue_initial(&self, job: Job) -> anyhow::Result<()> {
        match &job.stage {
            Stage::Delayed(delayed) => {
                if chrono::Utc::now() > delayed.date {
                    let job = job.transition();
                    self.save(&job)?;

                    self.handle_job_enqueue_initial(job)?;
                }
            }
            Stage::Waiting(_) => {
                let job = job.transition();
                self.save(&job)?;

                self.handle_job_enqueue_initial(job)?;
            }
            Stage::Enqueued(_) => {
                println!("Enqueue job {}", job.id);

                let message_bytes = encoder::encode(job)?;
                let channel = self.channel.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
                let exchange = Exchange::direct(&channel);

                exchange.publish(Publish::new(&message_bytes, self.routing_key.clone()))?;
            }
            Stage::Running(_) | Stage::Requeued(_) | Stage::Success(_) | Stage::Failed(_) => {
                unreachable!("stage is handled in consumer")
            }
        }

        Ok(())
    }
}