use std::sync::Arc;

use tracing::{info, instrument};

use crate::{
    encoder,
    id::Id,
    models::{Job, JobConfig, Stage},
    mq::{MqClient, MqConsumer, MqPayload, MqPublisher},
    persist::Persist,
    BackgroundJobServerPublisher, JobId, RecurringJobId,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct JobMeta {
    pub id: JobId,

    pub payload_type: String,

    pub config: JobConfig,
    pub stage: Stage,
    pub previous_stages: Vec<Stage>,
    pub recurring_job_id: Option<RecurringJobId>,
}

impl From<&Job> for JobMeta {
    fn from(j: &Job) -> Self {
        Self {
            id: j.id.clone(),
            payload_type: j.payload_type.clone(),
            config: j.config.clone(),
            stage: j.stage.clone(),
            previous_stages: j.previous_stages.clone(),
            recurring_job_id: j.recurring_job_id.clone(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) enum Event {
    NewJob(JobMeta),
    JobTransitioned(Stage, JobMeta),
}

enum IdOf {
    JobList,
    JobStage(String),
}

#[async_trait::async_trait]
pub(crate) trait EventsHandler: Sync + Send {
    async fn new_event(&self, event: Event); // Infallible
}

pub struct Stats {
    publisher: Box<dyn MqPublisher>,
}

impl Stats {
    pub(crate) async fn new(
        main_routing_key: &str,
        storage: Arc<Persist>,
        mq_client: &Box<dyn MqClient>,
    ) -> anyhow::Result<Self> {
        let routing_key = format!("{}-stats", main_routing_key);
        let publisher = mq_client.new_publisher(&routing_key).await?;
        let mut consumer = mq_client.new_consumer(&routing_key, 6).await?;
        publisher.ensure_consumer().await?;

        tokio::spawn(async move {
            let _ = handle_stat_events(&mut consumer, storage).await;
        });

        Ok(Self { publisher })
    }
}

async fn handle_stat_events(
    consumer: &mut Box<dyn MqConsumer>,
    storage: Arc<Persist>,
) -> anyhow::Result<()> {
    while let Some(delivery) = consumer.next().await {
        let _ = handle_event(delivery, &storage);
    }

    Ok(())
}

async fn handle_event(
    delivery: anyhow::Result<Box<dyn MqPayload>>,
    storage: &Arc<Persist>,
) -> anyhow::Result<()> {
    info!("Handling stat event");
    let payload = delivery?;
    let event = encoder::decode::<Event>(&payload.data())?;

    match event {
        Event::NewJob(job) => {
            storage.save(storage.get_id(IdOf::JobList), job.id).await?;
        }
        Event::JobTransitioned(old_stage, job) => {
            let job_id = job.id;
            let old_stage = old_stage.get_name();
            let old_stage_key = storage.get_id(IdOf::JobStage(old_stage.clone()));

            let new_stage = job.stage.get_name();
            let new_stage_key = storage.get_id(IdOf::JobStage(new_stage.clone()));

            // remove from old_stage_key hashset

            // add to new_stage_key hashset
            storage.save(new_stage_key, job_id).await?;
        }
    }
    let _ = payload.ack().await;

    Ok(())
}

impl Persist {
    fn get_id(&self, of: IdOf) -> Id {
        let id_str = match of {
            IdOf::JobList => "job-list".to_string(),
            IdOf::JobStage(s) => format!("job-in-{}", s),
        };
        let id_str = format!("stats-{}", id_str);

        self.new_id(&id_str)
    }
}

#[async_trait::async_trait]
impl EventsHandler for Stats {
    async fn new_event(&self, event: Event) {
        if let Ok(bytes) = encoder::encode(&event) {
            let _ = self.publisher.publish(&bytes).await;
        }
    }
}

pub struct NoOpStats;

#[async_trait::async_trait]
impl EventsHandler for NoOpStats {
    async fn new_event(&self, _: Event) {
        ()
    }
}

impl Persist {}

impl Event {
    #[instrument(skip(p), name = "publish_stat_event")]
    pub(crate) async fn publish(self, p: &BackgroundJobServerPublisher) {
        p.stats.new_event(self).await
    }
}
