use std::sync::Arc;

use tracing::instrument;

use crate::{
    models::{Job, JobConfig, Stage},
    mq::{MqClient, MqConsumer, MqPublisher},
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
        let mut consumer = mq_client.new_consumer(&routing_key, 1).await?;

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
    while let Some(_) = consumer.next().await {}

    Ok(())
}

#[async_trait::async_trait]
impl EventsHandler for Stats {
    async fn new_event(&self, event: Event) {}
}

pub struct NoOpStats;

#[async_trait::async_trait]
impl EventsHandler for NoOpStats {
    async fn new_event(&self, _: Event) {
        ()
    }
}

impl Persist {
    
}

impl Event {
    #[instrument(skip(p), name = "publish_stat_event")]
    pub(crate) async fn publish(self, p: &BackgroundJobServerPublisher) {
        p.stats.new_event(self).await
    }
}
