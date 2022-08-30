use std::sync::Arc;

use tracing::{info, instrument};

use crate::{
    encoder,
    id::Id,
    models::{Job, JobConfig, Stage},
    mq::{MqClient, MqConsumer, MqPayload, MqPublisher},
    persist::Persist,
    storage::{Storage, StorageEx},
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
    SaveJob(JobMeta),
    ExpireJob(JobMeta),
}

enum IdOf {
    JobList,
    JobsInStage(String),
    JobMeta(JobId),
}

#[async_trait::async_trait]
pub(crate) trait EventsHandler: Sync + Send {
    async fn new_event(&self, event: Event); // Infallible
}

pub struct Stats {
    publisher: Box<dyn MqPublisher>,
    storage: Arc<Persist>,
}

impl Stats {
    pub(crate) async fn new(
        main_routing_key: &str,
        storage: Arc<Persist>,
        mq_client: &Box<dyn MqClient>,
    ) -> anyhow::Result<Self> {
        let routing_key = format!("{}-stats", main_routing_key);
        let storage2 = storage.clone();
        let publisher = mq_client.new_publisher(&routing_key).await?;
        let mut consumer = mq_client.new_consumer(&routing_key, 6).await?;
        publisher.ensure_consumer().await?;

        tokio::spawn(async move {
            let _ = handle_stat_events(&mut consumer, storage).await;
        });

        Ok(Self {
            publisher,
            storage: storage2,
        })
    }
}

async fn handle_stat_events(
    consumer: &mut Box<dyn MqConsumer>,
    storage: Arc<Persist>,
) -> anyhow::Result<()> {
    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(payload) => {
                let _ = handle_event(&payload, &storage).await;
                let _ = payload.ack().await;
            }
            Err(e) => tracing::warn!("Error in consumer: {}", e),
        }
    }

    tracing::info!("Stat handler exit ... ");
    Ok(())
}

async fn handle_event(payload: &Box<dyn MqPayload>, storage: &Arc<Persist>) -> anyhow::Result<()> {
    let event = encoder::decode::<Event>(&payload.data())?;

    match event {
        Event::SaveJob(job) => {
            let meta_id = storage.get_id(IdOf::JobMeta(job.id.clone()));
            let ex_job = storage.get_of_type::<JobMeta>(meta_id.clone()).await;
            let is_new_job = ex_job.is_none();

            if let Some(ex_job) = ex_job {
                // update
                if ex_job.stage != job.stage {
                    // save change is stage
                    handle_stage(storage, Some(ex_job.stage), &job).await?;
                }
            };

            // save job list and job meta
            if is_new_job {
                storage
                    .push(storage.get_id(IdOf::JobList), job.id.clone())
                    .await?;
                // save first stage
                handle_stage(storage, None, &job).await?;
            }
            storage.save(meta_id, job).await?;
        }
        Event::ExpireJob(job) => {
            let job_id = job.id.clone();
            tracing::info!("STAT: Delete job {}", job_id);
            let meta_id = storage.get_id(IdOf::JobMeta(job.id.clone()));
            // remove from all stages

            // todo: remove from job-list
            //storage.get_id(IdOf::JobList)

            storage.del_by_meta_id(meta_id).await?;
        }
    }

    Ok(())
}

async fn handle_stage(
    storage: &Arc<Persist>,
    old_stage: Option<Stage>,
    job: &JobMeta,
) -> anyhow::Result<()> {
    let job_id = job.id.clone();
    if let Some(old_stage) = old_stage {
        if old_stage != job.stage {
            // remove job from old stage
            let _ = remove_job_id_from_list(&job_id, &old_stage, &storage).await;
        }
    }

    let new_stage = job.stage.get_name();
    let new_stage_key = storage.get_id(IdOf::JobsInStage(new_stage.clone()));

    // add to new_stage_key hashset
    storage.push(new_stage_key, job_id).await?;

    Ok(())
}

async fn remove_job_id_from_list(
    id: &JobId,
    stage: &Stage,
    persist: &Persist,
) -> anyhow::Result<()> {
    let old_stage = stage.get_name();
    let old_stage_key = persist.get_id(IdOf::JobsInStage(old_stage.clone()));

    // remove from old_stage_key hashset
    let id_encoded = encoder::encode(id.clone())?;
    if let Some(mut range) = persist
        .inner
        .scan_range_from(&old_stage_key.to_string(), &id_encoded)
        .await
    {
        range.del(&persist.inner).await;
    }

    Ok(())
}

impl Persist {
    fn get_id(&self, of: IdOf) -> Id {
        let id_str = match of {
            IdOf::JobList => "job-list".to_string(),
            IdOf::JobsInStage(s) => format!("job-in-{}", s),
            IdOf::JobMeta(id) => format!("job-{}", id),
        };
        let id_str = format!("stats-{}", id_str);

        self.new_id(&id_str)
    }

    async fn del_by_meta_id(&self, id: Id) -> anyhow::Result<()> {
        Ok(self.inner.del(&id.to_string()).await?)
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

mod http {
    const PAGE_SIZE: usize = 25;

    use serde::de::DeserializeOwned;

    use super::*;
    use std::collections::HashMap;

    #[macro_use]
    macro_rules! hashmap {
        ($(($k:literal, $v: literal)),+) => {
            {
                let mut hm = HashMap::new();
                $(hm.insert($k.to_string(), $v.to_string());),+
                hm
            }
        };
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct Request {
        cmd: Command,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Command {
        Index,
        AllJobs { page: usize },
        JobsInStage(String),
    }

    #[derive(serde::Serialize)]
    pub struct Response {
        status_code: u8,
        headers: HashMap<String, String>,
        body: String,
    }

    #[derive(thiserror::Error, Debug)]
    pub enum ResponseError {
        #[error("Internal server error {0:?}")]
        InternalServer(#[from] anyhow::Error),

        #[error("Bad request")]
        ParseError(#[from] serde_querystring::Error),
    }

    impl Stats {
        pub async fn handle_http(&self, query_string: String) -> Result<Response, ResponseError> {
            match serde_querystring::from_str::<Command>(&query_string)? {
                Command::Index => todo!(),
                Command::AllJobs { page } => {
                    let key = self.storage.get_id(IdOf::JobList);
                    let items = scan_range::<JobId>(&self.storage.inner, key, page).await?;

                    Ok(Response::json(items)?)
                }
                Command::JobsInStage(_) => todo!(),
            }
        }
    }

    async fn scan_range<T: DeserializeOwned>(
        storage: &Box<dyn Storage>,
        key: Id,
        page: usize,
    ) -> anyhow::Result<Vec<T>> {
        let mut range = storage.scan_range(&key.to_string()).await;
        range.skip(PAGE_SIZE * (page - 1));

        let mut result = Vec::new();
        for _ in 0..PAGE_SIZE {
            if let Some(item) = range.next(storage).await {
                result.push(encoder::decode::<T>(&item)?);
            } else {
                break;
            }
        }

        Ok(result)
    }

    impl Response {
        fn json<T: serde::Serialize>(json: T) -> anyhow::Result<Self> {
            Ok(Self {
                status_code: 200,
                headers: hashmap!(("content-type", "application/json")),
                body: serde_json::to_string(&json)?,
            })
        }

        fn html(json: String) -> Self {
            Self {
                status_code: 200,
                headers: hashmap!(("content-type", "text/html")),
                body: json,
            }
        }
    }
}
