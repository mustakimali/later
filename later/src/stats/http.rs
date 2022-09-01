use super::*;
use serde::de::DeserializeOwned;
use serde_json::json;
use std::collections::HashMap;

const PAGE_SIZE: usize = 25;


macro_rules! hashmap {
    ($(($k:literal, $v: literal)),+) => {
        {
            let mut hm = HashMap::new();
            $(hm.insert($k.to_string(), $v.to_string());)+
            hm
        }
    };
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(rename_all = "snake_case", tag = "cmd")]
pub enum DashboardCmd {
    Index,
    AllJobs { page: usize },
    JobsInStage { stage: String, page: usize },
    Count,
    Job { id: JobId },
}

#[derive(serde::Serialize, typed_builder::TypedBuilder, Debug)]
pub struct DashboardResponse {
    pub status_code: usize,
    #[builder(default=HashMap::default())]
    pub headers: HashMap<String, String>,
    pub body: String,
}

#[derive(thiserror::Error, Debug)]
pub enum ResponseError {
    #[error("Internal server error {0:?}")]
    InternalServer(#[from] anyhow::Error),

    #[error("Bad request: {0:?}")]
    ParseError(#[from] serde_json::Error),
}

pub(crate) async fn handle_http_raw(
    persist: Arc<Persist>,
    query_string: String,
) -> Result<DashboardResponse, ResponseError> {
    let cmd = serde_json::from_str::<DashboardCmd>(&query_string)?;
    handle_http(persist, cmd).await
}

async fn handle_http(
    persist: Arc<Persist>,
    cmd: DashboardCmd,
) -> Result<DashboardResponse, ResponseError> {
    let dash_resp = match cmd {
        DashboardCmd::Index => todo!(),
        DashboardCmd::AllJobs { page } => {
            let key = persist.get_id(IdOf::JobList);
            let (items, count) = scan_range::<JobId>(&persist.inner, key, page).await?;

            DashboardResponse::json(json!({
                "result": items,
                "paging": {
                    "page": page,
                    "total": (count as f64 / PAGE_SIZE as f64).ceil() as i64,
                    "items": count,
                }
            }))?
        }
        DashboardCmd::JobsInStage { stage, page } => {
            let key = persist.get_id(IdOf::JobsInStage(stage));
            let (items, count) = scan_range::<JobId>(&persist.inner, key, page).await?;

            DashboardResponse::json(json!({
                "result": items,
                "paging": {
                    "page": page,
                    "total": (count as f64 / PAGE_SIZE as f64).ceil() as i64,
                    "items": count,
                }
            }))?
        }
        DashboardCmd::Job { id } => {
            let meta_id = persist.get_id(IdOf::JobMeta(id));
            match persist
                .inner
                .get(&meta_id.to_string())
                .await
                .map(|b| encoder::decode::<JobMeta>(&b))
            {
                Some(Ok(job)) => DashboardResponse::json(job)?,
                _ => DashboardResponse::error(404, "Job not found"),
            }
        }
        DashboardCmd::Count => {
            let stages = Stage::get_all_stage_names();
            let mut jobs_in_stages = HashMap::new();

            for stage in stages {
                let stage2 = stage.clone();
                let key = persist.get_id(IdOf::JobsInStage(stage2));
                jobs_in_stages.insert(stage, scan_range_count(&persist.inner, key).await);
            }
            DashboardResponse::json(json!({ "stages": jobs_in_stages }))?
        }
    };

    Ok(dash_resp)
}

async fn scan_range<T: DeserializeOwned>(
    storage: &Box<dyn Storage>,
    key: Id,
    page: usize,
) -> anyhow::Result<(Vec<T>, usize)> {
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

    Ok((result, range.count().await))
}

async fn scan_range_count(storage: &Box<dyn Storage>, key: Id) -> usize {
    let range = storage.scan_range(&key.to_string()).await;

    range.count().await
}

impl DashboardResponse {
    pub fn json<T: serde::Serialize>(json: T) -> anyhow::Result<Self> {
        Ok(Self {
            status_code: 200,
            headers: hashmap!(
                ("content-type", "application/json"),
                ("access-control-allow-origin", "*")
            ),
            body: serde_json::to_string(&json)?,
        })
    }

    pub fn html(json: String) -> Self {
        Self {
            status_code: 200,
            headers: hashmap!(
                ("content-type", "text/html"),
                ("access-control-allow-origin", "*")
            ),
            body: json,
        }
    }

    pub fn error<S: Into<String>>(status_code: usize, error: S) -> Self {
        Self {
            status_code,
            headers: hashmap!(
                ("content-type", "application/json"),
                ("access-control-allow-origin", "*")
            ),
            body: json!({ "error": error.into() }).to_string(),
        }
    }
}
