use crate::{JobId, UtcDateTime};

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct EnqueuedJob {
    pub id: JobId,
    pub parent_id: Option<JobId>,
    pub not_before: Option<UtcDateTime>,
    pub payload_type: String,
    pub payload: Vec<u8>,
}
