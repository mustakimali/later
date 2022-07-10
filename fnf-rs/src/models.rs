use crate::JobId;

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct EnqueuedJob {
    pub id: JobId,
    pub parent_id: Option<JobId>,
    pub payload_type: String,
    pub payload: Vec<u8>,
}
