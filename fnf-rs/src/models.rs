#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct EnqueuedJob {
    pub id: String,
    pub payload: Vec<u8>,
}
