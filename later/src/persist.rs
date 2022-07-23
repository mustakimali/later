use std::sync::RwLock;

use crate::{
    encoder::{self, encode},
    id::IdOf,
    models::{Job, Stage, StageName, WaitingStage},
    storage::Storage,
    JobId,
};

pub(crate) struct Persist {
    inner: RwLock<Box<dyn Storage>>,
}

impl Persist {
    pub fn new(storage: Box<dyn Storage>) -> Self {
        Self {
            inner: RwLock::new(storage),
        }
    }

    pub fn get(&self, id: JobId) -> Option<Job> {
        let id = IdOf::SavedJob(id).get_id();
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))
            .ok()
            .and_then(|mut storage| storage.get(&id.to_string()))
            .and_then(|bytes| encoder::decode::<Job>(&bytes).ok())
    }

    pub fn save_jobs(&self, id: JobId, job: &Job) -> anyhow::Result<()> {
        let id = IdOf::SavedJob(id).get_id();
        let bytes = encoder::encode(job)?;
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .set(&id.to_string(), &bytes)
    }

    pub fn save_job_id(&self, id: &JobId, stage: &Stage) -> anyhow::Result<()> {
        let key = format!("stage-{}-jobs", stage.get_name());
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .push(&key, id.to_string().as_bytes())
    }

    pub fn get_waiting_jobs(&self) -> anyhow::Result<Box<dyn crate::storage::StorageIter>> {
        // let key = format!("stage-{}-jobs", WaitingStage::get_name());
        // let iter = self
        //     .inner
        //     .write()
        //     .map_err(|e| anyhow::anyhow!("{}", e))?
        //     .scan_range(&key);

        // Ok(iter)
        todo!()
    }
}
