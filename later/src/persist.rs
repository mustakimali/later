use std::sync::RwLock;

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    encoder::{self},
    id::{Id, IdOf},
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

    pub fn get_job(&self, id: JobId) -> Option<Job> {
        let id = IdOf::SavedJob(id).get_id();
        self.get_of_type::<Job>(id)
    }

    pub fn get_of_type<T>(&self, id: Id) -> Option<T>
    where
        T: DeserializeOwned,
    {
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))
            .ok()
            .and_then(|storage| storage.get(&id.to_string()))
            .and_then(|bytes| encoder::decode::<T>(&bytes).ok())
    }

    pub fn save_jobs(&self, id: JobId, job: &Job) -> anyhow::Result<()> {
        let id = IdOf::SavedJob(id).get_id();
        self.save(id, job)
    }

    pub fn save<T>(&self, id: Id, item: T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let bytes = encoder::encode(item)?;
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .set(&id.to_string(), &bytes)
    }

    pub fn save_continuation(&self, job_id: &JobId, parent_job_id: JobId) -> anyhow::Result<()> {
        let id = IdOf::ContinuationOf(parent_job_id).get_id();
        self.save(id, job_id)
    }

    pub fn get_continuation_job(&self, job: Job) -> Option<Job> {
        let id = IdOf::ContinuationOf(job.id).get_id();
        match self.get_of_type::<JobId>(id) {
            Some(next_job_id) => self.get_job(next_job_id),
            None => None,
        }
    }

    pub fn save_job_id(&self, id: &JobId, stage: &Stage) -> anyhow::Result<()> {
        let key = format!("stage-{}-jobs", stage.get_name());
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .push(&key, id.to_string().as_bytes())
    }

    pub fn get_waiting_jobs(&self) -> anyhow::Result<Box<dyn crate::storage::StorageIter>> {
        let key = format!("stage-{}-jobs", WaitingStage::get_name());
        let iter = self
            .inner
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .scan_range(&key);

        Ok(iter)
    }
}
