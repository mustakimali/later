use std::sync::RwLock;

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    encoder::{self},
    id::{Id, IdOf},
    models::{Job, RequeuedStage, Stage, StageName, WaitingStage},
    storage::{Storage, StorageIter},
    JobId,
};

pub(crate) struct Persist {
    inner: RwLock<Box<dyn Storage>>,
    key_prefix: String,
}

impl Persist {
    pub fn new(storage: Box<dyn Storage>, key_prefix: String) -> Self {
        Self {
            inner: RwLock::new(storage),
            key_prefix,
        }
    }

    pub fn get_job(&self, id: JobId) -> Option<Job> {
        let id = IdOf::SavedJob(id).get_id(&self.key_prefix);
        self.get_of_type::<Job>(id)
    }

    pub fn expire(&self, job_id: JobId) -> anyhow::Result<()> {
        let id = IdOf::SavedJob(job_id).get_id(&self.key_prefix);
        Ok(self
            .inner
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .del(&id.to_string())?)
    }

    pub fn trim(&self, range: Box<dyn StorageIter>) -> anyhow::Result<()> {
        Ok(self
            .inner
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .trim(&range)?)
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
        let id = IdOf::SavedJob(id).get_id(&self.key_prefix);
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
        let id = IdOf::ContinuationOf(parent_job_id).get_id(&self.key_prefix);
        self.save(id, job_id)
    }

    pub fn get_continuation_job(&self, job: Job) -> Option<Job> {
        let id = IdOf::ContinuationOf(job.id).get_id(&self.key_prefix);
        match self.get_of_type::<JobId>(id) {
            Some(next_job_id) => self.get_job(next_job_id),
            None => None,
        }
    }

    pub fn save_job_id(&self, id: &JobId, stage: &Stage) -> anyhow::Result<()> {
        let key = IdOf::JobsInStagesId(stage.get_name()).get_id(&&self.key_prefix);
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .push(&key.to_string(), &encoder::encode(&id)?)
    }

    pub fn get_waiting_jobs(&self) -> anyhow::Result<Box<dyn StorageIter>> {
        self.get_jobs_to_poll(&WaitingStage::get_name())
    }

    pub fn get_reqd_jobs(&self) -> anyhow::Result<Box<dyn StorageIter>> {
        self.get_jobs_to_poll(&RequeuedStage::get_name())
    }

    fn get_jobs_to_poll(&self, name: &str) -> Result<Box<dyn StorageIter>, anyhow::Error> {
        let key = IdOf::JobsInStagesId(name.to_string()).get_id(&&self.key_prefix);
        let iter = self
            .inner
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .scan_range(&key.to_string());
        Ok(iter)
    }
}
