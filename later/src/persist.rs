use crate::{
    encoder::{self},
    id::{Id, IdOf},
    models::{Job, RequeuedStage, Stage, StageName, WaitingStage},
    storage::{Storage, StorageIter},
    JobId,
};
use serde::{de::DeserializeOwned, Serialize};

pub(crate) struct Persist {
    inner: Box<dyn Storage>,
    key_prefix: String,
}

impl Persist {
    pub fn new(storage: Box<dyn Storage>, key_prefix: String) -> Self {
        Self {
            inner: storage,
            key_prefix,
        }
    }

    pub async fn get_job(&self, id: JobId) -> Option<Job> {
        let id = IdOf::SavedJob(id).get_id(&self.key_prefix);
        self.get_of_type::<Job>(id).await
    }

    pub async fn expire(&self, job_id: JobId) -> anyhow::Result<()> {
        let id = IdOf::SavedJob(job_id).get_id(&self.key_prefix);
        Ok(self.inner.del(&id.to_string()).await?)
    }

    pub async fn trim(&self, range: Box<dyn StorageIter>) -> anyhow::Result<()> {
        Ok(self.inner.trim(&range).await?)
    }

    pub async fn get_of_type<T>(&self, id: Id) -> Option<T>
    where
        T: DeserializeOwned,
    {
        self.inner
            .get(&id.to_string())
            .await
            .and_then(|bytes| encoder::decode::<T>(&bytes).ok())
    }

    pub async fn save_jobs(&self, id: JobId, job: &Job) -> anyhow::Result<()> {
        let id = IdOf::SavedJob(id).get_id(&self.key_prefix);
        self.save(id, job).await
    }

    pub async fn save<T>(&self, id: Id, item: T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let bytes = encoder::encode(item)?;
        self.inner.set(&id.to_string(), &bytes).await
    }

    pub async fn save_continuation(
        &self,
        job_id: &JobId,
        parent_job_id: JobId,
    ) -> anyhow::Result<()> {
        let id = IdOf::ContinuationOf(parent_job_id).get_id(&self.key_prefix);
        self.save(id, job_id).await
    }

    pub async fn get_continuation_job(&self, job: Job) -> Option<Job> {
        let id = IdOf::ContinuationOf(job.id).get_id(&self.key_prefix);
        // ToDo: more than one job be in scheduled
        match self.get_of_type::<JobId>(id).await {
            Some(next_job_id) => self.get_job(next_job_id).await,
            None => None,
        }
    }

    pub async fn save_job_id(&self, id: &JobId, stage: &Stage) -> anyhow::Result<()> {
        let key = IdOf::JobsInStagesId(stage.get_name()).get_id(&&self.key_prefix);

        self.inner
            .push(&key.to_string(), &encoder::encode(&id)?)
            .await?;

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn get_waiting_jobs(&self) -> anyhow::Result<Box<dyn StorageIter>> {
        self.get_jobs_to_poll(&WaitingStage::get_name()).await
    }

    pub async fn get_reqd_jobs(&self) -> anyhow::Result<Box<dyn StorageIter>> {
        self.get_jobs_to_poll(&RequeuedStage::get_name()).await
    }

    async fn get_jobs_to_poll(&self, name: &str) -> Result<Box<dyn StorageIter>, anyhow::Error> {
        let key = IdOf::JobsInStagesId(name.to_string()).get_id(&&self.key_prefix);
        let iter = self.inner.scan_range(&key.to_string()).await;
        Ok(iter)
    }
}
