use crate::{
    encoder::{self},
    id::{Id, IdOf},
    models::{DelayedStage, Job, RequeuedStage, Stage, StageName, WaitingStage},
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

    pub async fn push<T>(&self, id: Id, item: T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let bytes = encoder::encode(item)?;
        self.inner.push(&id.to_string(), &bytes).await
    }

    pub async fn save_continuation(
        &self,
        job_id: &JobId,
        parent_job_id: JobId,
    ) -> anyhow::Result<()> {
        let id = IdOf::ContinuationOf(parent_job_id).get_id(&self.key_prefix);
        self.push(id, job_id).await
    }

    pub async fn get_continuation_job(&self, job: &Job) -> Option<Vec<Job>> {
        let id = IdOf::ContinuationOf(job.id.clone()).get_id(&self.key_prefix);

        let mut items = Vec::default();
        let mut iter = self.inner.scan_range(&id.to_string()).await;
        while let Some(id_bytes) = iter.next().await {
            if let Ok(job_id) = encoder::decode::<JobId>(&id_bytes) {
                if let Some(job) = self.get_job(job_id).await {
                    items.push(job);
                }
            }
        }

        if items.len() > 0 {
            Some(items)
        } else {
            None
        }
    }

    pub async fn del_get_continuation_job(&self, job: &Job) -> anyhow::Result<()> {
        let id = IdOf::ContinuationOf(job.id.clone()).get_id(&self.key_prefix);
        self.inner.del_range(&id.to_string()).await
    }

    pub async fn save_job_id(&self, id: &JobId, stage: &Stage) -> anyhow::Result<()> {
        let key = IdOf::JobsInStagesId(stage.get_name()).get_id(&&self.key_prefix);

        self.inner
            .push(&key.to_string(), &encoder::encode(&id)?)
            .await?;

        Ok(())
    }

    pub async fn get_delayed_jobs(&self) -> anyhow::Result<Box<dyn StorageIter>> {
        self.get_jobs_to_poll(&DelayedStage::get_name()).await
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
