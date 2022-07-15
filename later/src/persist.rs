use std::sync::RwLock;

use crate::{
    encoder::{self, encode},
    id::IdOf,
    models::Job,
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
}
