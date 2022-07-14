use std::sync::RwLock;

use crate::{id::IdOf, storage::Storage, JobId};

pub(crate) struct Persist {
    inner: RwLock<Box<dyn Storage>>,
}

impl Persist {
    pub fn new(storage: Box<dyn Storage>) -> Self {
        Self {
            inner: RwLock::new(storage),
        }
    }

    pub fn save_jobs(&self, id: JobId, data: &[u8]) -> anyhow::Result<()> {
        let id = IdOf::SavedJob(id).get_id();
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .set(&id.to_string(), data)
    }
}
