use crate::JobId;
pub(crate) struct Id(String);
impl Id {
    pub fn to_string(&self) -> String {
        self.0.clone()
    }
}

pub(crate) enum IdOf {
    SavedJob(JobId),
    ContinuationOf(JobId)
}

impl IdOf {
    pub fn get_id(&self) -> Id {
        match self {
            IdOf::SavedJob(id) => Id(format!("job-{}", id)),
            IdOf::ContinuationOf(id) => Id(format!("job-{}-next", id)),
        }
    }
}
