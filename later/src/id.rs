use crate::JobId;
pub(crate) struct Id(String);
impl Id {
    pub fn to_string(&self) -> String {
        self.0.clone()
    }
}

pub(crate) enum IdOf {
    SavedJob(JobId),
    ContinuationOf(JobId),
    JobsInStagesId(String /* Stage name */),
}

impl IdOf {
    pub fn get_id(&self, prefix: &str) -> Id {
        let id_str = match self {
            IdOf::SavedJob(id) => format!("job-{}", id),
            IdOf::ContinuationOf(id) => format!("job-{}-next", id),
            IdOf::JobsInStagesId(stage) => format!("stage-{}-jobs", stage),
        };

        Id(format!("{}-{}", prefix, id_str))
    }
}
