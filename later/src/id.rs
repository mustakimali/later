use crate::{JobId, RecurringJobId};
pub(crate) struct Id(String);
impl Id {
    pub fn to_string(&self) -> String {
        self.0.clone()
    }
}

pub(crate) enum IdOf {
    SavedJob(JobId),
    SavedRecurringJob(RecurringJobId),
    ContinuationOf(JobId),
    JobsInStagesId(String /* Stage name */),
    ConfigDateLastPolledForReqdJobs,
    ConfigDateLastPolledForDelayedJobs,
}

impl IdOf {
    pub fn get_id(&self, prefix: &str) -> Id {
        let id_str = match self {
            IdOf::SavedJob(id) => format!("job-{}", id),
            IdOf::SavedRecurringJob(id) => format!("rec-job-{}", id),
            IdOf::ContinuationOf(id) => format!("job-{}-next", id),
            IdOf::JobsInStagesId(stage) => format!("stage-{}-jobs", stage),
            IdOf::ConfigDateLastPolledForReqdJobs => "date-polled-reqd-jobs".into(),
            IdOf::ConfigDateLastPolledForDelayedJobs => "date-polled-delayed-jobs".into(),
        };

        Id(format!("{}-{}", prefix, id_str))
    }
}
