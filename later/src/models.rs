use crate::{JobId, UtcDateTime};

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct Job {
    pub id: JobId,

    pub payload_type: String,
    pub payload: Vec<u8>,

    pub stages: Stage,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub(crate) enum Stage {
    /// Scheduled for later or waiting for
    Delayed(DelayedStage),
    Waiting(WaitingStage),
    Enqueued(EnqueuedStage),
    Running(RunningStage),
    Requeued(RequeuedStage),
    Success(SuccessStage),
    Failed(FailedStage),
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) struct DelayedStage {
    pub date: UtcDateTime,

    pub not_before: UtcDateTime,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) struct WaitingStage {
    pub date: UtcDateTime,

    pub parent_id: JobId,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) struct EnqueuedStage {
    pub date: UtcDateTime,

    pub previous_stages: Vec<Stage>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) struct RunningStage {
    pub date: UtcDateTime,

    pub previous_stages: Vec<Stage>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) struct SuccessStage {
    pub date: UtcDateTime,

    pub previous_stages: Vec<Stage>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) struct FailedStage {
    pub date: UtcDateTime,
    pub reason: String,

    pub previous_stages: Vec<Stage>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) struct RequeuedStage {
    pub date: UtcDateTime,
    pub requeue_count: u32,

    pub previous_stages: Vec<Stage>,
}

impl Job {
    pub fn transition(self) -> Job {
        Job {
            stages: self.stages.transition(),
            ..self
        }
    }
}

impl Stage {
    pub fn transition(self) -> Stage {
        match self {
            Stage::Delayed(delayed) => Stage::Enqueued(EnqueuedStage {
                date: chrono::Utc::now(),
                previous_stages: vec![Stage::Delayed(delayed.clone())],
            }),
            Stage::Waiting(waiting) => Stage::Enqueued(EnqueuedStage {
                date: chrono::Utc::now(),
                previous_stages: vec![Stage::Waiting(waiting.clone())],
            }),
            Stage::Enqueued(enqueued) => Stage::Running(RunningStage {
                date: chrono::Utc::now(),
                previous_stages: enqueued.previous_stages.clone(),
            }),
            Stage::Running(_) => todo!(),
            Stage::Requeued(_) => todo!(),
            Stage::Success(_) => self /* Terminal */,
            Stage::Failed(_) => self /* Terminal */,
        }
    }
}
