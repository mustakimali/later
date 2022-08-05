use lazy_static::lazy_static;
use prometheus::{register_int_counter_vec, Encoder, IntCounterVec, TextEncoder};

use crate::models::AmqpCommand;

lazy_static! {
    pub(crate) static ref COUNTER: Metrics = Metrics::new();
}

pub(crate) struct Metrics {
    pub commands_all: IntCounterVec,
    pub commands_failed: IntCounterVec,
    pub jobs_all: IntCounterVec,
    //registry: Registry,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            commands_all: register_int_counter_vec!(
                "commands_all",
                "total commands processed",
                &["type"]
            )
            .unwrap(),
            commands_failed: register_int_counter_vec!(
                "commands_failed",
                "total commands failed",
                &["type"]
            )
            .unwrap(),
            jobs_all: register_int_counter_vec!("jobs_all", "total jobs processed", &["type"])
                .unwrap(),
        }
    }

    pub fn output(&self) -> anyhow::Result<String> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        encoder.encode(&metric_families, &mut buffer)?;

        Ok(String::from_utf8(buffer.clone())?)
    }

    pub fn record_command(&self, cmd: &AmqpCommand) {
        let ty = cmd.get_type();
        self.commands_all.with_label_values(&[&ty]).inc();
        if let AmqpCommand::ExecuteJob(_) = cmd {
            self.jobs_all.with_label_values(&[&ty]).inc();
        }
    }
}
