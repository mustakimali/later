use lazy_static::lazy_static;
use prometheus::{
    core::MetricVec, register_int_counter_vec, Counter, Encoder, IntCounterVec, Opts, Registry,
    TextEncoder,
};

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
        let a = register_int_counter_vec!("commands_all", "total commands processed", &["type"])
            .unwrap();
        let b = register_int_counter_vec!("commands_failed", "total commands failed", &["type"])
            .unwrap();
        let c = register_int_counter_vec!("jobs_all", "total jobs processed", &["type"]).unwrap();

        // let registry = Registry::new();
        // registry.register(Box::new(a.clone())).unwrap();
        // registry.register(Box::new(b.clone())).unwrap();
        // registry.register(Box::new(c.clone())).unwrap();

        Metrics {
            commands_all: a,
            commands_failed: b,
            jobs_all: c,
            //registry: registry,
        }
    }

    pub fn output(&self) -> anyhow::Result<String> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        encoder.encode(&metric_families, &mut buffer)?;

        Ok(String::from_utf8(buffer.clone())?)
    }
}
