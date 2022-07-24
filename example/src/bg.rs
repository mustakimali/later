use serde::{Deserialize, Serialize};

later::background_job! {
    struct DeriveHandler {
        sample_message: SampleMessage,
        another_sample_message: AnotherSampleMessage,
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SampleMessage {
    pub txt: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AnotherSampleMessage {
    pub txt: String,
}

#[derive(Clone)]
pub struct JobContext {}
