use serde::{Deserialize, Serialize};

fnf_rs::background_job! {
    struct DeriveHandler {
        sample_message: SampleMessage,
        another_sample_message: AnotherSampleMessage,
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct SampleMessage {
    pub txt: String,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct AnotherSampleMessage {
    pub txt: String,
}

#[derive(Clone)]
pub struct JobContext {}
