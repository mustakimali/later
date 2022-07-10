use serde::{Deserialize, Serialize};

fnf_derive::background_job! {
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


// -- GENERATED --
/*
use ::fnf_rs::JobParameter;
impl ::fnf_rs::JobParameter for SampleMessage {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let result = ::fnf_rs::serde_json::to_vec(&self);
        let result = ::fnf_rs::anyhow::Context::context(result, "unable to serialize");
        Ok(result?)
    }
    fn from_bytes(payload: &[u8]) -> Self {
        ::fnf_rs::serde_json::from_slice(payload).unwrap()
    }

    fn get_ptype(&self) -> String {
        "sample_message".into()
    }
}
impl ::fnf_rs::JobParameter for AnotherSampleMessage {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let result = ::fnf_rs::serde_json::to_vec(&self);
        let result = ::fnf_rs::anyhow::Context::context(result, "unable to serialize");
        Ok(result?)
    }
    fn from_bytes(payload: &[u8]) -> Self {
        ::fnf_rs::serde_json::from_slice(payload).unwrap()
    }

    fn get_ptype(&self) -> String {
        "another_sample_message".into()
    }
}

pub struct DeriveHandler<C> {
    pub sample_message: Box<dyn Fn(&C, SampleMessage) -> anyhow::Result<()> + Send + Sync>,
    pub another_sample_message:
        Box<dyn Fn(&C, AnotherSampleMessage) -> anyhow::Result<()> + Send + Sync>,
}
impl<C> ::fnf_rs::BgJobHandler<C> for DeriveHandler<C> {
    fn dispatch(
        &self,
        ctx: C,
        ptype: String,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        match ptype.as_str() {
            "sample_message" => {
                let payload = SampleMessage::from_bytes(payload);
                (self.sample_message)(&ctx, payload)
            }
            "another_sample_message" => {
                let payload = AnotherSampleMessage::from_bytes(payload);
                (self.another_sample_message)(&ctx, payload)
            }
            _ => panic!("internal error: entered unreachable code"),
        }
    }
}
 */
