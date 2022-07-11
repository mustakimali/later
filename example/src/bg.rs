use fnf_rs::BackgroundJobServer;
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

#[allow(dead_code)]
pub mod not_generated {
    use fnf_rs::BackgroundJobServer;
    use serde::{Deserialize, Serialize};

    fn main() {
        let job_ctx = JobContext {};
        let _bjs = DeriveHandlerBuilder::new(
            job_ctx,
            "fnf-example".into(),
            "amqp://guest:guest@localhost:5672".into(),
        )
        .with_sample_message_handler(handle_sample_message)
        .with_another_sample_message_handler(handle_another_sample_message)
        .build()
        .expect("start bg server");
    }

    fn handle_sample_message(_ctx: &JobContext, payload: SampleMessage) -> anyhow::Result<()> {
        println!("On Handle handle_sample_message: {:?}", payload);

        Ok(())
    }
    fn handle_another_sample_message(
        _ctx: &JobContext,
        payload: AnotherSampleMessage,
    ) -> anyhow::Result<()> {
        println!("On Handle handle_another_sample_message: {:?}", payload);

        Ok(())
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

    /* GENERATED */

    use ::fnf_rs::JobParameter;
    pub struct DeriveHandlerBuilder<C> {
        ctx: C,
        id: String,
        amqp_address: String,
        sample_message: ::core::option::Option<
            Box<dyn Fn(&C, SampleMessage) -> anyhow::Result<()> + Send + Sync>,
        >,
        another_sample_message: ::core::option::Option<
            Box<dyn Fn(&C, AnotherSampleMessage) -> anyhow::Result<()> + Send + Sync>,
        >,
    }
    impl<C> DeriveHandlerBuilder<C> {
        pub fn new(context: C, id: String, amqp_address: String) -> Self {
            Self {
                ctx: context,
                id,
                amqp_address,
                sample_message: ::core::option::Option::None,
                another_sample_message: ::core::option::Option::None,
            }
        }
        ///Register a handler for [`SampleMessage`].
        ///This handler will be called when a job is enqueued with a payload of this type.
        pub fn with_sample_message_handler<M>(mut self, handler: M) -> Self
        where
            M: Fn(&C, SampleMessage) -> anyhow::Result<()> + Send + Sync + 'static,
        {
            self.sample_message = Some(Box::new(handler));
            self
        }
        ///Register a handler for [`AnotherSampleMessage`].
        ///This handler will be called when a job is enqueued with a payload of this type.
        pub fn with_another_sample_message_handler<M>(mut self, handler: M) -> Self
        where
            M: Fn(&C, AnotherSampleMessage) -> anyhow::Result<()> + Send + Sync + 'static,
        {
            self.another_sample_message = Some(Box::new(handler));
            self
        }
        pub fn build(self) -> anyhow::Result<BackgroundJobServer<C, DeriveHandler<C>>>
        where
            C: Sync + Send + Clone + 'static,
        {
            let handler = DeriveHandler {
                ctx: self.ctx,
                sample_message: self.sample_message,
                another_sample_message: self.another_sample_message,
            };

            BackgroundJobServer::start(&self.id, self.amqp_address, handler)
        }
    }
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
        pub ctx: C,
        pub sample_message: ::core::option::Option<
            Box<dyn Fn(&C, SampleMessage) -> anyhow::Result<()> + Send + Sync>,
        >,
        pub another_sample_message: ::core::option::Option<
            Box<dyn Fn(&C, AnotherSampleMessage) -> anyhow::Result<()> + Send + Sync>,
        >,
    }
    impl<C> ::fnf_rs::BgJobHandler<C> for DeriveHandler<C> {
        fn dispatch(&self, ctx: C, ptype: String, payload: &[u8]) -> anyhow::Result<()> {
            match ptype.as_str() {
                "sample_message" => {
                    let payload = SampleMessage::from_bytes(payload);
                    if let Some(handler) = &self.sample_message {
                        (handler)(&ctx, payload)
                    } else {
                        unimplemented!()
                    }
                }
                "another_sample_message" => {
                    let payload = AnotherSampleMessage::from_bytes(payload);
                    if let Some(handler) = &self.another_sample_message {
                        (handler)(&ctx, payload)
                    } else {
                        unimplemented!()
                    }
                }
                _ => unimplemented!(),
            }
        }
        fn get_ctx(&self) -> &C {
            &self.ctx
        }
    }
}
