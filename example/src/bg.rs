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
    use fnf_rs::{BackgroundJobServer, BackgroundJobServerPublisher};
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

    fn handle_sample_message(
        _ctx: &DeriveHandlerContext<JobContext>,
        payload: SampleMessage,
    ) -> anyhow::Result<()> {
        println!("On Handle handle_sample_message: {:?}", payload);

        Ok(())
    }
    fn handle_another_sample_message(
        _ctx: &DeriveHandlerContext<JobContext>,
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

    pub struct DeriveHandlerContext<C> {
        job: ::fnf_rs::BackgroundJobServerPublisher,
        app: C,
    }

    impl<C> DeriveHandlerContext<C> {
        pub fn enqueue(
            &self,
            message: impl ::fnf_rs::JobParameter,
        ) -> anyhow::Result<fnf_rs::JobId> {
            self.job.enqueue(message)
        }
    }

    pub struct DeriveHandlerBuilder<C>
    where
        C: Sync + Send + 'static,
    {
        ctx: C,
        id: String,
        amqp_address: String,
        sample_message: ::core::option::Option<
            Box<
                dyn Fn(&DeriveHandlerContext<C>, SampleMessage) -> anyhow::Result<()> + Send + Sync,
            >,
        >,
        another_sample_message: ::core::option::Option<
            Box<
                dyn Fn(&DeriveHandlerContext<C>, AnotherSampleMessage) -> anyhow::Result<()>
                    + Send
                    + Sync,
            >,
        >,
    }
    impl<C> DeriveHandlerBuilder<C>
    where
        C: Sync + Send + 'static,
    {
        pub fn new(context: C, id: String, amqp_address: String) -> Self
        where
            C: Sync + Send + 'static,
        {
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
            M: Fn(&DeriveHandlerContext<C>, SampleMessage) -> anyhow::Result<()>
                + Send
                + Sync
                + 'static,
            C: Sync + Send + 'static,
        {
            self.sample_message = Some(Box::new(handler));
            self
        }
        ///Register a handler for [`AnotherSampleMessage`].
        ///This handler will be called when a job is enqueued with a payload of this type.
        pub fn with_another_sample_message_handler<M>(mut self, handler: M) -> Self
        where
            M: Fn(&DeriveHandlerContext<C>, AnotherSampleMessage) -> anyhow::Result<()>
                + Send
                + Sync
                + 'static,
            C: Sync + Send + 'static,
        {
            self.another_sample_message = Some(Box::new(handler));
            self
        }
        pub fn build(self) -> anyhow::Result<BackgroundJobServer<C, DeriveHandler<C>>>
        where
            C: Sync + Send + Clone + 'static,
        {
            let publisher =
                BackgroundJobServerPublisher::new(self.id.clone(), self.amqp_address.clone())?;
            let ctx = DeriveHandlerContext {
                job: publisher,
                app: self.ctx,
            };
            let handler = DeriveHandler {
                ctx: ctx,
                sample_message: self.sample_message,
                another_sample_message: self.another_sample_message,
            };

            let publisher = BackgroundJobServerPublisher::new(self.id, self.amqp_address)?;
            BackgroundJobServer::start(handler, publisher)
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
    pub struct DeriveHandler<C>
    where
        C: Sync + Send + 'static,
    {
        pub ctx: DeriveHandlerContext<C>,
        pub sample_message: ::core::option::Option<
            Box<
                dyn Fn(&DeriveHandlerContext<C>, SampleMessage) -> anyhow::Result<()> + Send + Sync,
            >,
        >,
        pub another_sample_message: ::core::option::Option<
            Box<
                dyn Fn(&DeriveHandlerContext<C>, AnotherSampleMessage) -> anyhow::Result<()>
                    + Send
                    + Sync,
            >,
        >,
    }
    impl<C> ::fnf_rs::BgJobHandler<C> for DeriveHandler<C>
    where
        C: Sync + Send + 'static,
    {
        fn dispatch(&self, ptype: String, payload: &[u8]) -> anyhow::Result<()> {
            match ptype.as_str() {
                "sample_message" => {
                    let payload = SampleMessage::from_bytes(payload);
                    if let Some(handler) = &self.sample_message {
                        (handler)(&self.ctx, payload)
                    } else {
                        unimplemented!()
                    }
                }
                "another_sample_message" => {
                    let payload = AnotherSampleMessage::from_bytes(payload);
                    if let Some(handler) = &self.another_sample_message {
                        (handler)(&self.ctx, payload)
                    } else {
                        unimplemented!()
                    }
                }
                _ => unimplemented!(),
            }
        }
        fn get_ctx(&self) -> &C {
            &self.ctx.app
        }
    }
}
