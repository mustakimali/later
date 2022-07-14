use later::{core::JobParameter, storage::redis::Redis, BackgroundJobServer, JobId};

struct AppContext {
    jobs: BackgroundJobServer<JobContext, DeriveHandler<JobContext>>,
}

impl AppContext {
    pub fn enqueue<T: JobParameter>(&self, msg: T) -> anyhow::Result<JobId> {
        self.jobs
            //.lock()
            //.map_err(|e| anyhow::anyhow!(e.to_string()))?
            .enqueue(msg)
    }
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
    let _ = _ctx.enqueue(AnotherSampleMessage {
        txt: "test".to_string(),
    });

    println!("On Handle handle_another_sample_message: {:?}", payload);

    Ok(())
}

pub fn test_non_generated() {
    let job_ctx = JobContext {};
    let storage = Redis::new("redis://127.0.0.1/").expect("connect to redis");
    let bjs = DeriveHandlerBuilder::new(
        job_ctx,
        "fnf-example".into(),
        "amqp://guest:guest@localhost:5672".into(),
        Box::new(storage),
    )
    .with_sample_message_handler(handle_sample_message)
    .with_another_sample_message_handler(handle_another_sample_message)
    .build()
    .expect("start bg server");

    let _ctx = AppContext { jobs: bjs };
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

use serde::{Deserialize, Serialize};

pub struct DeriveHandlerContext<C> {
    job: ::later::BackgroundJobServerPublisher,
    app: C,
}

impl<C> std::ops::Deref for DeriveHandlerContext<C> {
    type Target = ::later::BackgroundJobServerPublisher;

    fn deref(&self) -> &Self::Target {
        &self.job
    }
}

pub struct DeriveHandlerBuilder<C>
where
    C: Sync + Send + 'static,
{
    ctx: C,
    id: String,
    amqp_address: String,
    storage: Box<dyn ::later::storage::Storage>,
    sample_message: ::core::option::Option<
        Box<dyn Fn(&DeriveHandlerContext<C>, SampleMessage) -> anyhow::Result<()> + Send + Sync>,
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
    pub fn new(
        context: C,
        id: String,
        amqp_address: String,
        storage: Box<dyn ::later::storage::Storage>,
    ) -> Self
    where
        C: Sync + Send + 'static,
    {
        Self {
            ctx: context,
            id,
            amqp_address,
            storage,

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
    pub fn build(self) -> anyhow::Result<BackgroundJobServer<C, DeriveHandler<C>>> {
        let publisher = ::later::BackgroundJobServerPublisher::new(
            self.id.clone(),
            self.amqp_address.clone(),
            self.storage,
        )?;
        let ctx = DeriveHandlerContext {
            job: publisher,
            app: self.ctx,
        };
        let handler = DeriveHandler {
            ctx: ctx,
            sample_message: self.sample_message,
            another_sample_message: self.another_sample_message,
        };

        BackgroundJobServer::start(handler)
    }
}
impl ::later::core::JobParameter for SampleMessage {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let result = ::later::encoder::encode(&self);
        let result = ::later::anyhow::Context::context(result, "unable to serialize");
        Ok(result?)
    }
    fn from_bytes(payload: &[u8]) -> Self {
        ::later::encoder::decode(payload).unwrap()
    }
    fn get_ptype(&self) -> String {
        "sample_message".into()
    }
}
impl ::later::core::JobParameter for AnotherSampleMessage {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let result = ::later::encoder::encode(&self);
        let result = ::later::anyhow::Context::context(result, "unable to serialize");
        Ok(result?)
    }
    fn from_bytes(payload: &[u8]) -> Self {
        ::later::encoder::decode(payload).unwrap()
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
        Box<dyn Fn(&DeriveHandlerContext<C>, SampleMessage) -> anyhow::Result<()> + Send + Sync>,
    >,
    pub another_sample_message: ::core::option::Option<
        Box<
            dyn Fn(&DeriveHandlerContext<C>, AnotherSampleMessage) -> anyhow::Result<()>
                + Send
                + Sync,
        >,
    >,
}
impl<C> ::later::core::BgJobHandler<C> for DeriveHandler<C>
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

    fn get_publisher(&self) -> &::later::BackgroundJobServerPublisher {
        &self.ctx.job
    }
}
