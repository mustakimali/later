use later::{storage::redis::Redis, BackgroundJobServer};
use macro_generated::*;
use rocket::State;
use serde::{Deserialize, Serialize};
#[macro_use]
extern crate rocket;

struct AppContext {
    jobs: BackgroundJobServer<JobContext, DeriveHandler<JobContext>>,
}

async fn handle_sample_message(
    ctx: DeriveHandlerContext<JobContext>,
    payload: SampleMessage,
) -> anyhow::Result<()> {
    println!("On Handle handle_sample_message: {:?}", payload);

    let _ = ctx
        .enqueue(AnotherSampleMessage {
            txt: "test".to_string(),
        })
        .await;

    Ok(())
}

async fn handle_another_sample_message(
    _ctx: DeriveHandlerContext<JobContext>,
    payload: AnotherSampleMessage,
) -> anyhow::Result<()> {
    println!("On Handle handle_another_sample_message: {:?}", payload);

    Ok(())
}

#[get("/")]
async fn hello(state: &State<AppContext>) -> String {
    let id = later::generate_id();
    let msg = SampleMessage { txt: id.clone() };
    let parent_id = state.jobs.enqueue(msg).await.expect("Enqueue Job");
    let msg2 = AnotherSampleMessage { txt: id };
    state
        .jobs
        .enqueue_continue(parent_id, msg2)
        .await
        .expect("Enqueue Job");
    "Hello, world!".to_string()
}

#[get("/metrics")]
async fn metrics(state: &State<AppContext>) -> String {
    state.jobs.get_metrics().expect("metrics")
}

#[launch]
async fn rocket() -> _ {
    let job_ctx = JobContext {};
    let storage = Redis::new("redis://127.0.0.1/")
        .await
        .expect("connect to redis");
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

    let ctx = AppContext { jobs: bjs };

    rocket::build()
        .mount("/", routes![hello, metrics])
        .manage(ctx)
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
mod macro_generated {
    pub struct DeriveHandlerContext<C: Send + Sync> {
        inner: std::sync::Arc<DeriveHandlerContextInner<C>>,
    }

    impl<C> std::ops::Deref for DeriveHandlerContext<C>
    where
        C: Send + Sync,
    {
        type Target = DeriveHandlerContextInner<C>;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    pub struct DeriveHandlerContextInner<C> {
        job: ::later::BackgroundJobServerPublisher,
        app: C,
    }

    impl<C> std::ops::Deref for DeriveHandlerContextInner<C> {
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
            Box<
                dyn Fn(
                        std::sync::Arc<DeriveHandlerContextInner<C>>,
                        super::SampleMessage,
                    )
                        -> ::later::futures::future::BoxFuture<'static, anyhow::Result<()>>
                    + Sync
                    + Send,
            >,
        >,
        another_sample_message: ::core::option::Option<
            Box<
                dyn Fn(
                        std::sync::Arc<DeriveHandlerContextInner<C>>,
                        super::AnotherSampleMessage,
                    )
                        -> ::later::futures::future::BoxFuture<'static, anyhow::Result<()>>
                    + Sync
                    + Send,
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

        /// Accept a simplified and ergonomic async function handler
        ///     Fn(Ctx<C>, Payload) -> impl Future<Output = anyhow::Result<()>>
        /// and map this to the complex/nasty stuff required internally to make the compiler happy.
        ///     Fn(Arc<CtxWrapper<C>>, Payload) -> Pin<Box<Future<Output = anyhow::Result<()>>>
        fn wrap_complex_handler<Payload, HandlerFunc, Fut>(
            arc_ctx: std::sync::Arc<DeriveHandlerContextInner<C>>,
            payload: Payload,
            handler: HandlerFunc,
        ) -> ::later::futures::future::BoxFuture<'static, Fut::Output>
        where
            HandlerFunc: FnOnce(DeriveHandlerContext<C>, Payload) -> Fut + Send + 'static,
            Payload: ::later::core::JobParameter + Send + 'static,
            Fut: ::later::futures::future::Future<Output = anyhow::Result<()>> + Send,
        {
            Box::pin(async move {
                let ctx = DeriveHandlerContext {
                    inner: arc_ctx.clone(),
                };
                handler(ctx, payload).await
            })
        }

        ///Register a handler for [`SampleMessage`].
        ///This handler will be called when a job is enqueued with a payload of this type.
        pub fn with_sample_message_handler<M, Fut>(mut self, handler: M) -> Self
        where
            M: FnOnce(DeriveHandlerContext<C>, super::SampleMessage) -> Fut
                + Send
                + Sync
                + Copy
                + 'static,
            Fut: ::later::futures::future::Future<Output = anyhow::Result<()>> + Send,
            C: Sync + Send + 'static,
        {
            self.sample_message = Some(Box::new(move |ctx, payload| {
                Self::wrap_complex_handler(ctx, payload, handler)
            }));
            self
        }

        ///Register a handler for [`AnotherSampleMessage`].
        ///This handler will be called when a job is enqueued with a payload of this type.
        pub fn with_another_sample_message_handler<M, Fut>(mut self, handler: M) -> Self
        where
            M: FnOnce(DeriveHandlerContext<C>, super::AnotherSampleMessage) -> Fut
                + Send
                + Sync
                + Copy
                + 'static,
            Fut: ::later::futures::future::Future<Output = anyhow::Result<()>> + Send,
            C: Sync + Send + 'static,
        {
            self.another_sample_message = Some(Box::new(move |ctx, payload| {
                Self::wrap_complex_handler(ctx, payload, handler)
            }));
            self
        }

        pub fn build(self) -> anyhow::Result<::later::BackgroundJobServer<C, DeriveHandler<C>>> {
            let publisher = ::later::BackgroundJobServerPublisher::new(
                self.id.clone(),
                self.amqp_address.clone(),
                self.storage,
            )?;
            let ctx_inner = DeriveHandlerContextInner {
                job: publisher,
                app: self.ctx,
            };
            let handler = DeriveHandler {
                ctx: std::sync::Arc::new(ctx_inner),
                sample_message: self.sample_message,
                another_sample_message: self.another_sample_message,
            };

            ::later::BackgroundJobServer::start(handler)
        }
    }
    impl ::later::core::JobParameter for super::SampleMessage {
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
    impl ::later::core::JobParameter for super::AnotherSampleMessage {
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
        pub ctx: std::sync::Arc<DeriveHandlerContextInner<C>>,
        pub sample_message: ::core::option::Option<
            Box<
                dyn Fn(
                        std::sync::Arc<DeriveHandlerContextInner<C>>,
                        super::SampleMessage,
                    )
                        -> ::later::futures::future::BoxFuture<'static, anyhow::Result<()>>
                    + Send
                    + Sync,
            >,
        >,
        pub another_sample_message: ::core::option::Option<
            Box<
                dyn Fn(
                        std::sync::Arc<DeriveHandlerContextInner<C>>,
                        super::AnotherSampleMessage,
                    )
                        -> ::later::futures::future::BoxFuture<'static, anyhow::Result<()>>
                    + Send
                    + Sync,
            >,
        >,
    }

    #[async_trait]
    impl<C> ::later::core::BgJobHandler<C> for DeriveHandler<C>
    where
        C: Sync + Send + 'static,
    {
        async fn dispatch(&self, ptype: String, payload: &[u8]) -> anyhow::Result<()> {
            use later::core::JobParameter;
            match ptype.as_str() {
                "sample_message" => {
                    let payload = super::SampleMessage::from_bytes(payload);
                    if let Some(handler) = &self.sample_message {
                        (handler)(self.ctx.clone(), payload).await
                    } else {
                        unimplemented!()
                    }
                }
                "another_sample_message" => {
                    let payload = super::AnotherSampleMessage::from_bytes(payload);
                    if let Some(handler) = &self.another_sample_message {
                        (handler)(self.ctx.clone(), payload).await
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
}
