#[macro_use]
extern crate rocket;

use bg::*;
use later::{mq::amqp, BackgroundJobServer, Config};
use rocket::State;
use tracing::Instrument;

mod bg {
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
}

#[tracing::instrument(skip(_ctx))]
async fn handle_sample_message(
    _ctx: DeriveHandlerContext<JobContext>,
    payload: SampleMessage,
) -> anyhow::Result<()> {
    tracing::info!("On Handle handle_sample_message: {:?}", payload);

    Ok(())
}

#[tracing::instrument(skip(_ctx))]
async fn handle_another_sample_message(
    _ctx: DeriveHandlerContext<JobContext>,
    payload: AnotherSampleMessage,
) -> anyhow::Result<()> {
    let prefix = format!("{}-cont", payload.txt);
    let parent_job_id = _ctx
        .enqueue(SampleMessage {
            txt: format!("{}-1", prefix),
        })
        .await?;
    let child_job_1_id = _ctx
        .enqueue_continue(
            parent_job_id.clone(),
            SampleMessage {
                txt: format!("{}-2", prefix),
            },
        )
        .await?;
    let child_job_2_id = _ctx
        .enqueue_continue(
            parent_job_id.clone(),
            SampleMessage {
                txt: format!("{}-3", prefix),
            },
        )
        .await?;

    tracing::info!(
        "On Handle handle_another_sample_message: {:?}, enqueued: cont-1:{}, cont-2(c): {}, cont-3(c): {}",
        payload, parent_job_id, child_job_1_id, child_job_2_id
    );

    Ok(())
}

struct AppContext {
    jobs: BackgroundJobServer<JobContext, DeriveHandler<JobContext>>,
}

#[get("/")]
#[tracing::instrument(skip(state))]
async fn hello(state: &State<AppContext>) -> String {
    let id = later::generate_id();
    let msg = AnotherSampleMessage {
        txt: format!("{id}-1"),
    };
    state.jobs.enqueue(msg).await.expect("Enqueue Job");
    "Hello, world!".to_string()
}

#[get("/metrics")]
#[tracing::instrument(skip(state))]
async fn metrics(state: &State<AppContext>) -> String {
    state.jobs.get_metrics().expect("metrics")
}

#[launch]
async fn rocket() -> rocket::Rocket<rocket::Build> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Registry;

    let layer_console = tracing_subscriber::fmt::Layer::new();

    let subscriber = Registry::default()
        .with(tracing_subscriber::EnvFilter::new("INFO"))
        .with(layer_console);

    match std::env::var("ENABLE_JAEGER") {
        Ok(_) => {
            let tracer_jaeger = opentelemetry_jaeger::new_pipeline()
                .with_service_name("later-redis-example")
                .install_simple()
                .unwrap();
            let layer_jaeger = tracing_opentelemetry::layer().with_tracer(tracer_jaeger);
            let subscriber = subscriber.with(layer_jaeger);
            tracing::subscriber::set_global_default(subscriber).unwrap();
        }
        Err(_) => tracing::subscriber::set_global_default(subscriber).unwrap(),
    };

    start()
        .instrument(tracing::info_span!("start application"))
        .await
}

async fn start() -> rocket::Rocket<rocket::Build> {
    let job_ctx = JobContext {};
    let storage = later::storage::Redis::new("redis://127.0.0.1/")
        .await
        .expect("connect to redis");
    let mq = amqp::RabbitMq::new("amqp://guest:guest@localhost:5672".into());
    let bjs = DeriveHandlerBuilder::new(
        Config::builder()
            .name("fnf-example".into())
            .context(job_ctx)
            .storage(Box::new(storage))
            .message_queue_client(Box::new(mq))
            .build(),
    )
    .with_sample_message_handler(handle_sample_message)
    .with_another_sample_message_handler(handle_another_sample_message)
    .build()
    .await
    .expect("start bg server");

    let ctx = AppContext { jobs: bjs };

    rocket::build()
        .mount("/", routes![hello, metrics])
        .manage(ctx)
}
