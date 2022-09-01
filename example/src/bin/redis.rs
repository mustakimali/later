use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use bg::*;
use later::{mq::amqp, BackgroundJobServer, Config};
use serde_json::json;
use std::sync::Arc;
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
async fn enqueue(state: actix_web::web::Data<Arc<AppContext>>) -> impl Responder {
    let id = later::generate_id();
    let msg = AnotherSampleMessage {
        txt: format!("{id}-1"),
    };
    let id = state.jobs.enqueue(msg).await.expect("Enqueue Job");
    HttpResponse::Ok().json(json!({ "id": id }))
}

#[get("/{num}")]
#[tracing::instrument(skip(state))]
async fn enqueue_num(num: web::Path<usize>, state: web::Data<Arc<AppContext>>) -> impl Responder {
    let mut ids = Vec::new();
    for i in 0..*num {
        let id = later::generate_id();
        let msg = SampleMessage {
            txt: format!("{id}-{}", i),
        };
        let id = state.jobs.enqueue(msg).await.expect("Enqueue Job");
        ids.push(id.to_string());
    }

    HttpResponse::Ok().json(ids)
}

#[get("/dash?{query}")]
async fn dashboard(state: web::Data<Arc<AppContext>>, query: web::Query<String>) -> impl Responder {
    if let Ok(res) = state.jobs.get_dashboard(query.to_string()).await {
        let mut builder = HttpResponse::Ok();
        builder.append_header(("access-control-allow-origin", "*"));

        for (k, v) in res.headers {
            builder.append_header((k, v));
        }

        return builder.body(res.body);
    }

    HttpResponse::NotFound().finish()
}

#[get("/metrics")]
#[tracing::instrument(skip(state))]
async fn metrics(state: web::Data<AppContext>) -> String {
    state.jobs.get_metrics().expect("metrics")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
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

async fn start() -> std::io::Result<()> {
    let port = std::env::var("PORT")
        .unwrap_or("8080".into())
        .parse()
        .unwrap_or(8080);
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
    let ctx = Arc::new(ctx);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(ctx.clone()))
            .service(enqueue)
            .service(enqueue_num)
            .service(dashboard)
            .service(metrics)
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await
}
