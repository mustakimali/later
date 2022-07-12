use super::bg::not_generated::*;
use later::{BackgroundJobServer, JobId, JobParameter};

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
    let bjs = DeriveHandlerBuilder::new(
        job_ctx,
        "fnf-example".into(),
        "amqp://guest:guest@localhost:5672".into(),
    )
    .with_sample_message_handler(handle_sample_message)
    .with_another_sample_message_handler(handle_another_sample_message)
    .build()
    .expect("start bg server");

    let _ctx = AppContext { jobs: bjs };
}
