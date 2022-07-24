use later::{storage::redis::Redis, BackgroundJobServer};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{sync::Mutex, time::SystemTime};

lazy_static! {
    /// This is an example for using doc comment attributes
    static ref COMMANDS: Mutex<Vec<TestCommand>> = Mutex::new(Vec::default());
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TestCommand {
    Success,
    Retry(usize),
}

#[derive(Clone)]
struct AppContext {}

later::background_job! {
    struct JobServer {
        test_command: TestCommand,
    }
}

fn handle_command(_ctx: &JobServerContext<AppContext>, payload: TestCommand) -> anyhow::Result<()> {
    COMMANDS.lock().unwrap().push(payload.clone());
    let retry_count = COMMANDS
        .lock()
        .unwrap()
        .iter()
        .filter(|cmd| {
            if let TestCommand::Retry(_) = cmd {
                true
            } else {
                false
            }
        })
        .count();

    match payload {
        TestCommand::Success => Ok(()),
        TestCommand::Retry(c) => match retry_count >= c {
            true => Ok(()),
            false => Err(anyhow::anyhow!("Failed, to test retry...")),
        },
    }
}

#[tokio::test]
async fn integration_basic() {
    let job_server = create();
    job_server
        .enqueue(TestCommand::Success)
        .expect("Enqueue job");

    sleep_ms(250).await;

    assert_eq!(1, COMMANDS.lock().unwrap().iter().count());
}

#[tokio::test]
async fn integration_retry() {
    let job_server = create();
    job_server
        .enqueue(TestCommand::Retry(3))
        .expect("Enqueue job");

    sleep_ms(250).await;

    assert_eq!(3, COMMANDS.lock().unwrap().iter().count());
}

async fn sleep_ms(ms: usize) {
    tokio::time::sleep(tokio::time::Duration::from_millis(ms as u64)).await;
}

fn create() -> BackgroundJobServer<AppContext, JobServer<AppContext>> {
    let job_ctx = AppContext {};
    let storage = Redis::new("redis://127.0.0.1/").expect("connect to redis");
    let id = format!(
        "later-test-{}-{}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        uuid::Uuid::new_v4()
    );

    JobServerBuilder::new(
        job_ctx,
        id,
        "amqp://guest:guest@localhost:5672".into(),
        Box::new(storage),
    )
    .with_test_command_handler(handle_command)
    .build()
    .expect("start bg server")
}
