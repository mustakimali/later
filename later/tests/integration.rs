use std::{sync::Mutex, time::SystemTime};

use later::{storage::redis::Redis, BackgroundJobServer};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

lazy_static! {
    static ref COMMANDS: Mutex<Vec<TestCommand>> = Mutex::new(Vec::default());
}

#[derive(Clone)]
struct AppContext {}

later::background_job! {
    struct JobServer {
        test_command: TestCommand,
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestCommand {
    name: String,
    outcome: Outcome,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Outcome {
    Success,
    Retry(usize),
}

fn handle_command(_ctx: &JobServerContext<AppContext>, payload: TestCommand) -> anyhow::Result<()> {
    let retry_count = {
        COMMANDS.lock().unwrap().push(payload.clone());

        COMMANDS
            .lock()
            .unwrap()
            .iter()
            .filter(|cmd| {
                if let Outcome::Retry(_) = cmd.outcome {
                    true
                } else {
                    false
                }
            })
            .count()
    };

    match payload.outcome {
        Outcome::Success => Ok(()),
        Outcome::Retry(c) => match retry_count >= c {
            true => Ok(()),
            false => Err(anyhow::anyhow!("Failed, to test retry...")),
        },
    }
}

#[test]
fn integration_basic() {
    let job_server = create();
    job_server
        .enqueue(TestCommand {
            name: "basic".to_string(),
            outcome: Outcome::Success,
        })
        .expect("Enqueue job");

    sleep_ms(250);

    assert_eq!(1, count("basic"));
}

#[test]
fn integration_retry() {
    let job_server = create();
    job_server
        .enqueue(TestCommand {
            name: "retry".to_string(),
            outcome: Outcome::Retry(3),
        })
        .expect("Enqueue job");

    sleep_ms(2000);

    assert_eq!(3, count("retry"));
}

fn count(ty: &str) -> usize {
    COMMANDS
        .lock()
        .unwrap()
        .iter()
        .filter(|c| c.name == ty)
        .count()
}

fn sleep_ms(ms: usize) {
    std::thread::sleep(std::time::Duration::from_millis(ms as u64));
}

fn create() -> BackgroundJobServer<AppContext, JobServer<AppContext>> {
    let job_ctx = AppContext {};
    let storage = Redis::new_cleared("redis://127.0.0.1").expect("connect to redis");
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
