use std::{sync::Mutex, time::SystemTime};

use later::BackgroundJobServer;
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
    Delay(usize /* Delay ms */),
}

fn handle_command(_ctx: &JobServerContext<AppContext>, payload: TestCommand) -> anyhow::Result<()> {
    let retry_count = {
        COMMANDS.lock().unwrap().push(payload.clone());

        println!("[TEST] Command received {}", payload.name.clone());

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
        Outcome::Delay(delay_ms) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            rt.block_on(sleep_ms(delay_ms));
            Ok(())
        }
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn integration_basic() {
    let job_server = create().await;
    job_server
        .enqueue(TestCommand {
            name: "basic".to_string(),
            outcome: Outcome::Success,
        })
        .await
        .expect("Enqueue job");

    assert_invocations(1, "basic").await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn integration_retry() {
    let job_server = create().await;
    job_server
        .enqueue(TestCommand {
            name: "retry".to_string(),
            outcome: Outcome::Retry(3),
        })
        .await
        .expect("Enqueue job");

    assert_invocations(3, "retry").await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn integration_continuation() {
    let job_server = create().await;
    let parent_job_id = job_server
        .enqueue(TestCommand {
            name: "continuation-1".to_string(),
            outcome: Outcome::Delay(250),
        })
        .await
        .expect("Enqueue job");

    let child_job_1 = job_server
        .enqueue_continue(
            parent_job_id,
            TestCommand {
                name: "continuation-2".to_string(),
                outcome: Outcome::Retry(3),
            },
        )
        .await
        .expect("Enqueue job");

    let _ = job_server
        .enqueue_continue(
            child_job_1,
            TestCommand {
                name: "continuation-3".to_string(),
                outcome: Outcome::Success,
            },
        )
        .await
        .expect("Enqueue job");

    println!("--- All job scheduled ---");

    assert_invocations(1, "continuation-3").await;
    assert_invocations(3, "continuation-2").await;
    assert_invocations(1, "continuation-1").await;
}

fn count_of_invocation(ty: &str) -> usize {
    COMMANDS
        .lock()
        .unwrap()
        .iter()
        .filter(|c| c.name == ty)
        .count()
}

#[cfg(feature = "postgres")]
async fn create() -> BackgroundJobServer<AppContext, JobServer<AppContext>> {
    let job_ctx = AppContext {};
    let storage = later::storage::redis::Redis::new_cleared("redis://127.0.0.1")
        .await
        .expect("connect to redis");
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

async fn assert_invocations(expected_num: usize, ty: &str) {
    let start = SystemTime::now();
    while SystemTime::now().duration_since(start).unwrap().as_millis() < 3000
        && count_of_invocation(ty) != expected_num
    {
        sleep_ms(250).await;
    }

    let invocations = count_of_invocation(ty);
    assert_eq!(expected_num, invocations);

    println!("Invocations: {} x {} ... Check", ty, invocations);
}

async fn sleep_ms(ms: usize) {
    tokio::time::sleep(std::time::Duration::from_millis(ms as u64)).await
}
