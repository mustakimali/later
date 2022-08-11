use async_std::sync::{Arc, Mutex, MutexGuard};
use later::{BackgroundJobServer, UtcDateTime};
use serde::{Deserialize, Serialize};
use std::{
    ops::Add,
    time::{Duration, SystemTime},
};

pub struct Invocation {
    date: UtcDateTime,
    command: TestCommand,
}

impl Invocation {
    pub fn new(cmd: TestCommand) -> Self {
        Self {
            date: chrono::Utc::now(),
            command: cmd,
        }
    }
}
pub struct AppContext {
    invc: Arc<Mutex<Vec<Invocation>>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestCommand {
    pub name: String,
    pub outcome: Outcome,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Outcome {
    Success,
    Retry(usize),
    Delay(usize /* Delay ms */),
}

later::background_job! {
    struct JobServer {
        test_command: TestCommand,
    }
}

pub async fn create_server(
    invc: Arc<Mutex<Vec<Invocation>>>,
) -> BackgroundJobServer<AppContext, JobServer<AppContext>> {
    let job_ctx = AppContext { invc: invc };
    let storage = later::storage::redis::Redis::new("redis://127.0.0.1")
        .await
        .expect("connect to redis");
    let mq = later::mq::amqp::RabbitMq::new("amqp://guest:guest@localhost:5672".into());
    let name = format!(
        "test-{}{}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        later::generate_id()[16..].to_string()
    );

    JobServerBuilder::new(
        later::Config::builder()
            .name(name)
            .context(job_ctx)
            .storage(Box::new(storage))
            .message_queue_client(Box::new(mq))
            .build(),
    )
    .with_test_command_handler(handle_internal)
    .build()
    .await
    .expect("start bg server")
}

async fn handle_internal(
    ctx: JobServerContext<AppContext>,
    payload: TestCommand,
) -> anyhow::Result<()> {
    let retry_count = {
        let mut invc = ctx.app.invc.lock().await;
        invc.push(Invocation::new(payload.clone()));

        println!("[TEST] Command received {}", payload.name.clone());

        invc.iter()
            .filter(|inv| {
                if let Outcome::Retry(_) = inv.command.outcome {
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
            sleep_ms(delay_ms).await;
            Ok(())
        }
    }
}

pub async fn assert_invocations(expected_num: usize, ty: &str, inv: Arc<Mutex<Vec<Invocation>>>) {
    assert_invocations_with_delay(expected_num, None, ty, inv).await;
}

pub async fn assert_invocations_with_delay(
    expected_num: usize,
    expected_delay: Option<Duration>,
    ty: &str,
    inv: Arc<Mutex<Vec<Invocation>>>,
) {
    let start = SystemTime::now();
    let test_timeout = expected_delay
        .unwrap_or_else(|| Duration::from_secs(10))
        .add(Duration::from_secs(5));

    while SystemTime::now().duration_since(start).unwrap() < test_timeout
        && count_of_invocation_for(ty, &inv.lock().await) != expected_num
    {
        sleep_ms(250).await;
    }

    let invocations = count_of_invocation_for(ty, &inv.lock().await);

    assert_eq!(
        expected_num, invocations,
        "Invocation for {} must be {}",
        ty, expected_num
    );

    if let Some(expected_delay) = expected_delay {
        let delay = SystemTime::now().duration_since(start).unwrap();

        assert!(
            delay.as_millis() > expected_delay.as_millis(),
            "Expected delay of at least {}ms, but was actually {}ms",
            expected_delay.as_millis(),
            delay.as_millis()
        );
    }

    println!("Invocations: {} x {} ... Check", ty, invocations);
}

fn count_of_invocation_for(ty: &str, inv: &MutexGuard<Vec<Invocation>>) -> usize {
    inv.iter().filter(|c| c.command.name == ty).count()
}

async fn sleep_ms(ms: usize) {
    tokio::time::sleep(std::time::Duration::from_millis(ms as u64)).await
}
