use later::BackgroundJobServer;
use serde::{Deserialize, Serialize};
use std::{
    ops::Add,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime},
};

#[derive(Clone)]
pub struct AppContext {}

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
    invc: Arc<Mutex<Vec<TestCommand>>>,
) -> BackgroundJobServer<AppContext, JobServer<AppContext>> {
    let job_ctx = AppContext {};
    let storage = later::storage::redis::Redis::new_cleared("redis://127.0.0.1")
        .await
        .expect("connect to redis");
    let id = format!(
        "test-{}{}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        uuid::Uuid::new_v4().simple().to_string()[28..].to_string()
    );

    JobServerBuilder::new(
        job_ctx,
        id,
        "amqp://guest:guest@localhost:5672".into(),
        Box::new(storage),
    )
    .with_test_command_handler(move |_ctx, payload| handle_internal(payload, invc.clone()))
    .build()
    .expect("start bg server")
}

pub fn handle_internal(
    payload: TestCommand,
    invc: Arc<Mutex<Vec<TestCommand>>>,
) -> anyhow::Result<()> {
    let retry_count = {
        let mut invc = invc.lock().expect("acquire lock to invc");
        invc.push(payload.clone());

        println!("[TEST] Command received {}", payload.name.clone());

        invc.iter()
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

pub async fn assert_invocations(expected_num: usize, ty: &str, inv: Arc<Mutex<Vec<TestCommand>>>) {
    assert_invocations_with_delay(expected_num, None, ty, inv).await;
}

pub async fn assert_invocations_with_delay(
    expected_num: usize,
    expected_delay: Option<Duration>,
    ty: &str,
    inv: Arc<Mutex<Vec<TestCommand>>>,
) {
    let start = SystemTime::now();
    let test_timeout = expected_delay
        .unwrap_or_else(|| Duration::from_secs(3))
        .add(Duration::from_secs(1));

    while SystemTime::now().duration_since(start).unwrap() < test_timeout
        && count_of_invocation_for(ty, &inv.lock().unwrap()) != expected_num
    {
        sleep_ms(250).await;
    }

    let invocations = count_of_invocation_for(ty, &inv.lock().unwrap());

    assert_eq!(
        expected_num, invocations,
        "Invocation for {} must be {}",
        ty, expected_num
    );

    if let Some(expected_delay) = expected_delay {
        let delay = SystemTime::now().duration_since(start).unwrap();

        assert!(
            delay.as_millis() > expected_delay.as_millis(),
            "Expected delay of {}ms but actually {}ms",
            expected_delay.as_millis(),
            delay.as_millis()
        );
    }

    println!("Invocations: {} x {} ... Check", ty, invocations);
}

fn count_of_invocation_for(ty: &str, inv: &MutexGuard<Vec<TestCommand>>) -> usize {
    inv.iter().filter(|c| c.name == ty).count()
}

async fn sleep_ms(ms: usize) {
    tokio::time::sleep(std::time::Duration::from_millis(ms as u64)).await
}
