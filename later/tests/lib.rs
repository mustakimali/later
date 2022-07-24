use later::storage::redis::Redis;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

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

    dbg!(retry_count);
    match payload {
        TestCommand::Success => Ok(()),
        TestCommand::Retry(c) => match retry_count >= c {
            true => Ok(()),
            false => Err(anyhow::anyhow!("Retrying...")),
        },
    }
}

#[test]
fn integration_retry() {
    let job_ctx = AppContext {};
    let storage = Redis::new("redis://127.0.0.1/").expect("connect to redis");
    let bjs = JobServerBuilder::new(
        job_ctx,
        format!("later-test-{}", uuid::Uuid::new_v4()),
        "amqp://guest:guest@localhost:5672".into(),
        Box::new(storage),
    )
    .with_test_command_handler(handle_command)
    .build()
    .expect("start bg server");

    bjs.enqueue(TestCommand::Retry(3)).expect("Enqueue job");

    std::thread::sleep_ms(1000);

    assert_eq!(5, COMMANDS.lock().unwrap().iter().count());
}
