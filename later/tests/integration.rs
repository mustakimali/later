#![cfg(feature = "redis")]

use async_std::sync::{Arc, Mutex};
use chrono::Timelike;
use common::*;
use std::time::Duration;

mod common;

#[tokio::test]
async fn integration_basic() {
    let invocations = Arc::new(Mutex::new(Vec::default()));
    let job_server = create_server(invocations.clone()).await;
    job_server
        .enqueue(TestCommand {
            name: "basic".to_string(),
            outcome: Outcome::Success,
        })
        .await
        .expect("Enqueue job");

    assert_invocations(1, "basic", invocations.clone()).await;
}

#[tokio::test]
async fn integration_retry() {
    let invocations = Arc::new(Mutex::new(Vec::default()));
    let job_server = create_server(invocations.clone()).await;
    job_server
        .enqueue(TestCommand {
            name: "retry".to_string(),
            outcome: Outcome::Retry(3),
        })
        .await
        .expect("Enqueue job");

    assert_invocations(3, "retry", invocations.clone()).await;
}

#[tokio::test]
async fn integration_delayed() {
    let invocations = Arc::new(Mutex::new(Vec::default()));
    let job_server = create_server(invocations.clone()).await;
    job_server
        .enqueue_delayed(
            TestCommand {
                name: "delay".to_string(),
                outcome: Outcome::Delay(500),
            },
            Duration::from_secs(5),
        )
        .await
        .expect("Enqueue job");

    assert_invocations_with_delay(
        1,
        Some(Duration::from_secs(5)),
        "delay",
        invocations.clone(),
    )
    .await;
}

#[tokio::test]
async fn integration_continuation_basic() {
    let invocations = Arc::new(Mutex::new(Vec::default()));
    let job_server = create_server(invocations.clone()).await;
    let parent_job_id = job_server
        .enqueue(TestCommand {
            name: "continuation-1".to_string(),
            outcome: Outcome::Delay(100),
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

    assert_invocations(1, "continuation-3", invocations.clone()).await;
    assert_invocations(3, "continuation-2", invocations.clone()).await;
    assert_invocations(1, "continuation-1", invocations.clone()).await;
}

#[tokio::test]
async fn integration_continuation_multiple() {
    let invocations = Arc::new(Mutex::new(Vec::default()));
    let job_server = create_server(invocations.clone()).await;
    let parent_job_id = job_server
        .enqueue(TestCommand {
            name: "continuation-multiple-1".to_string(),
            outcome: Outcome::Delay(250),
        })
        .await
        .expect("Enqueue job");

    let _ = job_server
        .enqueue_continue(
            parent_job_id.clone(),
            TestCommand {
                name: "continuation-multiple-2".to_string(),
                outcome: Outcome::Retry(3),
            },
        )
        .await
        .expect("Enqueue job");

    let _ = job_server
        .enqueue_continue(
            parent_job_id,
            TestCommand {
                name: "continuation-multiple-3".to_string(),
                outcome: Outcome::Success,
            },
        )
        .await
        .expect("Enqueue job");

    println!("--- All job scheduled ---");

    assert_invocations(3, "continuation-multiple-2", invocations.clone()).await;
    assert_invocations(1, "continuation-multiple-3", invocations.clone()).await;
    assert_invocations(1, "continuation-multiple-1", invocations.clone()).await;
}

#[tokio::test]
async fn integration_recurring() {
    let invocations = Arc::new(Mutex::new(Vec::default()));
    let job_server = create_server(invocations.clone()).await;
    let now = chrono::Utc::now();
    let next_min = now.minute() + 1;
    let next_min_str = format!("{}:{}", now.hour(), next_min);
    let sec_left = 60 - now.second();
    println!(
        "This recurring job should execute in {} ({} sec later)",
        next_min_str, sec_left
    );

    job_server
        .enqueue_recurring(
            "recurring-job-1".to_string(),
            TestCommand {
                name: "schedule".to_string(),
                outcome: Outcome::Delay(500),
            },
            format!("{} * * * * *", next_min),
        )
        .await
        .expect("Enqueue job");

    assert_invocations_with_delay(
        1,
        Some(Duration::from_secs((sec_left - 1).into())),
        "schedule",
        invocations.clone(),
    )
    .await;
}

#[tokio::test]
async fn integration_recurring_validates_cron() {
    let invocations = Arc::new(Mutex::new(Vec::default()));
    let job_server = create_server(invocations.clone()).await;

    let result = job_server
        .enqueue_recurring(
            "recurring-job-2".to_string(),
            TestCommand {
                name: "schedule".to_string(),
                outcome: Outcome::Delay(500),
            },
            "invalid_cron".to_string(),
        )
        .await;

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "error parsing cron expression"
    );
}
