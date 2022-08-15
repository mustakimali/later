use super::{Postgres, Redis, StorageIter};
use crate::{
    generate_id,
    storage::{Storage, StorageEx},
};
use test_case::test_case;

async fn create_redis_client() -> Box<dyn Storage> {
    let redis = Redis::new("redis://127.0.0.1/")
        .await
        .expect("connect to redis");

    Box::new(redis)
}

async fn create_postgres_client() -> Box<dyn Storage> {
    let postgres = Postgres::new("postgres://test:test@localhost/later_test")
        .await
        .expect("connect to postgres");

    Box::new(postgres)
}

// ----
#[test_case(create_redis_client().await; "redis")]
#[test_case(create_postgres_client().await; "postgres")]
#[tokio::test]
async fn basic(storage: Box<dyn Storage>) {
    let data = crate::generate_id();
    let my_data = data.as_bytes();

    storage.set("key", my_data).await.expect("set failed");

    let result = storage.get("key").await.expect("get failed");
    assert_eq!(my_data, result);
}

#[test_case(create_redis_client().await; "redis")]
#[test_case(create_postgres_client().await; "postgres")]
#[tokio::test]
async fn range_basic(storage: Box<dyn Storage>) {
    let key = format!("key-{}", crate::generate_id());

    for _ in 0..10 {
        storage
            .push(&key, crate::generate_id().as_bytes())
            .await
            .unwrap();
    }

    let mut scan_result = storage.scan_range(&key).await;
    let count = scan_result.count(&storage).await;

    assert_eq!(10, count);
}

#[test_case(create_redis_client().await; "redis")]
#[test_case(create_postgres_client().await; "postgres")]
#[tokio::test]
async fn range_basic_2_items(storage: Box<dyn Storage>) {
    let key = format!("key-{}", crate::generate_id());

    storage
        .push(&key, "item-1".to_string().as_bytes())
        .await
        .unwrap();

    storage
        .push(&key, "item-2".to_string().as_bytes())
        .await
        .unwrap();

    let mut scan_result = storage.scan_range(&key).await;
    let count = scan_result.count(&storage).await;

    assert_eq!(2, count);

    let mut read_items = Vec::default();
    let mut scan_result = storage.scan_range(&key).await;
    while let Some(item) = scan_result.next(&storage).await {
        read_items.push(String::from_utf8(item).unwrap());
    }

    assert_eq!(2, read_items.len());
    assert_eq!("item-1", read_items[0]);
    assert_eq!("item-2", read_items[1]);
}

#[test_case(create_redis_client().await; "redis")]
#[test_case(create_postgres_client().await; "postgres")]
#[tokio::test]
async fn range_basic_1_item(storage: Box<dyn Storage>) {
    let key = format!("key-{}", crate::generate_id());

    storage
        .push(&key, "item-1".to_string().as_bytes())
        .await
        .unwrap();

    let mut scan_result = storage.scan_range(&key).await;
    let count = scan_result.count(&storage).await;

    assert_eq!(1, count);

    let mut read_items = Vec::default();
    let mut scan_result = storage.scan_range(&key).await;
    while let Some(item) = scan_result.next(&storage).await {
        read_items.push(String::from_utf8(item).unwrap());
    }

    assert_eq!(1, read_items.len());
    assert_eq!("item-1", read_items[0]);
}

#[test_case(create_redis_client().await; "redis")]
#[test_case(create_postgres_client().await; "postgres")]
#[tokio::test]
async fn range_trim(storage: Box<dyn Storage>) {
    let key = format!("key-{}", crate::generate_id());

    for idx in 0..100 {
        storage
            .push(&key, idx.to_string().as_bytes())
            .await
            .unwrap();
    }

    assert_eq!(100, storage.scan_range(&key).await.count(&storage).await);

    // scan first 50
    let mut range = storage.scan_range(&key).await;
    let mut counter = 0;
    while counter < 50 {
        let next_item = range.next(&storage).await;
        assert!(next_item.is_some());

        counter += 1;
    }

    // trim
    let _ = storage.trim(range).await;

    // should have only 50
    let mut range = storage.scan_range(&key).await;
    assert_eq!(50, range.count(&storage).await);

    // should be empty
    let mut range = storage.scan_range(&key).await;
    while range.next(&storage).await.is_some() {}
    let _ = storage.trim(range).await;

    assert_eq!(0, storage.scan_range(&key).await.count(&storage).await); // should be empty
}

#[test_case(create_redis_client().await; "redis")]
#[test_case(create_postgres_client().await; "postgres")]
#[tokio::test]
async fn range_trim_10_items(storage: Box<dyn Storage>) {
    let key = format!("key-{}", crate::generate_id());

    for i in 1..11 {
        // creates item-1 ... item-10
        storage
            .push(&key, format!("item-{}", i).as_bytes())
            .await
            .unwrap();
    }

    let mut range = storage.scan_range(&key).await;
    assert!(range.next(&storage).await.is_some());
    assert!(range.next(&storage).await.is_some());
    assert!(range.next(&storage).await.is_some());
    assert!(range.next(&storage).await.is_some());
    assert!(range.next(&storage).await.is_some());

    storage.trim(range).await.unwrap();

    let mut range = storage.scan_range(&key).await;
    assert_eq!(
        "item-6",
        &String::from_utf8(range.next(&storage).await.unwrap()).unwrap()
    );
    assert_eq!(
        "item-7",
        &String::from_utf8(range.next(&storage).await.unwrap()).unwrap()
    );
    assert_eq!(
        "item-8",
        &String::from_utf8(range.next(&storage).await.unwrap()).unwrap()
    );
    assert_eq!(
        "item-9",
        &String::from_utf8(range.next(&storage).await.unwrap()).unwrap()
    );
    assert_eq!(
        "item-10",
        &String::from_utf8(range.next(&storage).await.unwrap()).unwrap()
    );
    assert!(range.next(&storage).await.is_none());
}

#[test_case(create_redis_client().await; "redis")]
#[test_case(create_postgres_client().await; "postgres")]
#[tokio::test]
async fn scan_reverse(storage: Box<dyn Storage>) {
    let key = format!("key-{}", generate_id());

    for i in 1..5 {
        // creates item-1 ... item-4
        storage
            .push(&key, format!("item-{}", i).as_bytes())
            .await
            .expect("push");
    }

    let mut range = storage.scan_range_reverse(&key).await; // start from reverse
    let remaining_items = read_all_string(&mut range, &storage).await;
    assert_eq!(remaining_items, &["item-4", "item-3", "item-2", "item-1"]);
}

#[test_case(create_redis_client().await; "redis")]
#[test_case(create_postgres_client().await; "postgres")]
#[tokio::test]
async fn scan_reverse_trim(storage: Box<dyn Storage>) {
    let key = format!("key-{}", generate_id());

    for i in 1..5 {
        // creates item-1 ... item-4
        storage
            .push(&key, format!("item-{}", i).as_bytes())
            .await
            .expect("push");
    }

    let mut range = storage.scan_range_reverse(&key).await; // start from reverse

    assert_eq!(
        "item-4".to_string(),
        String::from_utf8(range.next(&storage).await.unwrap()).unwrap()
    );
    assert_eq!(
        "item-3".to_string(),
        String::from_utf8(range.next(&storage).await.unwrap()).unwrap()
    );

    storage.trim(range).await.unwrap();

    let mut range = storage.scan_range_reverse(&key).await; // start from reverse
    let remaining_items = read_all_string(&mut range, &storage).await;
    assert_eq!(remaining_items, &["item-2", "item-1"]);
}

#[test_case(create_redis_client().await; "redis")]
#[test_case(create_postgres_client().await; "postgres")]
#[tokio::test]
async fn scan_del_first_item(storage: Box<dyn Storage>) {
    let key = format!("key-{}", generate_id());

    for i in 1..5 {
        // creates item-1 ... item-4
        storage
            .push(&key, format!("item-{}", i).as_bytes())
            .await
            .expect("push");
    }

    let mut range = storage.scan_range(&key).await;
    range.del(&storage).await;

    let remaining_items = read_all_string(&mut range, &storage).await;
    assert_eq!(remaining_items, &["item-2", "item-3", "item-4"]);
}

#[test_case(create_redis_client().await; "redis")]
#[test_case(create_postgres_client().await; "postgres")]
#[tokio::test]
async fn scan_del_second_item(storage: Box<dyn Storage>) {
    let key = format!("key-{}", generate_id());

    for i in 1..5 {
        // creates item-1 ... item-4
        storage
            .push(&key, format!("item-{}", i).as_bytes())
            .await
            .expect("push");
    }

    let mut range = storage.scan_range(&key).await;
    assert!(range.next(&storage).await.is_some());

    range.del(&storage).await;

    let mut range = storage.scan_range(&key).await; // start from beginning
    let remaining_items = read_all_string(&mut range, &storage).await;
    assert_eq!(remaining_items, &["item-1", "item-3", "item-4"]);
}

async fn read_all_string(
    range: &mut Box<dyn StorageIter>,
    storage: &Box<dyn Storage>,
) -> Vec<String> {
    let mut result = Vec::default();

    while let Some(item) = range.next(storage).await {
        result.push(String::from_utf8(item).expect("read string from range"));
    }

    result
}
