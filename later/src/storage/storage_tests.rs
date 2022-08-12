macro_rules! storage_tests {
    () => {
        use crate::storage::StorageIterator;

        #[tokio::test]
        async fn basic() {
            let data = crate::generate_id();
            let my_data = data.as_bytes();
            let storage = create_client().await;
            storage.set("key", my_data).await.expect("set failed");

            let result = storage.get("key").await.expect("get failed");
            assert_eq!(my_data, result);
        }

        #[tokio::test]
        async fn range_basic() {
            let key = format!("key-{}", crate::generate_id());

            let storage = create_client().await;

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

        #[tokio::test]
        async fn range_basic_2_items() {
            let key = format!("key-{}", crate::generate_id());

            let storage = create_client().await;

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

        #[tokio::test]
        async fn range_basic_1_item() {
            let key = format!("key-{}", crate::generate_id());

            let storage = create_client().await;

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

        #[tokio::test]
        async fn range_trim() {
            let key = format!("key-{}", crate::generate_id());

            let storage = create_client().await;

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

        #[tokio::test]
        async fn range_trim_10_items() {
            let key = format!("key-{}", crate::generate_id());

            let storage = create_client().await;

            storage.push(&key, "item-1".as_bytes()).await.unwrap();
            storage.push(&key, "item-2".as_bytes()).await.unwrap();
            storage.push(&key, "item-3".as_bytes()).await.unwrap();
            storage.push(&key, "item-4".as_bytes()).await.unwrap();
            storage.push(&key, "item-5".as_bytes()).await.unwrap();
            storage.push(&key, "item-6".as_bytes()).await.unwrap();
            storage.push(&key, "item-7".as_bytes()).await.unwrap();
            storage.push(&key, "item-8".as_bytes()).await.unwrap();
            storage.push(&key, "item-9".as_bytes()).await.unwrap();
            storage.push(&key, "item-10".as_bytes()).await.unwrap();

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
        }
    };
}

pub(crate) use storage_tests;
