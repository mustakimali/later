use crate::encoder;
use crate::Storage;
use serde::de::DeserializeOwned;
#[async_trait::async_trait]
pub trait StorageIterator {
    async fn get_of_type<T: DeserializeOwned>(&self, key: &str) -> Option<T>;
    // hashset
    async fn push(&self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    async fn trim(&self, range: Box<dyn StorageIter>) -> anyhow::Result<()>;
    async fn scan_range(&self, key: &str) -> Box<dyn StorageIter>;
    async fn del_range(&self, key: &str) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
pub trait StorageIter: Sync + Send {
    fn get_key(&self) -> String;
    fn get_start(&self) -> usize;
    fn get_index(&self) -> usize;

    async fn next(&mut self, storage: &Box<dyn Storage>) -> Option<Vec<u8>>;
    async fn count(&mut self, storage: &Box<dyn Storage>) -> usize;
}

pub(crate) struct ScanRange {
    key: String,
    count: usize,
    start: usize,
    index: usize,
    //parent: &'s Box<dyn Storage>,
}

#[async_trait::async_trait]
impl<T: Storage + ?Sized> StorageIterator for T {
    async fn get_of_type<V: DeserializeOwned>(&self, key: &str) -> Option<V> {
        if let Some(bytes) = self.get(key).await {
            return encoder::decode::<V>(&bytes).ok();
        }

        None
    }

    async fn push(&self, key: &str, value: &[u8]) -> anyhow::Result<()> {
        let count_key = format!("{}-count", key);
        let count = self
            .get_of_type::<i32>(&count_key)
            .await
            .unwrap_or_else(|| 0);

        let key = format!("{}-{}", key, count);

        match self.set(&key, value).await {
            Ok(_) => {
                // store the count
                self.set(&count_key, &encoder::encode(&count + 1)?).await?;

                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    async fn trim(&self, range: Box<dyn StorageIter>) -> anyhow::Result<()> {
        let key = range.get_key();
        let start_key = format!("{}-start", key);
        let idx = range.get_index();

        if idx == range.get_start() {
            return Ok(());
        }

        self.set(&start_key, &encoder::encode(idx)?).await?;

        let start = range.get_start();
        let end = range.get_index();
        for i in start..end {
            let key = get_scan_item_key(&key, i);
            let _ = self.del(&key).await;
        }

        Ok(())
    }
    async fn scan_range(&self, key: &str) -> Box<dyn StorageIter> {
        let start_key = format!("{}-start", key);
        let count_key = format!("{}-count", key);
        let start_from_idx = self
            .get_of_type::<usize>(&start_key)
            .await
            .unwrap_or_else(|| 0);
        let item_in_range = self
            .get_of_type::<usize>(&count_key)
            .await
            .unwrap_or_else(|| 0);

        let scan = ScanRange {
            key: key.to_string(),
            count: item_in_range,
            start: start_from_idx,
            index: start_from_idx,
            //parent: &self,
        };

        Box::new(scan)
    }

    async fn del_range(&self, key: &str) -> anyhow::Result<()> {
        let count_key = format!("{}-count", key);
        let start_key = format!("{}-start", key);
        let start_from_idx = self
            .get_of_type::<usize>(&start_key)
            .await
            .unwrap_or_else(|| 0);
        let item_in_range = self
            .get_of_type::<usize>(&count_key)
            .await
            .unwrap_or_else(|| 0);

        for idx in start_from_idx..item_in_range {
            let item_key = get_scan_item_key(key, idx);
            self.del(&item_key).await?;
        }

        // delete start + count
        self.del(&start_key).await?;
        self.del(&count_key).await?;

        // ToDo: make atomic

        Ok(())
    }
}

#[async_trait::async_trait]
impl StorageIter for ScanRange {
    fn get_index(&self) -> usize {
        self.index
    }

    fn get_start(&self) -> usize {
        self.start
    }

    fn get_key(&self) -> String {
        self.key.clone()
    }

    async fn next(&mut self, storage: &Box<dyn Storage>) -> Option<Vec<u8>> {
        if self.count == 0 || self.index == self.count {
            return None;
        }

        let key = get_scan_item_key(&self.key, self.index);

        let item = storage.get(&key).await;

        if item.is_some() {
            self.index += 1;
        }

        item
    }

    async fn count(&mut self, storage: &Box<dyn Storage>) -> usize {
        let mut count = 0;
        while self.next(storage).await.is_some() {
            count += 1;
        }

        count
    }
}

fn get_scan_item_key(range_key: &str, idx: usize) -> String {
    format!("{}-{}", range_key, idx)
}
