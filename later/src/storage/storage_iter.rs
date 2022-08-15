use crate::encoder;
use crate::Storage;
use serde::de::DeserializeOwned;
#[async_trait::async_trait]
pub trait StorageEx {
    async fn get_of_type<T: DeserializeOwned>(&self, key: &str) -> Option<T>;
    // hashset
    async fn push(&self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    async fn trim(&self, range: Box<dyn StorageIter>) -> anyhow::Result<()>;
    async fn scan_range(&self, key: &str) -> Box<dyn StorageIter>;
    async fn scan_range_reverse(&self, key: &str) -> Box<dyn StorageIter>;
    async fn del_range(&self, key: &str) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
pub trait StorageIter: Sync + Send {
    fn get_key(&self) -> String;
    fn get_start(&self) -> usize;
    fn get_index(&self) -> usize;
    fn get_count(&self) -> usize;

    fn is_scanning_reverse(&self) -> bool;

    async fn next(&mut self, storage: &Box<dyn Storage>) -> Option<Vec<u8>>;
    async fn del(&mut self, storage: &Box<dyn Storage>);
    async fn count(&mut self, storage: &Box<dyn Storage>) -> usize;
}

pub(crate) struct ScanRange {
    key: String,
    count: usize,
    start: usize,
    index: usize,
    scan_forward: bool,
    exhausted: bool,
}

#[async_trait::async_trait]
impl<T: Storage + ?Sized> StorageEx for T {
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

        let (start, end) = match range.is_scanning_reverse() {
            true => (range.get_index() + 1, range.get_count()),
            false => (range.get_start(), range.get_index()),
        };

        for i in start..end {
            let key = get_scan_item_key(&key, i);
            let _ = self.del(&key).await;
        }

        Ok(())
    }

    async fn scan_range_reverse(&self, key: &str) -> Box<dyn StorageIter> {
        scan_range(self, key, false).await
    }

    async fn scan_range(&self, key: &str) -> Box<dyn StorageIter> {
        scan_range(self, key, true).await
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

async fn scan_range<T: Storage + ?Sized>(
    storage: &T,
    key: &str,
    forward: bool,
) -> Box<dyn StorageIter> {
    let start_key = format!("{}-start", key);
    let count_key = format!("{}-count", key);
    let start_from_idx = storage
        .get_of_type::<usize>(&start_key)
        .await
        .unwrap_or_else(|| 0);
    let item_in_range = storage
        .get_of_type::<usize>(&count_key)
        .await
        .unwrap_or_else(|| 0);

    let scan = ScanRange {
        key: key.to_string(),
        count: item_in_range,
        start: start_from_idx,
        index: match forward {
            true => start_from_idx,
            false => item_in_range,
        },
        scan_forward: forward,
        exhausted: false,
    };

    Box::new(scan)
}

#[async_trait::async_trait]
impl StorageIter for ScanRange {
    fn get_index(&self) -> usize {
        self.index
    }

    fn get_start(&self) -> usize {
        self.start
    }

    fn get_count(&self) -> usize {
        self.count
    }

    fn get_key(&self) -> String {
        self.key.clone()
    }

    fn is_scanning_reverse(&self) -> bool {
        !self.scan_forward
    }

    async fn next(&mut self, storage: &Box<dyn Storage>) -> Option<Vec<u8>> {
        loop {
            if self.exhausted || self.scan_forward && (self.count == 0 || self.index >= self.count)
            {
                return None;
            }

            let key = get_scan_item_key(&self.key, self.index);

            let item = storage.get(&key).await;

            if self.scan_forward {
                self.index += 1;
            } else if self.index == 0 {
                self.exhausted = true;
            } else {
                self.index -= 1;
            }

            if item.is_some() {
                return item;
            }
            // try until reach the end of the range
        }
    }

    async fn del(&mut self, storage: &Box<dyn Storage>) {
        let key = get_scan_item_key(&self.key, self.index);
        let _ = storage.del(&key).await;
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
