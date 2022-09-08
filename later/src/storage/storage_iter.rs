use crate::encoder;
use crate::encoder::encode;
use crate::Storage;
use serde::de::DeserializeOwned;
#[async_trait::async_trait]
/// Extra functionality for all storage implementations.
pub trait StorageEx {
    async fn get_of_type<T: DeserializeOwned>(&self, key: &str) -> Option<T>;

    // hashset
    /// insert an item into a hahset keyed by `key`. Hashet will be created if this is the first call.
    ///
    /// ## Basic of Hashset
    /// * Duplicate item will be ignored.
    /// * Hashset can be iterated forward or reverse using [`scan_range`] or [`scan_range_reverse`].
    /// * An entire hashset can be deleted using `del_range`.
    /// * At any point during the iteration, calling the [`trim`] will remove already read items.
    ///
    async fn push(&self, key: &str, value: &[u8]) -> anyhow::Result<()>;

    /// Given an iteration, all read items will be removed.
    ///
    /// This works regardless of forward or backward read iterator.
    async fn trim(&self, range: Box<dyn StorageIter>) -> anyhow::Result<()>;

    /// Creates an iterator that read all items in similar order they were added into the hashset.
    ///
    /// Note: Deleted items are filled with the first item to keep
    /// the scanning consistent regardless of the size of the hashset.
    /// Therefore the scan order won't be strictly maintained.
    async fn scan_range(&self, key: &str) -> Box<dyn StorageIter>;

    /// Creates an iterator starting from the item with the given value
    async fn scan_range_from(&self, key: &str, value: &[u8]) -> Option<Box<dyn StorageIter>>;

    /// Creates an iterator that read all items in reverse order (newest to oldest)
    ///
    /// See note in [`scan_range`].
    async fn scan_range_reverse(&self, key: &str) -> Box<dyn StorageIter>;

    /// Deletes an entire hashset with all items.
    async fn del_range(&self, key: &str) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
/// Iterator for a hashset that works with any [`storage`] implementation.
pub trait StorageIter: Sync + Send {
    fn get_key(&self) -> String;
    /// start pointer
    fn get_start(&self) -> usize;
    /// current pointer
    fn get_index(&self) -> usize;
    /// end poin&ter
    fn get_end(&self) -> usize;

    fn is_scanning_reverse(&self) -> bool;

    /// Skip some items
    fn skip(&mut self, item: usize);

    /// return the next item if available.
    async fn next(&mut self, storage: &Box<dyn Storage>) -> Option<Vec<u8>>;

    /// delete an item from the hashset.
    /// This replace the current item with the first item and moves
    /// the starts pointer by one.
    async fn del(&mut self, storage: &Box<dyn Storage>);

    /// Returns the total element in this hashset.
    ///
    /// Note: this does not take into account any items that
    /// may have been added since the iterator was created.
    /// In order to get the true count you must scan the entire set
    /// by calling `next`.
    async fn count(&self) -> usize;
}

pub(crate) struct ScanRange {
    key: String,
    end: usize,
    start: usize,
    index: usize,
    scan_forward: bool,
    exhausted: bool,
}

impl ScanRange {
    fn from_range_with_index(range: Box<dyn StorageIter>, index: usize) -> ScanRange {
        ScanRange {
            end: range.get_end(),
            start: range.get_start(),
            scan_forward: !range.is_scanning_reverse(),
            exhausted: false,
            key: range.get_key(),
            index,
        }
    }
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
        push_internal(self, key, value, None).await
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
            true => (range.get_index() + 1, range.get_end()),
            false => (range.get_start(), range.get_index()),
        };

        for i in start..end {
            let _ = del_range_item(self, &key, i).await;
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
        let item_in_range = get_count_of_atomic_key(self, &count_key)
            .await
            .unwrap_or_else(|_| 0);

        for idx in start_from_idx..item_in_range {
            let _ = del_range_item(self, key, idx).await;
        }

        // delete start + count
        self.del(&start_key).await?;
        self.del(&count_key).await?;

        // ToDo: make atomic

        Ok(())
    }

    async fn scan_range_from(&self, key: &str, value: &[u8]) -> Option<Box<dyn StorageIter>> {
        // get the idx
        let hash = encoder::hash(value);
        let idx_by_hash_key = format!("{}-{}", key, hash);

        if let Some(index) = self.get(&idx_by_hash_key).await {
            if let Ok(index) = encoder::decode::<i32>(&index) {
                let range = self.scan_range(key).await;
                return Some(Box::new(ScanRange::from_range_with_index(
                    range,
                    index as usize,
                )));
            }
        }

        None
    }
}
async fn extend_hashset_get_index<T: Storage + ?Sized>(
    storage: &T,
    key: &str,
) -> anyhow::Result<i32> {
    let count_key = format!("{}-count", key);
    let index = storage.atomic_incr(&count_key, 1).await?;

    Ok(index as i32)
}

async fn get_count_of_atomic_key<T: Storage + ?Sized>(
    storage: &T,
    key: &str,
) -> anyhow::Result<usize> {
    storage.atomic_incr(key, 0).await
}

async fn push_internal<T: Storage + ?Sized>(
    storage: &T,
    key: &str,
    value: &[u8],
    index: Option<i32>,
) -> anyhow::Result<()> {
    let hash = encoder::hash(value);
    let index_by_hash_key = format!("{}-{}", key, hash);
    let append_at_bottom = index.is_none();

    if append_at_bottom && storage.exist(&index_by_hash_key).await? {
        // already exist
        return Ok(());
    }

    let index = if let Some(index) = index {
        index
    } else {
        extend_hashset_get_index(storage, key).await?
    };

    let key = format!("{}-{}", key, index);
    let hash_by_index_key = format!("{}-hash", key);

    match storage.set(&key, value).await {
        Ok(_) => {
            // find index by hash
            storage.set(&index_by_hash_key, &encode(index)?).await?;
            storage.set(&hash_by_index_key, &encode(hash)?).await?;

            Ok(())
        }
        Err(e) => {
            // the range has been extended already, acceptable tradeoff
            Err(e)
        }
    }
}

async fn del_range_item<T: Storage + ?Sized>(
    storage: &T,
    key: &str,
    index: usize,
) -> anyhow::Result<()> {
    let item_key = get_scan_item_key(&key, index);
    let _ = storage.del(&item_key).await;

    let hash_key = &format!("{}-hash", item_key);
    if let Some(hash_val) = storage.get(&hash_key).await {
        if let Ok(hash_str) = encoder::decode::<String>(&hash_val) {
            let index_by_hash_key = format!("{}-{}", key, hash_str);
            let _ = storage.del(&index_by_hash_key).await;
        }
    }

    let _ = storage.del(&hash_key).await;

    Ok(())
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
        .unwrap_or_else(|| 1);
    let item_in_range = get_count_of_atomic_key(storage, &count_key)
        .await
        .unwrap_or_else(|_| 0);

    let scan = ScanRange {
        key: key.to_string(),
        end: item_in_range,
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

    fn get_end(&self) -> usize {
        self.end
    }

    fn get_key(&self) -> String {
        self.key.clone()
    }

    fn is_scanning_reverse(&self) -> bool {
        !self.scan_forward
    }

    fn skip(&mut self, item: usize) {
        self.index = if self.scan_forward {
            self.index + item
        } else {
            self.index.checked_sub(item).unwrap_or_else(|| 0)
        };
    }

    async fn next(&mut self, storage: &Box<dyn Storage>) -> Option<Vec<u8>> {
        loop {
            if self.exhausted || self.scan_forward && (self.end == 0 || self.index > self.end) {
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
        let key_to_be_deleted = get_scan_item_key(&self.key, self.index);
        tracing::debug!("Delete Key: {}", key_to_be_deleted);

        if self.index == self.start {
            self.shift_one(&storage).await;
            let _ = del_range_item(storage.as_ref(), &self.key, self.index).await;
            return;
        }

        let key_first_item = get_scan_item_key(&self.key, self.start);
        if let Some(first_item) = storage.get(&key_first_item).await {
            let _ = del_range_item(storage.as_ref(), &self.key, self.index).await;
            let _ = push_internal(
                storage.as_ref(),
                &self.key,
                &first_item,
                Some(self.index as i32),
            )
            .await;
            self.shift_one(&storage).await;
        }
    }

    async fn count(&self) -> usize {
        (self.end - self.start) + 1
    }
}

fn get_scan_item_key(range_key: &str, idx: usize) -> String {
    format!("{}-{}", range_key, idx)
}
impl ScanRange {
    async fn shift_one(&mut self, storage: &Box<dyn Storage>) {
        let start_key = format!("{}-start", self.key);
        if let Ok(new_start) = encoder::encode(self.start + 1) {
            let _ = storage.set(&start_key, &new_start).await;
            self.start += 1;
        }
    }
}
