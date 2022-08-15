pub mod memory;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;

pub(crate) mod storage_iter;

#[cfg(feature = "postgres")]
pub use crate::storage::postgres::Postgres;

#[cfg(feature = "redis")]
pub use crate::storage::redis::Redis;

#[cfg(test)]
mod tests;

pub(crate) use storage_iter::StorageIter;
pub(crate) use storage_iter::StorageEx;

#[async_trait::async_trait]
pub trait Storage: Sync + Send {
    async fn get(&self, key: &str) -> Option<Vec<u8>>;
    async fn set(&self, key: &str, value: &[u8]) -> anyhow::Result<()>;
    async fn del(&self, key: &str) -> anyhow::Result<()>;

    async fn expire(&self, key: &str, ttl_sec: usize) -> anyhow::Result<()>;
}

mod wtf_happening {
    #[async_trait::async_trait]
    pub trait Storage: Sync + Send {
        async fn get(&self, key: &str) -> Option<Vec<u8>>;
    }

    pub struct Postgres;

    #[async_trait::async_trait]
    impl Storage for Postgres {
        async fn get(&self, key: &str) -> Option<Vec<u8>> {
            Some(format!("value of {}", key).as_bytes().to_vec())
        }
    }

    // anything that implements `Storage` should have `get_two` and `iter`.
    #[async_trait::async_trait]
    trait StorageEx: Storage {
        async fn get_two(&self, key1: &str, key2: &str) -> (Option<Vec<u8>>, Option<Vec<u8>>);
        async fn iter(&self) -> Box<dyn Iter + 'life0>;
    }

    #[async_trait::async_trait]
    impl<T: Storage> StorageEx for T {
        async fn get_two(&self, key1: &str, key2: &str) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
            (self.get(key1).await, self.get(key2).await)
        }

        async fn iter(&self) -> Box<dyn Iter + 'life0> {
            let item = IterImpl {
                inner: self,
                remaining: 5,
            };

            Box::new(item)
        }
    }

    // the iter

    #[async_trait::async_trait]
    trait Iter {
        async fn next(&mut self) -> Option<Vec<u8>>;
    }

    struct IterImpl<'s, T: Storage> {
        inner: &'s T,
        remaining: usize,
    }

    #[async_trait::async_trait]
    impl<'s, T: Storage> Iter for IterImpl<'s, T> {
        async fn next(&mut self) -> Option<Vec<u8>> {
            self.remaining -= 1;

            if self.remaining == 0 {
                return None;
            }

            self.inner.get("key-iter").await
        }
    }

    #[tokio::test]
    async fn now_the_meat() {
        let p = Box::new(Postgres {});
        let mut iter = p.iter().await;
        let mut result = Vec::default();
        while let Some(item) = iter.next().await {
            result.push(String::from_utf8(item).unwrap());
        }

        assert_eq!(4, result.len());
    }
}
