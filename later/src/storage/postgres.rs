use super::Storage;
use sqlx::{postgres::PgPoolOptions, Pool};

pub struct Postgres {
    pool: Pool<sqlx::Postgres>,
}

impl Postgres {
    /// Create a new Postgre storage
    ///
    /// **Example `connection_string` format**: `"postgres://postgres:password@localhost/test"`
    pub async fn new(connection_string: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(connection_string)
            .await?;

        sqlx::migrate!("./migrations").run(&pool).await?;

        Ok(Self { pool })
    }
}

struct Item {
    //key: String,
    value: Vec<u8>,
    // update_count: i32,
    // date_created: crate::UtcDateTime,
    // date_updated: Option<crate::UtcDateTime>,
    // date_expire: Option<crate::UtcDateTime>,
}

#[async_trait::async_trait]
impl Storage for Postgres {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        sqlx::query_as!(
            Item,
            "SELECT value FROM later_storage WHERE key = $1 AND date_expire IS NULL",
            key
        )
        .fetch_one(&self.pool)
        .await
        .ok()
        .map(|item| item.value)
    }

    async fn set(&self, key: &str, value: &[u8]) -> anyhow::Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO later_storage (key, value, update_count, date_created)
            VALUES($1, $2, 0, $3) ON CONFLICT (key) DO UPDATE
            SET value = $2, date_updated = $3, update_count = later_storage.update_count + 1, date_expire = null
        "#,
            key,
            value,
            chrono::Utc::now()
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn del(&self, key: &str) -> anyhow::Result<()> {
        sqlx::query!(
            r#"
            DELETE FROM later_storage WHERE key = $1
        "#,
            key
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn expire(&self, key: &str, ttl_sec: usize) -> anyhow::Result<()> {
        let expire_utc = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::from_std(std::time::Duration::from_secs(
                ttl_sec.try_into()?,
            ))?)
            .ok_or(anyhow::anyhow!("error"))?;

        sqlx::query!(
            r#"
            UPDATE later_storage SET date_expire = $2 WHERE key = $1
        "#,
            key,
            expire_utc
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn exist(&self, key: &str) -> anyhow::Result<bool> {
        let result = sqlx::query!(
            r#"SELECT COUNT(*) as count FROM later_storage WHERE key = $1"#,
            key
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(result.count.unwrap_or(0) > 0)
    }
}
