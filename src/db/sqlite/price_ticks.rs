use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use lnm_sdk::rest::v3::models::LastPrice;
use sqlx::{QueryBuilder, Sqlite, SqlitePool};

use crate::db::{
    error::{DbError, Result},
    models::PriceTickRow,
    repositories::{PriceTicksRepository, PriceTicksRepositoryRead},
};

pub(crate) struct SqlitePriceTicksRepo {
    pool: Arc<SqlitePool>,
}

impl SqlitePriceTicksRepo {
    pub(crate) fn new(pool: Arc<SqlitePool>) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &SqlitePool {
        self.pool.as_ref()
    }
}

#[async_trait]
impl PriceTicksRepositoryRead for SqlitePriceTicksRepo {
    async fn get_latest_entry(&self) -> Result<Option<(DateTime<Utc>, f64)>> {
        struct PriceEntry {
            pub time: DateTime<Utc>,
            pub price: f64,
        }

        let last_tick_opt = sqlx::query_as!(
            PriceEntry,
            r#"
                SELECT
                    time as "time!: DateTime<Utc>",
                    last_price as "price!: f64"
                FROM price_ticks
                ORDER BY time DESC
                LIMIT 1
            "#
        )
        .fetch_optional(self.pool())
        .await
        .map_err(DbError::Query)?;

        let last_candle_opt = sqlx::query_as!(
            PriceEntry,
            r#"
                SELECT
                    time as "time!: DateTime<Utc>",
                    close as "price!: f64"
                FROM ohlc_candles
                ORDER BY time DESC
                LIMIT 1
            "#
        )
        .fetch_optional(self.pool())
        .await
        .map_err(DbError::Query)?;

        // Prefer candle over tick when times are equal, since candles are minute-floored.
        let latest_entry = [last_tick_opt, last_candle_opt]
            .into_iter()
            .flatten()
            .max_by_key(|entry| entry.time)
            .map(|entry| (entry.time, entry.price));

        Ok(latest_entry)
    }

    async fn get_price_range_from(
        &self,
        start: DateTime<Utc>,
    ) -> Result<Option<(f64, f64, DateTime<Utc>, f64)>> {
        struct PriceEntry {
            pub time: DateTime<Utc>,
            pub price: f64,
        }

        let tick_entries = sqlx::query_as!(
            PriceEntry,
            r#"
                SELECT
                    time as "time!: DateTime<Utc>",
                    last_price as "price!: f64"
                FROM price_ticks
                WHERE time >= ?1
                ORDER BY time ASC
            "#,
            start
        )
        .fetch_all(self.pool())
        .await
        .map_err(DbError::Query)?;

        struct OhlcCandlePartial {
            pub time: DateTime<Utc>,
            pub high: f64,
            pub low: f64,
            pub close: f64,
        }

        let candle_entries = sqlx::query_as!(
            OhlcCandlePartial,
            r#"
                SELECT
                    time as "time!: DateTime<Utc>",
                    high as "high!: f64",
                    low as "low!: f64",
                    close as "close!: f64"
                FROM ohlc_candles
                WHERE time >= ?1
                ORDER BY time ASC
            "#,
            start
        )
        .fetch_all(self.pool())
        .await
        .map_err(DbError::Query)?;

        if tick_entries.is_empty() && candle_entries.is_empty() {
            return Ok(None);
        }

        let mut min_price = f64::INFINITY;
        let mut max_price = f64::NEG_INFINITY;

        for entry in &tick_entries {
            let entry_price = entry.price;
            if entry_price < min_price {
                min_price = entry_price;
            }
            if entry_price > max_price {
                max_price = entry_price;
            }
        }

        for candle in &candle_entries {
            if candle.low < min_price {
                min_price = candle.low;
            }
            if candle.high > max_price {
                max_price = candle.high;
            }
        }

        let last_tick_opt = tick_entries.into_iter().last();

        let last_candle_opt = candle_entries.into_iter().last().map(|c| PriceEntry {
            time: c.time,
            price: c.close,
        });

        // Prefer candle over tick when times are equal, since candles are minute-floored.
        let latest_entry = [last_tick_opt, last_candle_opt]
            .into_iter()
            .flatten()
            .max_by_key(|entry| entry.time)
            .expect("at least one entry exists");

        Ok(Some((
            min_price,
            max_price,
            latest_entry.time,
            latest_entry.price,
        )))
    }
}

#[async_trait]
impl PriceTicksRepository for SqlitePriceTicksRepo {
    async fn add_ticks(&self, ticks: &[LastPrice]) -> Result<Vec<PriceTickRow>> {
        if ticks.is_empty() {
            return Ok(Vec::new());
        }

        let mut query_builder =
            QueryBuilder::<Sqlite>::new("INSERT INTO price_ticks (time, last_price) ");

        query_builder.push_values(ticks, |mut row, tick| {
            row.push_bind(tick.time())
                .push_bind(tick.last_price().as_f64());
        });

        query_builder.push(" ON CONFLICT (time) DO NOTHING RETURNING time, last_price, created_at");

        let inserted = query_builder
            .build_query_as::<(DateTime<Utc>, f64, DateTime<Utc>)>()
            .fetch_all(self.pool())
            .await
            .map_err(DbError::Query)?;

        Ok(inserted
            .into_iter()
            .map(|(time, last_price, created_at)| PriceTickRow {
                time,
                last_price,
                created_at,
            })
            .collect())
    }

    async fn remove_ticks(&self, before: DateTime<Utc>) -> Result<()> {
        sqlx::query!("DELETE FROM price_ticks WHERE time <= ?1", before)
            .execute(self.pool())
            .await
            .map_err(DbError::Query)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;

    async fn repo() -> SqlitePriceTicksRepo {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        sqlx::migrate!("./migrations/sqlite")
            .run(&pool)
            .await
            .unwrap();

        SqlitePriceTicksRepo::new(Arc::new(pool))
    }

    fn tick(year: i32, minute: u32, price: f64) -> LastPrice {
        serde_json::from_value(json!({
            "time": Utc.with_ymd_and_hms(year, 1, 1, 0, minute, 0).unwrap(),
            "lastPrice": price,
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn add_ticks_returns_inserted_rows_and_ignores_duplicates() {
        let repo = repo().await;
        let ticks = [tick(2026, 0, 100.0), tick(2026, 1, 120.5)];

        let inserted = repo.add_ticks(&ticks).await.unwrap();
        assert_eq!(inserted.len(), 2);
        assert_eq!(inserted[0].time, ticks[0].time());
        assert_eq!(inserted[0].last_price, 100.0);
        assert_eq!(inserted[1].time, ticks[1].time());
        assert_eq!(inserted[1].last_price, 120.5);

        let duplicate_inserted = repo.add_ticks(&ticks[..1]).await.unwrap();
        assert!(duplicate_inserted.is_empty());

        let latest = repo.get_latest_entry().await.unwrap();
        assert_eq!(latest, Some((ticks[1].time(), 120.5)));
    }

    #[tokio::test]
    async fn price_range_combines_ticks_and_candles() {
        let repo = repo().await;
        let ticks = [tick(2026, 0, 100.0), tick(2026, 1, 120.5)];
        repo.add_ticks(&ticks).await.unwrap();

        let candle_time = Utc.with_ymd_and_hms(2026, 1, 1, 0, 2, 0).unwrap();
        sqlx::query(
            r#"
                INSERT INTO ohlc_candles (time, open, high, low, close, volume)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
        )
        .bind(candle_time)
        .bind(121.0)
        .bind(130.0)
        .bind(90.0)
        .bind(125.0)
        .bind(42_i64)
        .execute(repo.pool())
        .await
        .unwrap();

        let range = repo
            .get_price_range_from(Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 30).unwrap())
            .await
            .unwrap();

        assert_eq!(range, Some((90.0, 130.0, candle_time, 125.0)));
    }

    #[tokio::test]
    async fn remove_ticks_deletes_entries_at_or_before_boundary() {
        let repo = repo().await;
        let ticks = [tick(2026, 0, 100.0), tick(2026, 1, 120.5)];
        repo.add_ticks(&ticks).await.unwrap();

        repo.remove_ticks(ticks[0].time()).await.unwrap();

        let range = repo
            .get_price_range_from(Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap())
            .await
            .unwrap();

        assert_eq!(range, Some((120.5, 120.5, ticks[1].time(), 120.5)));
    }
}
