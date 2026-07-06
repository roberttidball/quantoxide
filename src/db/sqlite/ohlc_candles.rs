use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Timelike, Utc};
use lnm_sdk::rest::v3::models::OhlcCandle;
use sqlx::{QueryBuilder, Sqlite, SqlitePool, Transaction};

use crate::{
    db::{
        CANDLE_STABLE_AGE,
        error::{DbError, Result},
        models::OhlcCandleRow,
        repositories::{OhlcCandlesRepository, OhlcCandlesRepositoryRead},
    },
    shared::OhlcResolution,
    util::DateTimeExt,
};

pub(crate) struct SqliteOhlcCandlesRepo {
    pool: Arc<SqlitePool>,
}

struct ConsolidatedAccumulator {
    time: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    stable: bool,
}

impl SqliteOhlcCandlesRepo {
    pub(crate) fn new(pool: Arc<SqlitePool>) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &SqlitePool {
        self.pool.as_ref()
    }

    async fn start_transaction(&self) -> Result<Transaction<'static, Sqlite>> {
        self.pool.begin().await.map_err(DbError::TransactionBegin)
    }

    fn consolidate_candles(
        candles: Vec<OhlcCandleRow>,
        to: DateTime<Utc>,
        resolution: OhlcResolution,
    ) -> Vec<OhlcCandleRow> {
        let mut consolidated = Vec::new();
        let mut current: Option<ConsolidatedAccumulator> = None;

        for candle in candles {
            let bucket_time = candle.time.floor_to_resolution(resolution);

            match current.as_mut() {
                Some(acc) if acc.time == bucket_time => {
                    acc.high = acc.high.max(candle.high);
                    acc.low = acc.low.min(candle.low);
                    acc.close = candle.close;
                    acc.volume += candle.volume;
                    acc.created_at = acc.created_at.min(candle.created_at);
                    acc.updated_at = acc.updated_at.max(candle.updated_at);
                    acc.stable &= candle.stable;
                }
                Some(acc) => {
                    consolidated.push(Self::finish_consolidated(acc, to, resolution));
                    current = Some(ConsolidatedAccumulator::from_candle(bucket_time, candle));
                }
                None => current = Some(ConsolidatedAccumulator::from_candle(bucket_time, candle)),
            }
        }

        if let Some(acc) = current.as_ref() {
            consolidated.push(Self::finish_consolidated(acc, to, resolution));
        }

        consolidated
    }

    fn finish_consolidated(
        acc: &ConsolidatedAccumulator,
        to: DateTime<Utc>,
        resolution: OhlcResolution,
    ) -> OhlcCandleRow {
        OhlcCandleRow {
            time: acc.time,
            open: acc.open,
            high: acc.high,
            low: acc.low,
            close: acc.close,
            volume: acc.volume,
            created_at: acc.created_at,
            updated_at: acc.updated_at,
            stable: acc.stable
                && acc.time + Duration::seconds(resolution.as_seconds() as i64) <= to,
        }
    }
}

impl ConsolidatedAccumulator {
    fn from_candle(time: DateTime<Utc>, candle: OhlcCandleRow) -> Self {
        Self {
            time,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume,
            created_at: candle.created_at,
            updated_at: candle.updated_at,
            stable: candle.stable,
        }
    }
}

#[async_trait]
impl OhlcCandlesRepositoryRead for SqliteOhlcCandlesRepo {
    async fn get_candles(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<OhlcCandleRow>> {
        let rows = sqlx::query_as!(
            OhlcCandleRow,
            r#"
                SELECT
                    time as "time!: DateTime<Utc>",
                    open as "open!: f64",
                    high as "high!: f64",
                    low as "low!: f64",
                    close as "close!: f64",
                    volume as "volume!: i64",
                    created_at as "created_at!: DateTime<Utc>",
                    updated_at as "updated_at!: DateTime<Utc>",
                    stable as "stable!: bool"
                FROM ohlc_candles
                WHERE time >= ?1 AND time <= ?2
                ORDER BY time ASC
            "#,
            from,
            to,
        )
        .fetch_all(self.pool())
        .await
        .map_err(DbError::Query)?;

        Ok(rows)
    }

    async fn get_candles_consolidated(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        resolution: OhlcResolution,
    ) -> Result<Vec<OhlcCandleRow>> {
        let candles = self.get_candles(from, to).await?;

        if matches!(resolution, OhlcResolution::OneMinute) {
            return Ok(candles);
        }

        Ok(Self::consolidate_candles(candles, to, resolution))
    }

    async fn get_earliest_candle_time(&self) -> Result<Option<DateTime<Utc>>> {
        struct TimeRow {
            pub time: DateTime<Utc>,
        }

        let row = sqlx::query_as!(
            TimeRow,
            r#"
                SELECT time as "time!: DateTime<Utc>"
                FROM ohlc_candles
                ORDER BY time ASC
                LIMIT 1
            "#
        )
        .fetch_optional(self.pool())
        .await
        .map_err(DbError::Query)?;

        Ok(row.map(|r| r.time))
    }

    async fn get_latest_candle_time(&self) -> Result<Option<DateTime<Utc>>> {
        struct TimeRow {
            pub time: DateTime<Utc>,
        }

        let row = sqlx::query_as!(
            TimeRow,
            r#"
                SELECT time as "time!: DateTime<Utc>"
                FROM ohlc_candles
                ORDER BY time DESC
                LIMIT 1
            "#
        )
        .fetch_optional(self.pool())
        .await
        .map_err(DbError::Query)?;

        Ok(row.map(|r| r.time))
    }

    async fn get_gaps(&self) -> Result<Vec<(DateTime<Utc>, DateTime<Utc>)>> {
        let gaps = sqlx::query!(
            r#"
                SELECT
                    (
                        SELECT time FROM ohlc_candles
                        WHERE time < gap_candle.time AND stable = 1
                        ORDER BY time DESC
                        LIMIT 1
                    ) as "from_time!: DateTime<Utc>",
                    gap_candle.time as "gap_time!: DateTime<Utc>"
                FROM ohlc_candles gap_candle
                WHERE gap_candle.gap = 1
                AND gap_candle.stable = 1
                AND EXISTS (
                    SELECT 1 FROM ohlc_candles
                    WHERE time < gap_candle.time AND stable = 1
                )
                ORDER BY gap_candle.time ASC
            "#
        )
        .fetch_all(self.pool())
        .await
        .map_err(DbError::Query)?
        .into_iter()
        .map(|row| (row.from_time, row.gap_time))
        .collect();

        Ok(gaps)
    }
}

#[async_trait]
impl OhlcCandlesRepository for SqliteOhlcCandlesRepo {
    async fn add_candles(
        &self,
        before_candle_time: Option<DateTime<Utc>>,
        new_candles: &[OhlcCandle],
    ) -> Result<()> {
        if new_candles.is_empty() {
            return Ok(());
        }

        for window in new_candles.windows(2) {
            let [current, next] = window else {
                unreachable!()
            };

            if current.time().second() != 0 || current.time().nanosecond() != 0 {
                return Err(DbError::NewDbCandlesTimesNotRoundedToMinute);
            }

            if next.time() >= current.time() {
                return Err(DbError::NewDbCandlesNotOrderedByTimeDesc {
                    inconsistency_at: next.time(),
                });
            }
        }

        let period_start = new_candles.last().expect("not empty").time();

        if period_start.second() != 0 || period_start.nanosecond() != 0 {
            return Err(DbError::NewDbCandlesTimesNotRoundedToMinute);
        }

        let mut tx = self.start_transaction().await?;

        if let Some(before_candle_time) = before_candle_time {
            sqlx::query!(
                "UPDATE ohlc_candles SET gap = 0 WHERE time = ?1",
                before_candle_time
            )
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;
        }

        let before_period_time = period_start - Duration::minutes(1);
        let before_period_candle_exists = sqlx::query_scalar!(
            "SELECT EXISTS(SELECT 1 FROM ohlc_candles WHERE time = ?1 AND stable = 1)",
            before_period_time
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(DbError::Query)?
            != 0;

        let mut gaps: Vec<bool> = vec![false; new_candles.len()];
        gaps[new_candles.len() - 1] = !before_period_candle_exists;

        let stable_cutoff = Utc::now() - CANDLE_STABLE_AGE;
        let stables: Vec<bool> = new_candles
            .iter()
            .map(|candle| candle.time() <= stable_cutoff)
            .collect();

        let mut query_builder = QueryBuilder::<Sqlite>::new(
            "INSERT INTO ohlc_candles (time, open, high, low, close, volume, gap, stable) ",
        );

        query_builder.push_values(
            new_candles.iter().zip(gaps.iter()).zip(stables.iter()),
            |mut row, ((candle, gap), stable)| {
                row.push_bind(candle.time())
                    .push_bind(candle.open().as_f64())
                    .push_bind(candle.high().as_f64())
                    .push_bind(candle.low().as_f64())
                    .push_bind(candle.close().as_f64())
                    .push_bind(candle.volume() as i64)
                    .push_bind(*gap)
                    .push_bind(*stable);
            },
        );

        query_builder.push(
            r#"
                ON CONFLICT (time) DO UPDATE
                SET open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    gap = excluded.gap,
                    stable = excluded.stable
                WHERE ohlc_candles.open IS NOT excluded.open
                   OR ohlc_candles.high IS NOT excluded.high
                   OR ohlc_candles.low IS NOT excluded.low
                   OR ohlc_candles.close IS NOT excluded.close
                   OR ohlc_candles.volume IS NOT excluded.volume
                   OR ohlc_candles.gap IS NOT excluded.gap
                   OR ohlc_candles.stable IS NOT excluded.stable
            "#,
        );

        query_builder
            .build()
            .execute(&mut *tx)
            .await
            .map_err(DbError::Query)?;

        tx.commit().await.map_err(DbError::TransactionCommit)?;

        Ok(())
    }

    async fn remove_gap_flag(&self, time: DateTime<Utc>) -> Result<()> {
        sqlx::query!("UPDATE ohlc_candles SET gap = 0 WHERE time = ?1", time)
            .execute(self.pool())
            .await
            .map_err(DbError::Query)?;

        Ok(())
    }

    async fn flag_missing_candles(&self, range: Duration) -> Result<()> {
        let mut tx = self.start_transaction().await?;
        let cutoff_time = Utc::now() - range;

        struct TimeRow {
            pub time: DateTime<Utc>,
        }

        let gap_after_times = sqlx::query_as!(
            TimeRow,
            r#"
                WITH ordered AS (
                    SELECT
                        time,
                        stable,
                        LEAD(time) OVER (ORDER BY time ASC) AS next_time,
                        LEAD(stable) OVER (ORDER BY time ASC) AS next_stable,
                        LEAD(gap) OVER (ORDER BY time ASC) AS next_gap
                    FROM ohlc_candles
                    WHERE time >= ?1
                )
                SELECT time as "time!: DateTime<Utc>"
                FROM ordered
                WHERE stable = 1
                AND next_time IS NOT NULL
                AND unixepoch(next_time) > unixepoch(time) + 60
                AND next_stable = 1
                AND next_gap = 0
                ORDER BY time ASC
            "#,
            cutoff_time,
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        let mut unstable_times = HashSet::new();
        let mut gap_times = HashSet::new();

        for gap_after_time in gap_after_times.into_iter().map(|row| row.time) {
            let before_gap = sqlx::query_as!(
                TimeRow,
                r#"
                    SELECT time as "time!: DateTime<Utc>"
                    FROM ohlc_candles
                    WHERE time >= ?1 AND time <= ?2
                    ORDER BY time DESC
                    LIMIT 5
                "#,
                cutoff_time,
                gap_after_time,
            )
            .fetch_all(&mut *tx)
            .await
            .map_err(DbError::Query)?;

            unstable_times.extend(before_gap.into_iter().map(|row| row.time));

            let after_gap = sqlx::query_as!(
                TimeRow,
                r#"
                    SELECT time as "time!: DateTime<Utc>"
                    FROM ohlc_candles
                    WHERE time >= ?1 AND time > ?2
                    ORDER BY time ASC
                    LIMIT 6
                "#,
                cutoff_time,
                gap_after_time,
            )
            .fetch_all(&mut *tx)
            .await
            .map_err(DbError::Query)?;

            for (index, row) in after_gap.into_iter().enumerate() {
                if index < 5 {
                    unstable_times.insert(row.time);
                } else {
                    gap_times.insert(row.time);
                }
            }
        }

        for time in unstable_times {
            sqlx::query!("UPDATE ohlc_candles SET stable = 0 WHERE time = ?1", time)
                .execute(&mut *tx)
                .await
                .map_err(DbError::Query)?;
        }

        for time in gap_times {
            sqlx::query!("UPDATE ohlc_candles SET gap = 1 WHERE time = ?1", time)
                .execute(&mut *tx)
                .await
                .map_err(DbError::Query)?;
        }

        sqlx::query!(
            r#"
                UPDATE ohlc_candles
                SET gap = 1
                WHERE time IN (
                    SELECT c_stable.time
                    FROM ohlc_candles c_stable
                    WHERE c_stable.time >= ?1
                    AND c_stable.stable = 1
                    AND c_stable.gap = 0
                    AND (
                        SELECT stable
                        FROM ohlc_candles
                        WHERE time < c_stable.time
                        ORDER BY time DESC
                        LIMIT 1
                    ) = 0
                )
            "#,
            cutoff_time,
        )
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;

        tx.commit().await.map_err(DbError::TransactionCommit)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration as StdDuration};

    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;

    async fn repo() -> SqliteOhlcCandlesRepo {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        sqlx::migrate!("./migrations/sqlite")
            .run(&pool)
            .await
            .unwrap();

        SqliteOhlcCandlesRepo::new(Arc::new(pool))
    }

    fn candle(minute: u32, open: f64, high: f64, low: f64, close: f64, volume: u64) -> OhlcCandle {
        serde_json::from_value(json!({
            "time": Utc.with_ymd_and_hms(2025, 1, 1, 0, minute, 0).unwrap(),
            "open": open,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }))
        .unwrap()
    }

    async fn insert_stable_candle(repo: &SqliteOhlcCandlesRepo, minute: u32) {
        sqlx::query(
            r#"
                INSERT INTO ohlc_candles (time, open, high, low, close, volume, stable)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)
            "#,
        )
        .bind(Utc.with_ymd_and_hms(2025, 1, 1, 0, minute, 0).unwrap())
        .bind(100.0 + minute as f64)
        .bind(101.0 + minute as f64)
        .bind(99.0 + minute as f64)
        .bind(100.5 + minute as f64)
        .bind(minute as i64)
        .execute(repo.pool())
        .await
        .unwrap();
    }

    async fn candle_flags(repo: &SqliteOhlcCandlesRepo, minute: u32) -> (bool, bool) {
        sqlx::query_as::<_, (bool, bool)>(
            r#"
                SELECT gap, stable
                FROM ohlc_candles
                WHERE time = ?1
            "#,
        )
        .bind(Utc.with_ymd_and_hms(2025, 1, 1, 0, minute, 0).unwrap())
        .fetch_one(repo.pool())
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn add_candles_upserts_and_reads_ranges_without_touching_unchanged_rows() {
        let repo = repo().await;
        let candles = [
            candle(2, 102.0, 112.0, 92.0, 107.0, 12),
            candle(1, 101.0, 111.0, 91.0, 106.0, 11),
        ];

        repo.add_candles(None, &candles).await.unwrap();

        let from = Utc.with_ymd_and_hms(2025, 1, 1, 0, 1, 0).unwrap();
        let to = Utc.with_ymd_and_hms(2025, 1, 1, 0, 2, 0).unwrap();
        let first_read = repo.get_candles(from, to).await.unwrap();
        assert_eq!(first_read.len(), 2);
        assert_eq!(first_read[0].time, candles[1].time());
        assert_eq!(first_read[0].open, 101.0);
        assert!(first_read[0].stable);

        repo.add_candles(None, &candles).await.unwrap();
        let unchanged_read = repo.get_candles(from, to).await.unwrap();
        assert_eq!(unchanged_read[0].updated_at, first_read[0].updated_at);

        tokio::time::sleep(StdDuration::from_millis(10)).await;
        let changed = [
            candle(2, 102.0, 112.0, 92.0, 107.0, 12),
            candle(1, 101.0, 111.0, 91.0, 108.0, 11),
        ];
        repo.add_candles(None, &changed).await.unwrap();
        let changed_read = repo.get_candles(from, to).await.unwrap();
        assert_eq!(changed_read[0].close, 108.0);
        assert!(changed_read[0].updated_at > first_read[0].updated_at);
    }

    #[tokio::test]
    async fn add_candles_rejects_invalid_input_ordering_and_rounding() {
        let repo = repo().await;
        let ascending = [
            candle(1, 101.0, 111.0, 91.0, 106.0, 11),
            candle(2, 102.0, 112.0, 92.0, 107.0, 12),
        ];
        assert!(matches!(
            repo.add_candles(None, &ascending).await,
            Err(DbError::NewDbCandlesNotOrderedByTimeDesc { .. })
        ));

        let unrounded = serde_json::from_value::<OhlcCandle>(json!({
            "time": Utc.with_ymd_and_hms(2025, 1, 1, 0, 1, 1).unwrap(),
            "open": 101.0,
            "high": 111.0,
            "low": 91.0,
            "close": 106.0,
            "volume": 11,
        }))
        .unwrap();
        assert!(matches!(
            repo.add_candles(None, &[unrounded]).await,
            Err(DbError::NewDbCandlesTimesNotRoundedToMinute)
        ));
    }

    #[tokio::test]
    async fn consolidated_reads_aggregate_candles_by_resolution() {
        let repo = repo().await;
        let candles = [
            candle(2, 102.0, 112.0, 92.0, 107.0, 12),
            candle(1, 101.0, 111.0, 91.0, 106.0, 11),
            candle(0, 100.0, 110.0, 90.0, 105.0, 10),
        ];
        repo.add_candles(None, &candles).await.unwrap();

        let rows = repo
            .get_candles_consolidated(
                Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2025, 1, 1, 0, 3, 0).unwrap(),
                OhlcResolution::ThreeMinutes,
            )
            .await
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].time,
            Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()
        );
        assert_eq!(rows[0].open, 100.0);
        assert_eq!(rows[0].high, 112.0);
        assert_eq!(rows[0].low, 90.0);
        assert_eq!(rows[0].close, 107.0);
        assert_eq!(rows[0].volume, 33);
        assert!(rows[0].stable);
    }

    #[tokio::test]
    async fn earliest_and_latest_candle_time_return_bounds() {
        let repo = repo().await;
        assert_eq!(repo.get_earliest_candle_time().await.unwrap(), None);
        assert_eq!(repo.get_latest_candle_time().await.unwrap(), None);

        repo.add_candles(
            None,
            &[
                candle(2, 102.0, 112.0, 92.0, 107.0, 12),
                candle(1, 101.0, 111.0, 91.0, 106.0, 11),
            ],
        )
        .await
        .unwrap();

        assert_eq!(
            repo.get_earliest_candle_time().await.unwrap(),
            Some(Utc.with_ymd_and_hms(2025, 1, 1, 0, 1, 0).unwrap())
        );
        assert_eq!(
            repo.get_latest_candle_time().await.unwrap(),
            Some(Utc.with_ymd_and_hms(2025, 1, 1, 0, 2, 0).unwrap())
        );
    }

    #[tokio::test]
    async fn gap_flags_are_placed_cleared_and_read() {
        let repo = repo().await;
        insert_stable_candle(&repo, 0).await;

        repo.add_candles(None, &[candle(3, 103.0, 113.0, 93.0, 108.0, 13)])
            .await
            .unwrap();
        assert_eq!(
            repo.get_gaps().await.unwrap(),
            vec![(
                Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2025, 1, 1, 0, 3, 0).unwrap(),
            )]
        );

        repo.add_candles(
            Some(Utc.with_ymd_and_hms(2025, 1, 1, 0, 3, 0).unwrap()),
            &[
                candle(2, 102.0, 112.0, 92.0, 107.0, 12),
                candle(1, 101.0, 111.0, 91.0, 106.0, 11),
            ],
        )
        .await
        .unwrap();
        assert!(repo.get_gaps().await.unwrap().is_empty());

        repo.remove_gap_flag(Utc.with_ymd_and_hms(2025, 1, 1, 0, 3, 0).unwrap())
            .await
            .unwrap();
        assert!(repo.get_gaps().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn flag_missing_candles_marks_unstable_neighbors_and_gap_marker() {
        let repo = repo().await;
        for minute in 0..=6 {
            insert_stable_candle(&repo, minute).await;
        }
        for minute in 10..=16 {
            insert_stable_candle(&repo, minute).await;
        }

        repo.flag_missing_candles(Duration::days(1000))
            .await
            .unwrap();

        assert_eq!(candle_flags(&repo, 1).await, (false, true));
        assert_eq!(candle_flags(&repo, 2).await, (false, false));
        assert_eq!(candle_flags(&repo, 10).await, (false, false));
        assert_eq!(candle_flags(&repo, 14).await, (false, false));
        assert_eq!(candle_flags(&repo, 15).await, (true, true));

        assert_eq!(
            repo.get_gaps().await.unwrap(),
            vec![(
                Utc.with_ymd_and_hms(2025, 1, 1, 0, 1, 0).unwrap(),
                Utc.with_ymd_and_hms(2025, 1, 1, 0, 15, 0).unwrap(),
            )]
        );
    }
}
