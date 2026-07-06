use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use lnm_sdk::rest::v3::models::FundingSettlement;
use sqlx::{QueryBuilder, Sqlite, SqlitePool};

use crate::{
    db::{
        error::{DbError, Result},
        models::FundingSettlementRow,
        repositories::{FundingSettlementsRepository, FundingSettlementsRepositoryRead},
    },
    sync::{
        LNM_SETTLEMENT_A_END, LNM_SETTLEMENT_A_START, LNM_SETTLEMENT_B_END, LNM_SETTLEMENT_B_START,
        LNM_SETTLEMENT_C_START, LNM_SETTLEMENT_INTERVAL_8H, LNM_SETTLEMENT_INTERVAL_DAY,
    },
    util::DateTimeExt,
};

pub(crate) struct SqliteFundingSettlementsRepo {
    pool: Arc<SqlitePool>,
}

impl SqliteFundingSettlementsRepo {
    pub(crate) fn new(pool: Arc<SqlitePool>) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &SqlitePool {
        self.pool.as_ref()
    }

    fn push_settlement_times(
        settlement_times: &mut Vec<DateTime<Utc>>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        interval: Duration,
    ) {
        if from > to {
            return;
        }

        let mut time = from;
        while time <= to {
            settlement_times.push(time);
            time += interval;
        }
    }
}

#[async_trait]
impl FundingSettlementsRepositoryRead for SqliteFundingSettlementsRepo {
    async fn get_settlements(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<FundingSettlementRow>> {
        let rows = sqlx::query_as!(
            FundingSettlementRow,
            r#"
                SELECT
                    id as "id!: uuid::Uuid",
                    time as "time!: DateTime<Utc>",
                    fixing_price as "fixing_price!: f64",
                    funding_rate as "funding_rate!: f64",
                    created_at as "created_at!: DateTime<Utc>"
                FROM funding_settlements
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

    async fn get_earliest_settlement_time(&self) -> Result<Option<DateTime<Utc>>> {
        struct TimeRow {
            pub time: DateTime<Utc>,
        }

        let row = sqlx::query_as!(
            TimeRow,
            r#"
                SELECT time as "time!: DateTime<Utc>"
                FROM funding_settlements
                ORDER BY time ASC
                LIMIT 1
            "#
        )
        .fetch_optional(self.pool())
        .await
        .map_err(DbError::Query)?;

        Ok(row.map(|r| r.time))
    }

    async fn get_latest_settlement_time(&self) -> Result<Option<DateTime<Utc>>> {
        struct TimeRow {
            pub time: DateTime<Utc>,
        }

        let row = sqlx::query_as!(
            TimeRow,
            r#"
                SELECT time as "time!: DateTime<Utc>"
                FROM funding_settlements
                ORDER BY time DESC
                LIMIT 1
            "#
        )
        .fetch_optional(self.pool())
        .await
        .map_err(DbError::Query)?;

        Ok(row.map(|r| r.time))
    }

    async fn get_missing_settlement_times(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<DateTime<Utc>>> {
        if !from.is_valid_funding_settlement_time() {
            return Err(DbError::InvalidFundingSettlementTime { time: from });
        }
        if !to.is_valid_funding_settlement_time() {
            return Err(DbError::InvalidFundingSettlementTime { time: to });
        }

        let mut expected_settlement_times = Vec::new();

        if from <= LNM_SETTLEMENT_A_END {
            let phase_a_from = from.max(LNM_SETTLEMENT_A_START);
            let phase_a_to = to.min(LNM_SETTLEMENT_A_END);
            Self::push_settlement_times(
                &mut expected_settlement_times,
                phase_a_from,
                phase_a_to,
                LNM_SETTLEMENT_INTERVAL_DAY,
            );
        }

        if from <= LNM_SETTLEMENT_B_END && to >= LNM_SETTLEMENT_B_START {
            let phase_b_from = from.max(LNM_SETTLEMENT_B_START);
            let phase_b_to = to.min(LNM_SETTLEMENT_B_END);
            Self::push_settlement_times(
                &mut expected_settlement_times,
                phase_b_from,
                phase_b_to,
                LNM_SETTLEMENT_INTERVAL_8H,
            );
        }

        if to >= LNM_SETTLEMENT_C_START {
            let phase_c_from = from.max(LNM_SETTLEMENT_C_START);
            Self::push_settlement_times(
                &mut expected_settlement_times,
                phase_c_from,
                to,
                LNM_SETTLEMENT_INTERVAL_8H,
            );
        }

        if expected_settlement_times.is_empty() {
            return Ok(Vec::new());
        }

        let existing_settlement_times = self
            .get_settlements(from, to)
            .await?
            .into_iter()
            .map(|row| row.time)
            .collect::<HashSet<_>>();

        let missing = expected_settlement_times
            .into_iter()
            .filter(|time| !existing_settlement_times.contains(time))
            .collect();

        Ok(missing)
    }
}

#[async_trait]
impl FundingSettlementsRepository for SqliteFundingSettlementsRepo {
    async fn add_settlements(&self, settlements: &[FundingSettlement]) -> Result<()> {
        if settlements.is_empty() {
            return Ok(());
        }

        let mut query_builder = QueryBuilder::<Sqlite>::new(
            "INSERT INTO funding_settlements (id, time, fixing_price, funding_rate) ",
        );

        query_builder.push_values(settlements, |mut row, settlement| {
            row.push_bind(settlement.id())
                .push_bind(settlement.time())
                .push_bind(settlement.fixing_price())
                .push_bind(settlement.funding_rate());
        });

        query_builder.push(" ON CONFLICT (time) DO NOTHING");

        query_builder
            .build()
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

    async fn repo() -> SqliteFundingSettlementsRepo {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        sqlx::migrate!("./migrations/sqlite")
            .run(&pool)
            .await
            .unwrap();

        SqliteFundingSettlementsRepo::new(Arc::new(pool))
    }

    fn settlement(time: DateTime<Utc>, fixing_price: f64) -> FundingSettlement {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "time": time,
            "fixingPrice": fixing_price,
            "fundingRate": 0.0001,
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn add_settlements_ignores_duplicate_times_and_reads_range() {
        let repo = repo().await;
        let first_time = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let second_time = Utc.with_ymd_and_hms(2026, 1, 1, 8, 0, 0).unwrap();
        let settlements = [
            settlement(first_time, 100.0),
            settlement(second_time, 110.0),
        ];

        repo.add_settlements(&settlements).await.unwrap();
        repo.add_settlements(&[settlement(first_time, 120.0)])
            .await
            .unwrap();

        let rows = repo.get_settlements(first_time, second_time).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].time, first_time);
        assert_eq!(rows[0].fixing_price, 100.0);
        assert_eq!(rows[1].time, second_time);
        assert_eq!(rows[1].fixing_price, 110.0);
    }

    #[tokio::test]
    async fn earliest_and_latest_settlement_time_return_bounds() {
        let repo = repo().await;
        assert_eq!(repo.get_earliest_settlement_time().await.unwrap(), None);
        assert_eq!(repo.get_latest_settlement_time().await.unwrap(), None);

        let first_time = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let second_time = Utc.with_ymd_and_hms(2026, 1, 1, 8, 0, 0).unwrap();
        repo.add_settlements(&[
            settlement(second_time, 110.0),
            settlement(first_time, 100.0),
        ])
        .await
        .unwrap();

        assert_eq!(
            repo.get_earliest_settlement_time().await.unwrap(),
            Some(first_time)
        );
        assert_eq!(
            repo.get_latest_settlement_time().await.unwrap(),
            Some(second_time)
        );
    }

    #[tokio::test]
    async fn missing_settlement_times_use_phase_grids_and_existing_rows() {
        let repo = repo().await;
        let from = LNM_SETTLEMENT_A_START;
        let middle = from + LNM_SETTLEMENT_INTERVAL_DAY;
        let to = middle + LNM_SETTLEMENT_INTERVAL_DAY;

        repo.add_settlements(&[settlement(middle, 100.0)])
            .await
            .unwrap();

        let missing = repo.get_missing_settlement_times(from, to).await.unwrap();
        assert_eq!(missing, vec![from, to]);
    }

    #[tokio::test]
    async fn missing_settlement_times_cross_phase_boundary() {
        let repo = repo().await;

        repo.add_settlements(&[settlement(LNM_SETTLEMENT_B_END, 100.0)])
            .await
            .unwrap();

        let missing = repo
            .get_missing_settlement_times(LNM_SETTLEMENT_B_END, LNM_SETTLEMENT_C_START)
            .await
            .unwrap();
        assert_eq!(missing, vec![LNM_SETTLEMENT_C_START]);
    }

    #[tokio::test]
    async fn missing_settlement_times_reject_invalid_bounds() {
        let repo = repo().await;
        let invalid_from = Utc.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap();
        let valid_to = Utc.with_ymd_and_hms(2026, 1, 1, 8, 0, 0).unwrap();

        let error = repo
            .get_missing_settlement_times(invalid_from, valid_to)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DbError::InvalidFundingSettlementTime { time } if time == invalid_from
        ));
    }
}
