use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use lnm_sdk::rest::v3::models::PercentageCapped;
use sqlx::{QueryBuilder, Sqlite, SqlitePool};
use uuid::Uuid;

use crate::{
    db::{
        error::{DbError, Result},
        models::RunningTrade,
        repositories::{RunningTradesRepository, RunningTradesRepositoryRead},
    },
    trade::TradeTrailingStoploss,
};

pub(crate) struct SqliteRunningTradesRepo {
    pool: Arc<SqlitePool>,
}

impl SqliteRunningTradesRepo {
    pub(crate) fn new(pool: Arc<SqlitePool>) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &SqlitePool {
        self.pool.as_ref()
    }
}

#[async_trait]
impl RunningTradesRepositoryRead for SqliteRunningTradesRepo {
    async fn get_running_trades_map(
        &self,
        account_id: Uuid,
    ) -> Result<HashMap<Uuid, Option<TradeTrailingStoploss>>> {
        let running_trades = sqlx::query_as!(
            RunningTrade,
            r#"
                SELECT
                    trade_id as "trade_id!: Uuid",
                    trailing_stoploss as "trailing_stoploss?: f64"
                FROM running_trades
                WHERE account_id = ?1
                ORDER BY created_at ASC
            "#,
            account_id,
        )
        .fetch_all(self.pool())
        .await
        .map_err(DbError::Query)?;

        let mut running_trades_map = HashMap::new();

        for trade in running_trades.into_iter() {
            let trailing_stoploss = trade
                .trailing_stoploss
                .map(|tsl| {
                    PercentageCapped::try_from(tsl)
                        .map_err(|e| {
                            DbError::UnexpectedQueryResult(format!(
                                "`trailing_stoploss` ({tsl}) cannot be casted as `PercentageCapped`: {e}"
                            ))
                        })
                        .map(TradeTrailingStoploss::prev_validated)
                })
                .transpose()?;

            running_trades_map.insert(trade.trade_id, trailing_stoploss);
        }

        Ok(running_trades_map)
    }
}

#[async_trait]
impl RunningTradesRepository for SqliteRunningTradesRepo {
    async fn add_running_trade(
        &self,
        account_id: Uuid,
        trade_id: Uuid,
        trailing_stoploss: Option<TradeTrailingStoploss>,
    ) -> Result<()> {
        sqlx::query!(
            r#"
                INSERT INTO running_trades (account_id, trade_id, trailing_stoploss)
                VALUES (?1, ?2, ?3)
            "#,
            account_id,
            trade_id,
            trailing_stoploss.map(|tsl| tsl.as_f64()),
        )
        .execute(self.pool())
        .await
        .map_err(DbError::Query)?;

        Ok(())
    }

    async fn remove_running_trades(&self, account_id: Uuid, trade_ids: &[Uuid]) -> Result<()> {
        if trade_ids.is_empty() {
            return Ok(());
        }

        let mut query_builder =
            QueryBuilder::<Sqlite>::new("DELETE FROM running_trades WHERE account_id = ");
        query_builder.push_bind(account_id);
        query_builder.push(" AND trade_id IN (");

        let mut separated = query_builder.separated(", ");
        for trade_id in trade_ids {
            separated.push_bind(trade_id);
        }
        separated.push_unseparated(")");

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

    use lnm_sdk::rest::v3::models::PercentageCapped;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;

    async fn repo() -> SqliteRunningTradesRepo {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        sqlx::migrate!("./migrations/sqlite")
            .run(&pool)
            .await
            .unwrap();

        SqliteRunningTradesRepo::new(Arc::new(pool))
    }

    fn trailing_stoploss(value: f64) -> TradeTrailingStoploss {
        TradeTrailingStoploss::prev_validated(PercentageCapped::try_from(value).unwrap())
    }

    #[tokio::test]
    async fn running_trades_are_scoped_by_account() {
        let repo = repo().await;
        let account_a = Uuid::new_v4();
        let account_b = Uuid::new_v4();
        let shared_trade_id = Uuid::new_v4();

        repo.add_running_trade(account_a, shared_trade_id, Some(trailing_stoploss(5.0)))
            .await
            .unwrap();
        repo.add_running_trade(account_b, shared_trade_id, None)
            .await
            .unwrap();

        let account_a_trades = repo.get_running_trades_map(account_a).await.unwrap();
        assert_eq!(account_a_trades.len(), 1);
        assert_eq!(
            account_a_trades.get(&shared_trade_id).copied().flatten(),
            Some(trailing_stoploss(5.0))
        );

        let account_b_trades = repo.get_running_trades_map(account_b).await.unwrap();
        assert_eq!(account_b_trades.len(), 1);
        assert_eq!(
            account_b_trades.get(&shared_trade_id).copied().flatten(),
            None
        );
    }

    #[tokio::test]
    async fn remove_running_trades_deletes_only_requested_trades_for_account() {
        let repo = repo().await;
        let account_a = Uuid::new_v4();
        let account_b = Uuid::new_v4();
        let removed_trade_id = Uuid::new_v4();
        let kept_trade_id = Uuid::new_v4();

        repo.add_running_trade(account_a, removed_trade_id, None)
            .await
            .unwrap();
        repo.add_running_trade(account_a, kept_trade_id, None)
            .await
            .unwrap();
        repo.add_running_trade(account_b, removed_trade_id, None)
            .await
            .unwrap();

        repo.remove_running_trades(account_a, &[removed_trade_id])
            .await
            .unwrap();

        let account_a_trades = repo.get_running_trades_map(account_a).await.unwrap();
        assert!(!account_a_trades.contains_key(&removed_trade_id));
        assert!(account_a_trades.contains_key(&kept_trade_id));

        let account_b_trades = repo.get_running_trades_map(account_b).await.unwrap();
        assert!(account_b_trades.contains_key(&removed_trade_id));
    }

    #[tokio::test]
    async fn remove_running_trades_accepts_empty_trade_ids() {
        let repo = repo().await;
        let account_id = Uuid::new_v4();
        let trade_id = Uuid::new_v4();

        repo.add_running_trade(account_id, trade_id, None)
            .await
            .unwrap();

        repo.remove_running_trades(account_id, &[]).await.unwrap();

        let trades = repo.get_running_trades_map(account_id).await.unwrap();
        assert!(trades.contains_key(&trade_id));
    }
}
