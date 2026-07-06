use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{
    db::{
        error::Result,
        repositories::{RunningTradesRepository, RunningTradesRepositoryRead},
    },
    trade::TradeTrailingStoploss,
};

pub(crate) struct SqliteRunningTradesRepo {
    #[allow(dead_code)]
    pool: Arc<SqlitePool>,
}

impl SqliteRunningTradesRepo {
    pub(crate) fn new(pool: Arc<SqlitePool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RunningTradesRepositoryRead for SqliteRunningTradesRepo {
    async fn get_running_trades_map(
        &self,
        _account_id: Uuid,
    ) -> Result<HashMap<Uuid, Option<TradeTrailingStoploss>>> {
        todo!("implement SQLite running-trades map read")
    }
}

#[async_trait]
impl RunningTradesRepository for SqliteRunningTradesRepo {
    async fn add_running_trade(
        &self,
        _account_id: Uuid,
        _trade_id: Uuid,
        _trailing_stoploss: Option<TradeTrailingStoploss>,
    ) -> Result<()> {
        todo!("implement SQLite running-trade insertion")
    }

    async fn remove_running_trades(&self, _account_id: Uuid, _trade_ids: &[Uuid]) -> Result<()> {
        todo!("implement SQLite running-trades removal")
    }
}
