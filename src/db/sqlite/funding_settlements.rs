use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use lnm_sdk::rest::v3::models::FundingSettlement;
use sqlx::SqlitePool;

use crate::db::{
    error::Result,
    models::FundingSettlementRow,
    repositories::{FundingSettlementsRepository, FundingSettlementsRepositoryRead},
};

pub(crate) struct SqliteFundingSettlementsRepo {
    #[allow(dead_code)]
    pool: Arc<SqlitePool>,
}

impl SqliteFundingSettlementsRepo {
    pub(crate) fn new(pool: Arc<SqlitePool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl FundingSettlementsRepositoryRead for SqliteFundingSettlementsRepo {
    async fn get_settlements(
        &self,
        _from: DateTime<Utc>,
        _to: DateTime<Utc>,
    ) -> Result<Vec<FundingSettlementRow>> {
        todo!("implement SQLite funding-settlements range read")
    }

    async fn get_earliest_settlement_time(&self) -> Result<Option<DateTime<Utc>>> {
        todo!("implement SQLite earliest funding-settlement read")
    }

    async fn get_latest_settlement_time(&self) -> Result<Option<DateTime<Utc>>> {
        todo!("implement SQLite latest funding-settlement read")
    }

    async fn get_missing_settlement_times(
        &self,
        _from: DateTime<Utc>,
        _to: DateTime<Utc>,
    ) -> Result<Vec<DateTime<Utc>>> {
        todo!("implement SQLite missing funding-settlement detection")
    }
}

#[async_trait]
impl FundingSettlementsRepository for SqliteFundingSettlementsRepo {
    async fn add_settlements(&self, _settlements: &[FundingSettlement]) -> Result<()> {
        todo!("implement SQLite funding-settlements insertion")
    }
}
