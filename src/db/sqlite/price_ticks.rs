use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use lnm_sdk::rest::v3::models::LastPrice;
use sqlx::SqlitePool;

use crate::db::{
    error::Result,
    models::PriceTickRow,
    repositories::{PriceTicksRepository, PriceTicksRepositoryRead},
};

pub(crate) struct SqlitePriceTicksRepo {
    #[allow(dead_code)]
    pool: Arc<SqlitePool>,
}

impl SqlitePriceTicksRepo {
    pub(crate) fn new(pool: Arc<SqlitePool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PriceTicksRepositoryRead for SqlitePriceTicksRepo {
    async fn get_latest_entry(&self) -> Result<Option<(DateTime<Utc>, f64)>> {
        todo!("implement SQLite price tick latest-entry read")
    }

    async fn get_price_range_from(
        &self,
        _start: DateTime<Utc>,
    ) -> Result<Option<(f64, f64, DateTime<Utc>, f64)>> {
        todo!("implement SQLite price range read")
    }
}

#[async_trait]
impl PriceTicksRepository for SqlitePriceTicksRepo {
    async fn add_ticks(&self, _ticks: &[LastPrice]) -> Result<Vec<PriceTickRow>> {
        todo!("implement SQLite price tick insertion")
    }

    async fn remove_ticks(&self, _before: DateTime<Utc>) -> Result<()> {
        todo!("implement SQLite price tick cleanup")
    }
}
