use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use lnm_sdk::rest::v3::models::OhlcCandle;
use sqlx::SqlitePool;

use crate::{
    db::{
        error::Result,
        models::OhlcCandleRow,
        repositories::{OhlcCandlesRepository, OhlcCandlesRepositoryRead},
    },
    shared::OhlcResolution,
};

pub(crate) struct SqliteOhlcCandlesRepo {
    #[allow(dead_code)]
    pool: Arc<SqlitePool>,
}

impl SqliteOhlcCandlesRepo {
    pub(crate) fn new(pool: Arc<SqlitePool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl OhlcCandlesRepositoryRead for SqliteOhlcCandlesRepo {
    async fn get_candles(
        &self,
        _from: DateTime<Utc>,
        _to: DateTime<Utc>,
    ) -> Result<Vec<OhlcCandleRow>> {
        todo!("implement SQLite OHLC candle range read")
    }

    async fn get_candles_consolidated(
        &self,
        _from: DateTime<Utc>,
        _to: DateTime<Utc>,
        _resolution: OhlcResolution,
    ) -> Result<Vec<OhlcCandleRow>> {
        todo!("implement SQLite OHLC candle consolidated read")
    }

    async fn get_earliest_candle_time(&self) -> Result<Option<DateTime<Utc>>> {
        todo!("implement SQLite earliest OHLC candle read")
    }

    async fn get_latest_candle_time(&self) -> Result<Option<DateTime<Utc>>> {
        todo!("implement SQLite latest OHLC candle read")
    }

    async fn get_gaps(&self) -> Result<Vec<(DateTime<Utc>, DateTime<Utc>)>> {
        todo!("implement SQLite OHLC gap read")
    }
}

#[async_trait]
impl OhlcCandlesRepository for SqliteOhlcCandlesRepo {
    async fn add_candles(
        &self,
        _before_candle_time: Option<DateTime<Utc>>,
        _new_candles: &[OhlcCandle],
    ) -> Result<()> {
        todo!("implement SQLite OHLC candle insertion")
    }

    async fn remove_gap_flag(&self, _time: DateTime<Utc>) -> Result<()> {
        todo!("implement SQLite OHLC gap removal")
    }

    async fn flag_missing_candles(&self, _range: Duration) -> Result<()> {
        todo!("implement SQLite OHLC missing-candle flagging")
    }
}
