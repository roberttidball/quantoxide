use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use lnm_sdk::rest::v3::models::{FundingSettlement, LastPrice, OhlcCandle};

use crate::{shared::OhlcResolution, trade::TradeTrailingStoploss};

use super::{
    error::Result,
    models::{FundingSettlementRow, OhlcCandleRow, PriceTickRow},
};

#[async_trait]
/// Read-only price tick repository API.
pub trait PriceTicksRepositoryRead: Send + Sync {
    /// Returns the latest known price entry as `(time, price)`, or `None` when no price data exists.
    ///
    /// ```rust,no_run
    /// # async fn example(db: &quantoxide::Database) -> Result<(), Box<dyn std::error::Error>> {
    /// let latest = db.price_ticks().get_latest_entry().await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn get_latest_entry(&self) -> Result<Option<(DateTime<Utc>, f64)>>;

    /// Returns the price range since `start` as `(min_price, max_price, latest_time, latest_price)`.
    ///
    /// ```rust,no_run
    /// # async fn example(db: &quantoxide::Database) -> Result<(), Box<dyn std::error::Error>> {
    /// use chrono::{Duration, Utc};
    ///
    /// let start = Utc::now() - Duration::hours(24);
    /// let range = db.price_ticks().get_price_range_from(start).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn get_price_range_from(
        &self,
        start: DateTime<Utc>,
    ) -> Result<Option<(f64, f64, DateTime<Utc>, f64)>>;
}

#[async_trait]
pub(crate) trait PriceTicksRepository: PriceTicksRepositoryRead {
    /// Adds multiple price ticks to the database in a single batch operation.
    /// Uses INSERT ON CONFLICT DO NOTHING to avoid duplicate entries.
    ///
    /// Returns only the ticks that were successfully inserted (new entries).
    async fn add_ticks(&self, ticks: &[LastPrice]) -> Result<Vec<PriceTickRow>>;

    async fn remove_ticks(&self, before: DateTime<Utc>) -> Result<()>;
}

#[async_trait]
pub(crate) trait RunningTradesRepository: Send + Sync {
    async fn add_running_trade(
        &self,
        account_id: Uuid,
        trade_id: Uuid,
        trailing_stoploss: Option<TradeTrailingStoploss>,
    ) -> Result<()>;

    async fn get_running_trades_map(
        &self,
        account_id: Uuid,
    ) -> Result<HashMap<Uuid, Option<TradeTrailingStoploss>>>;

    async fn remove_running_trades(&self, account_id: Uuid, trade_ids: &[Uuid]) -> Result<()>;
}

#[async_trait]
/// Read-only OHLC candle repository API.
pub trait OhlcCandlesRepositoryRead: Send + Sync {
    /// Fetches one-minute OHLC candles within the specified time range, ordered by time ASC.
    ///
    /// ```rust,no_run
    /// # async fn example(db: &quantoxide::Database) -> Result<(), Box<dyn std::error::Error>> {
    /// use chrono::{Duration, Utc};
    ///
    /// let to = Utc::now();
    /// let from = to - Duration::hours(24);
    ///
    /// let candles = db.ohlc_candles().get_candles(from, to).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn get_candles(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<OhlcCandleRow>>;

    /// Fetches OHLC candles consolidated to the specified resolution.
    ///
    /// ```rust,no_run
    /// # async fn example(db: &quantoxide::Database) -> Result<(), Box<dyn std::error::Error>> {
    /// use chrono::{Duration, Utc};
    /// use quantoxide::models::OhlcResolution;
    ///
    /// let to = Utc::now();
    /// let from = to - Duration::hours(24);
    /// let resolution = OhlcResolution::OneHour;
    ///
    /// let candles = db
    ///     .ohlc_candles()
    ///     .get_candles_consolidated(from, to, resolution)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn get_candles_consolidated(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        resolution: OhlcResolution,
    ) -> Result<Vec<OhlcCandleRow>>;

    /// Returns the earliest candle time in the database, or `None` when no candles exist.
    ///
    /// ```rust,no_run
    /// # async fn example(db: &quantoxide::Database) -> Result<(), Box<dyn std::error::Error>> {
    /// let earliest = db.ohlc_candles().get_earliest_candle_time().await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn get_earliest_candle_time(&self) -> Result<Option<DateTime<Utc>>>;

    /// Returns the latest candle time in the database, or `None` when no candles exist.
    ///
    /// ```rust,no_run
    /// # async fn example(db: &quantoxide::Database) -> Result<(), Box<dyn std::error::Error>> {
    /// let latest = db.ohlc_candles().get_latest_candle_time().await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn get_latest_candle_time(&self) -> Result<Option<DateTime<Utc>>>;

    /// Returns stable candle gaps as `(from_time, gap_time)` pairs ordered by `gap_time` ASC.
    ///
    /// ```rust,no_run
    /// # async fn example(db: &quantoxide::Database) -> Result<(), Box<dyn std::error::Error>> {
    /// let gaps = db.ohlc_candles().get_gaps().await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn get_gaps(&self) -> Result<Vec<(DateTime<Utc>, DateTime<Utc>)>>;
}

#[async_trait]
pub(crate) trait OhlcCandlesRepository: OhlcCandlesRepositoryRead {
    /// Adds OHLC candles to the database, distinguishing between stable and unstable candles.
    async fn add_candles(
        &self,
        before_candle_time: Option<DateTime<Utc>>,
        new_candles: &[OhlcCandle],
    ) -> Result<()>;

    async fn remove_gap_flag(&self, time: DateTime<Utc>) -> Result<()>;

    /// Finds unflagged gaps in the candle history and marks surrounding candles as unstable
    /// so they can be re-fetched from the API.
    async fn flag_missing_candles(&self, range: Duration) -> Result<()>;
}

#[async_trait]
pub(crate) trait FundingSettlementsRepository: Send + Sync {
    /// Adds multiple funding settlements to the database. Idempotent.
    async fn add_settlements(&self, settlements: &[FundingSettlement]) -> Result<()>;

    /// Retrieves funding settlements within the specified time range, ordered by time ASC.
    async fn get_settlements(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<FundingSettlementRow>>;

    /// Returns the earliest settlement time in the database.
    async fn get_earliest_settlement_time(&self) -> Result<Option<DateTime<Utc>>>;

    /// Returns the latest settlement time in the database.
    async fn get_latest_settlement_time(&self) -> Result<Option<DateTime<Utc>>>;

    /// Returns the times of missing settlements on the funding settlement grid between the given
    /// bounds, ordered by time ASC. Handles all three LNM funding settlement grid phases
    /// transitions internally:
    /// + Phase A ({08} UTC, 24h)
    /// + Phase B ({04, 12, 20} UTC, 8h)
    /// + Phase C ({00, 08, 16} UTC, 8h)
    async fn get_missing_settlement_times(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<DateTime<Utc>>>;
}
