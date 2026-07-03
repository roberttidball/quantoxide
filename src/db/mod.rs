use std::{sync::Arc, time};

use chrono::Duration;
use lazy_static::lazy_static;
use sqlx::postgres::PgPoolOptions;

pub(crate) mod error;
pub(crate) mod models;

/// Candles are only marked stable (skip re-fetch) once their time is at least this far in the
/// past. The API may return slightly different OHLC values for recent candles across requests.
pub(crate) const CANDLE_STABLE_AGE: Duration = Duration::hours(1);

mod postgres;
mod repositories;

pub use repositories::{
    FundingSettlementsRepositoryRead, OhlcCandlesRepositoryRead, PriceTicksRepositoryRead,
    RunningTradesRepositoryRead,
};

use error::{DbError, Result};
use postgres::{
    funding_settlements::PgFundingSettlementsRepo, ohlc_candles::PgOhlcCandlesRepo,
    price_ticks::PgPriceTicksRepo, running_trades::PgRunningTradesRepo,
};
use repositories::{
    FundingSettlementsRepository, OhlcCandlesRepository, PriceTicksRepository,
    RunningTradesRepository,
};

lazy_static! {
    /// Default [`PgPoolOptions`] used by [`Database::new`].
    pub static ref DEFAULT_PG_POOL_OPTIONS: PgPoolOptions = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(time::Duration::from_secs(60));
}

/// Pool options used when constructing a [`Database`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum DatabasePoolOptions {
    /// PostgreSQL pool options.
    Postgres(PgPoolOptions),
}

impl From<PgPoolOptions> for DatabasePoolOptions {
    fn from(pool_options: PgPoolOptions) -> Self {
        Self::Postgres(pool_options)
    }
}

/// Primary database interface for market data persistence and retrieval.
///
/// Provides access to repositories for OHLC candle data, price tick data, and running trade
/// information. Uses PostgreSQL as the underlying storage engine with automatic migrations.
pub struct Database {
    pub(crate) ohlc_candles: Box<dyn OhlcCandlesRepository>,
    pub(crate) price_ticks: Box<dyn PriceTicksRepository>,
    pub(crate) running_trades: Box<dyn RunningTradesRepository>,
    pub(crate) funding_settlements: Box<dyn FundingSettlementsRepository>,
}

impl Database {
    /// Creates a new database instance with default pool options and runs migrations.
    ///
    /// Establishes a connection pool to the PostgreSQL database and automatically applies any
    /// pending migrations. Returns an error if the connection fails or migrations cannot be
    /// applied.
    ///
    /// The default PostgreSQL pool currently allows up to 10 connections and waits up to 60
    /// seconds to acquire a connection.
    ///
    /// ```rust,no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use quantoxide::Database;
    ///
    /// let db_url = std::env::var("POSTGRES_DB_URL")?;
    /// let db = Database::new(&db_url).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(postgres_db_url: &str) -> Result<Arc<Self>> {
        Self::with_pool_options(postgres_db_url, DEFAULT_PG_POOL_OPTIONS.clone()).await
    }

    /// Creates a new database instance with caller-provided pool options and runs migrations.
    ///
    /// Use this constructor when the default pool size or acquire timeout is not appropriate for
    /// the workload or host. [`PgPoolOptions`] is re-exported by this crate so consumers can
    /// configure the pool without depending on `sqlx` directly.
    ///
    /// ```rust,no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::time::Duration;
    ///
    /// use quantoxide::{Database, PgPoolOptions};
    ///
    /// let db_url = std::env::var("POSTGRES_DB_URL")?;
    /// let pool_options = PgPoolOptions::new()
    ///     .max_connections(20)
    ///     .acquire_timeout(Duration::from_secs(120));
    ///
    /// let db = Database::with_pool_options(&db_url, pool_options).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_pool_options(
        database_url: &str,
        pool_options: impl Into<DatabasePoolOptions>,
    ) -> Result<Arc<Self>> {
        match pool_options.into() {
            DatabasePoolOptions::Postgres(pool_options) => {
                let pool = pool_options
                    .connect(database_url)
                    .await
                    .map_err(DbError::Connection)?;

                sqlx::migrate!("./migrations")
                    .run(&pool)
                    .await
                    .map_err(DbError::Migration)?;

                let pool = Arc::new(pool);
                let ohlc_candles = Box::new(PgOhlcCandlesRepo::new(pool.clone()));
                let price_ticks = Box::new(PgPriceTicksRepo::new(pool.clone()));
                let running_trades = Box::new(PgRunningTradesRepo::new(pool.clone()));
                let funding_settlements = Box::new(PgFundingSettlementsRepo::new(pool.clone()));

                Ok(Arc::new(Self {
                    ohlc_candles,
                    price_ticks,
                    running_trades,
                    funding_settlements,
                }))
            }
        }
    }

    /// Returns read-only access to OHLC candle data.
    ///
    /// ```rust,no_run
    /// # async fn example(db: &quantoxide::Database) -> Result<(), Box<dyn std::error::Error>> {
    /// use chrono::{Utc, Duration};
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
    pub fn ohlc_candles(&self) -> &(dyn OhlcCandlesRepositoryRead + '_) {
        self.ohlc_candles.as_ref()
    }

    /// Returns read-only access to price tick data.
    ///
    /// ```rust,no_run
    /// # async fn example(db: &quantoxide::Database) -> Result<(), Box<dyn std::error::Error>> {
    /// let latest = db.price_ticks().get_latest_entry().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn price_ticks(&self) -> &(dyn PriceTicksRepositoryRead + '_) {
        self.price_ticks.as_ref()
    }

    /// Returns read-only access to funding settlement data.
    ///
    /// ```rust,no_run
    /// # async fn example(db: &quantoxide::Database) -> Result<(), Box<dyn std::error::Error>> {
    /// use chrono::{Duration, Utc};
    ///
    /// let to = Utc::now();
    /// let from = to - Duration::days(30);
    /// let settlements = db.funding_settlements().get_settlements(from, to).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn funding_settlements(&self) -> &(dyn FundingSettlementsRepositoryRead + '_) {
        self.funding_settlements.as_ref()
    }

    /// Returns read-only access to running trade recovery data.
    ///
    /// ```rust,no_run
    /// # async fn example(db: &quantoxide::Database) -> Result<(), Box<dyn std::error::Error>> {
    /// let account_id = uuid::Uuid::new_v4();
    /// let running_trades = db
    ///     .running_trades()
    ///     .get_running_trades_map(account_id)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn running_trades(&self) -> &(dyn RunningTradesRepositoryRead + '_) {
        self.running_trades.as_ref()
    }
}
