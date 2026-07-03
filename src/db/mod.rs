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
    /// Default PostgreSQL pool options used by [`Database::new`].
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
    /// Creates a new database instance and runs migrations.
    ///
    /// Establishes a connection pool to the PostgreSQL database and automatically applies any
    /// pending migrations. Returns an error if the connection fails or migrations cannot be
    /// applied.
    pub async fn new(postgres_db_url: &str) -> Result<Arc<Self>> {
        Self::with_pool_options(postgres_db_url, DEFAULT_PG_POOL_OPTIONS.clone()).await
    }

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
}
