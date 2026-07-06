use std::sync::Arc;
#[cfg(feature = "postgres")]
use std::time;

use chrono::Duration;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
use lazy_static::lazy_static;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgPoolOptions;
#[cfg(feature = "sqlite")]
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

pub(crate) mod error;
pub(crate) mod models;

/// Candles are only marked stable (skip re-fetch) once their time is at least this far in the
/// past. The API may return slightly different OHLC values for recent candles across requests.
pub(crate) const CANDLE_STABLE_AGE: Duration = Duration::hours(1);

#[cfg(feature = "postgres")]
mod postgres;
mod repositories;
#[cfg(feature = "sqlite")]
mod sqlite;

pub use repositories::{
    FundingSettlementsRepositoryRead, OhlcCandlesRepositoryRead, PriceTicksRepositoryRead,
    RunningTradesRepositoryRead,
};

use error::{DbError, Result};
#[cfg(feature = "postgres")]
use postgres::{
    funding_settlements::PgFundingSettlementsRepo, ohlc_candles::PgOhlcCandlesRepo,
    price_ticks::PgPriceTicksRepo, running_trades::PgRunningTradesRepo,
};
use repositories::{
    FundingSettlementsRepository, OhlcCandlesRepository, PriceTicksRepository,
    RunningTradesRepository,
};
#[cfg(feature = "sqlite")]
use sqlite::{
    SqliteFundingSettlementsRepo, SqliteOhlcCandlesRepo, SqlitePriceTicksRepo,
    SqliteRunningTradesRepo,
};
#[cfg(feature = "sqlite")]
use std::str::FromStr;

#[cfg(feature = "postgres")]
lazy_static! {
    /// Default [`PgPoolOptions`] used by [`Database::new`] for PostgreSQL URLs.
    pub static ref DEFAULT_PG_POOL_OPTIONS: PgPoolOptions = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(time::Duration::from_secs(60));
}

#[cfg(feature = "sqlite")]
lazy_static! {
    /// Default [`SqlitePoolOptions`] used by [`Database::new`] for SQLite URLs.
    pub static ref DEFAULT_SQLITE_POOL_OPTIONS: SqlitePoolOptions = SqlitePoolOptions::new();
}

/// Pool options used when constructing a [`Database`].
#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum DatabasePoolOptions {
    /// PostgreSQL pool options.
    #[cfg(feature = "postgres")]
    Postgres(PgPoolOptions),

    /// SQLite pool options.
    #[cfg(feature = "sqlite")]
    Sqlite(SqlitePoolOptions),
}

#[cfg(feature = "postgres")]
impl From<PgPoolOptions> for DatabasePoolOptions {
    fn from(pool_options: PgPoolOptions) -> Self {
        Self::Postgres(pool_options)
    }
}

#[cfg(feature = "sqlite")]
impl From<SqlitePoolOptions> for DatabasePoolOptions {
    fn from(pool_options: SqlitePoolOptions) -> Self {
        Self::Sqlite(pool_options)
    }
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
impl DatabasePoolOptions {
    fn for_database_url(database_url: &str) -> Result<Self> {
        let scheme = database_url
            .split_once(':')
            .map(|(scheme, _)| scheme)
            .unwrap_or(database_url);

        match scheme {
            "postgres" | "postgresql" => {
                #[cfg(feature = "postgres")]
                {
                    Ok(Self::Postgres(DEFAULT_PG_POOL_OPTIONS.clone()))
                }
                #[cfg(not(feature = "postgres"))]
                {
                    Err(DbError::UnsupportedDatabaseUrl(scheme.to_string()))
                }
            }
            "sqlite" => {
                #[cfg(feature = "sqlite")]
                {
                    Ok(Self::Sqlite(DEFAULT_SQLITE_POOL_OPTIONS.clone()))
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    Err(DbError::UnsupportedDatabaseUrl(scheme.to_string()))
                }
            }
            _ => Err(DbError::UnsupportedDatabaseUrl(scheme.to_string())),
        }
    }
}

/// Primary database interface for market data persistence and retrieval.
///
/// Provides access to repositories for OHLC candle data, price tick data, and running trade
/// information. Backend constructors initialize the underlying storage engine with automatic
/// migrations.
pub struct Database {
    pub(crate) ohlc_candles: Box<dyn OhlcCandlesRepository>,
    pub(crate) price_ticks: Box<dyn PriceTicksRepository>,
    pub(crate) running_trades: Box<dyn RunningTradesRepository>,
    pub(crate) funding_settlements: Box<dyn FundingSettlementsRepository>,
}

impl Database {
    fn from_repositories(
        ohlc_candles: Box<dyn OhlcCandlesRepository>,
        price_ticks: Box<dyn PriceTicksRepository>,
        running_trades: Box<dyn RunningTradesRepository>,
        funding_settlements: Box<dyn FundingSettlementsRepository>,
    ) -> Arc<Self> {
        Arc::new(Self {
            ohlc_candles,
            price_ticks,
            running_trades,
            funding_settlements,
        })
    }

    /// Creates a database instance with default backend pool options and runs migrations.
    ///
    /// The backend is selected from the database URL scheme. PostgreSQL URLs use the default
    /// PostgreSQL pool; SQLite URLs create missing database files automatically.
    ///
    /// Supported URL schemes depend on enabled crate features:
    /// - `postgres://` and `postgresql://` require the `postgres` feature.
    /// - `sqlite:` requires the `sqlite` feature.
    ///
    /// ```rust,no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use quantoxide::Database;
    ///
    /// let db_url = std::env::var("DATABASE_URL")?;
    /// let db = Database::new(&db_url).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub async fn new(database_url: &str) -> Result<Arc<Self>> {
        let pool_options = DatabasePoolOptions::for_database_url(database_url)?;
        Self::with_pool_options(database_url, pool_options).await
    }

    /// Creates a database instance with caller-provided pool options and runs migrations.
    ///
    /// Pass backend-specific pool options to select the backend and tune the connection pool.
    /// [`PgPoolOptions`] and [`SqlitePoolOptions`] are re-exported by this crate when their
    /// corresponding backend features are enabled so consumers can configure pools without
    /// depending on `sqlx` directly.
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
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub async fn with_pool_options(
        database_url: &str,
        pool_options: impl Into<DatabasePoolOptions>,
    ) -> Result<Arc<Self>> {
        match pool_options.into() {
            #[cfg(feature = "postgres")]
            DatabasePoolOptions::Postgres(pool_options) => {
                Self::postgres_with_pool_options(database_url, pool_options).await
            }
            #[cfg(feature = "sqlite")]
            DatabasePoolOptions::Sqlite(pool_options) => {
                Self::sqlite_with_pool_options(database_url, pool_options).await
            }
        }
    }

    #[cfg(feature = "postgres")]
    async fn postgres_with_pool_options(
        database_url: &str,
        pool_options: PgPoolOptions,
    ) -> Result<Arc<Self>> {
        let pool = pool_options
            .connect(database_url)
            .await
            .map_err(DbError::Connection)?;

        sqlx::migrate!("./migrations/postgres")
            .run(&pool)
            .await
            .map_err(DbError::Migration)?;

        let pool = Arc::new(pool);
        let ohlc_candles = Box::new(PgOhlcCandlesRepo::new(pool.clone()));
        let price_ticks = Box::new(PgPriceTicksRepo::new(pool.clone()));
        let running_trades = Box::new(PgRunningTradesRepo::new(pool.clone()));
        let funding_settlements = Box::new(PgFundingSettlementsRepo::new(pool.clone()));

        Ok(Self::from_repositories(
            ohlc_candles,
            price_ticks,
            running_trades,
            funding_settlements,
        ))
    }

    #[cfg(feature = "sqlite")]
    async fn sqlite_with_pool_options(
        database_url: &str,
        pool_options: SqlitePoolOptions,
    ) -> Result<Arc<Self>> {
        let connect_options = SqliteConnectOptions::from_str(database_url)
            .map_err(DbError::Connection)?
            .create_if_missing(true);

        let pool = pool_options
            .connect_with(connect_options)
            .await
            .map_err(DbError::Connection)?;

        sqlx::migrate!("./migrations/sqlite")
            .run(&pool)
            .await
            .map_err(DbError::Migration)?;

        let pool = Arc::new(pool);
        let ohlc_candles = Box::new(SqliteOhlcCandlesRepo::new(pool.clone()));
        let price_ticks = Box::new(SqlitePriceTicksRepo::new(pool.clone()));
        let running_trades = Box::new(SqliteRunningTradesRepo::new(pool.clone()));
        let funding_settlements = Box::new(SqliteFundingSettlementsRepo::new(pool.clone()));

        Ok(Self::from_repositories(
            ohlc_candles,
            price_ticks,
            running_trades,
            funding_settlements,
        ))
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

#[cfg(all(test, feature = "sqlite"))]
mod sqlite_tests {
    use std::{env, fs};

    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn database_new_sqlite_creates_file_and_runs_migrations() {
        let path = env::temp_dir().join(format!("quantoxide-{}.sqlite", Uuid::new_v4()));
        let database_url = format!("sqlite:{}", path.display());
        assert!(!path.exists());

        let db = Database::new(&database_url).await.unwrap();
        assert!(path.exists());

        assert_eq!(db.price_ticks().get_latest_entry().await.unwrap(), None);

        drop(db);
        for suffix in ["", "-shm", "-wal"] {
            let _ = fs::remove_file(format!("{}{}", path.display(), suffix));
        }
    }
}
