//! Example demonstrating how to run the backtest process with a raw operator, using its TUI
//! abstraction.

use std::env;

use dotenvy::dotenv;

use quantoxide::{
    Database,
    error::Result,
    sync::PriceHistoryState,
    trade::{BacktestConfig, BacktestEngine},
    tui::{BacktestTui, TuiConfig},
};

#[path = "operators/mod.rs"]
mod operators;
#[path = "util/mod.rs"]
mod util;

use operators::raw::RawOperatorTemplate;
use util::input;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let db_url = env::var("DATABASE_URL").map_err(|_| "`DATABASE_URL` is not set")?;

    println!("Initializing database...");

    let db = Database::new(&db_url).await?;

    println!("Database ready. Evaluating `PriceHistoryState`...");

    let price_history_state = PriceHistoryState::evaluate(&db).await?;

    println!("\n{price_history_state}\n");

    if price_history_state.bound_end().is_none() {
        println!("Some price history must be available in the local database to run the backtest.");
        println!("Run a synchronization example, in backfill mode, to fetch historical data.");

        return Ok(());
    }

    println!("Please provide the backtest parameters.");
    println!(
        "Note: There must be data available for the lookback period, if any, of the raw operator used.\n"
    );

    let start_time = input::prompt_date("Start date (YYYY-MM-DD): ")?;

    let start_balance =
        input::prompt_balance("Start balance (sats, default: 10000000): ", 10_000_000)?;

    let end_time = input::prompt_date("End date (YYYY-MM-DD): ")?;

    println!("\nBacktest Configuration:");
    println!("Start date: {}", start_time.format("%Y-%m-%d %H:%M %Z"));
    println!("Start balance: {}", start_balance);
    println!("End date: {}\n", end_time.format("%Y-%m-%d %H:%M %Z"));

    println!("Initializing `BacktestEngine`...");

    let operator = RawOperatorTemplate::boxed();

    let backtest_engine = BacktestEngine::with_raw_operator(
        BacktestConfig::default(),
        db,
        operator,
        start_time,
        start_balance,
        end_time,
    )
    .await?;

    println!("Launching `BacktestTui`...");

    // Be careful with logging in backtests. Since many iterations can be processed per second,
    // excessive per-iteration logging can create a severe performance bottleneck.

    let backtest_tui = BacktestTui::launch(TuiConfig::default(), None).await?;
    backtest_tui.couple(backtest_engine).await?;

    let final_status = backtest_tui.until_stopped().await;
    println!("`BacktestTui` status: {final_status}");

    Ok(())
}
