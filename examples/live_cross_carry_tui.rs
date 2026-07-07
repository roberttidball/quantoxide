//! Example demonstrating how to run the cross-margin carry trade with the live trading TUI.
//!
//! **Warning:** this example uses live LN Markets credentials and can place real cross-margin
//! market orders and transfer real account balance between isolated/free balance and cross margin.
//! Test the shared operator with `backtest_cross_carry_tui` before adapting this example for a live
//! account.

use std::env;

use dotenvy::dotenv;

use quantoxide::{
    Database,
    error::Result,
    models::PercentageCapped,
    trade::{LiveTradeConfig, LiveTradeEngine},
    tui::{LiveTui, TuiConfig},
};

#[path = "operators/mod.rs"]
mod operators;
#[path = "util/mod.rs"]
mod util;

use operators::cross_carry::{CrossCarryOperator, CrossCarryOperatorConfig};
use util::input;

const DEFAULT_HEDGE_PERC: f64 = 100.0;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let db_url = env::var("DATABASE_URL").map_err(|_| "`DATABASE_URL` is not set")?;
    let key = env::var("LNM_API_KEY").map_err(|_| "`LNM_API_KEY` is not set")?;
    let secret = env::var("LNM_API_SECRET").map_err(|_| "`LNM_API_SECRET` is not set")?;
    let passphrase =
        env::var("LNM_API_PASSPHRASE").map_err(|_| "`LNM_API_PASSPHRASE` is not set")?;

    println!("Stop now if you have not reviewed the operator and your LN Markets account state.\n");

    let hedge_perc = input::prompt_percentage_capped(
        &format!("Hedge percentage (default: {DEFAULT_HEDGE_PERC}): "),
        PercentageCapped::bounded(DEFAULT_HEDGE_PERC),
    )?;

    println!("\nLive Cross-Margin Carry Trade TUI Configuration:");
    println!("Hedge percentage: {:.2}%\n", hedge_perc.as_f64());

    println!("Initializing database...");

    let db = Database::new(&db_url).await?;

    println!("Database ready. Launching `LiveTui`...");

    let live_tui = LiveTui::launch(TuiConfig::default(), None).await?;

    // Build the operator and engine after launch so the operator can log through `live_tui`.
    // `until_stopped` restores the terminal before any `stdout`/`stderr` output.

    let init_result = async {
        let operator = CrossCarryOperator::boxed(CrossCarryOperatorConfig::default(), hedge_perc)
            .enable_tui_logger(live_tui.as_logger());

        let live_engine = LiveTradeEngine::with_raw_operator(
            LiveTradeConfig::default().with_shutdown_clean_up_trades(true),
            db,
            key,
            secret,
            passphrase,
            operator,
        )?;

        live_tui.couple(live_engine).await?;

        Ok(())
    }
    .await;

    if init_result.is_err() {
        let _ = live_tui.shutdown().await;
    }

    let final_status = live_tui.until_stopped().await;

    match &init_result {
        Ok(()) => println!("`LiveTui` status: {final_status}\n"),
        Err(error) => eprintln!("`LiveTui` initialization error: {error}\n"),
    }

    init_result
}
