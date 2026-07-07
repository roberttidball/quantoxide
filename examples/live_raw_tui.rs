//! Example demonstrating how to run the live trading process with a raw operator, using its TUI
//! abstraction.

use std::env;

use dotenvy::dotenv;

use quantoxide::{
    Database,
    error::Result,
    trade::{LiveTradeConfig, LiveTradeEngine},
    tui::{LiveTui, TuiConfig},
};

#[path = "operators/mod.rs"]
mod operators;
use operators::raw::RawOperatorTemplate;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let db_url = env::var("DATABASE_URL").map_err(|_| "`DATABASE_URL` is not set")?;
    let key = env::var("LNM_API_KEY").map_err(|_| "`LNM_API_KEY` is not set")?;
    let secret = env::var("LNM_API_SECRET").map_err(|_| "`LNM_API_SECRET` is not set")?;
    let passphrase =
        env::var("LNM_API_PASSPHRASE").map_err(|_| "`LNM_API_PASSPHRASE` is not set")?;

    println!("Initializing database...");

    let db = Database::new(&db_url).await?;

    println!("Database ready. Launching `LiveTui`...");

    let live_tui = LiveTui::launch(TuiConfig::default(), None).await?;

    // Build the operator and engine after launch so the operator can log through `live_tui`.
    // `until_stopped` restores the terminal before any `stdout`/`stderr` output.

    let init_result = async {
        let operator = RawOperatorTemplate::boxed().enable_tui_logger(live_tui.as_logger());

        let live_engine = LiveTradeEngine::with_raw_operator(
            LiveTradeConfig::default(),
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
