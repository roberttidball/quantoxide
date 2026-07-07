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

    println!("Database ready. Initializing `LiveTradeEngine`...");

    let operator = RawOperatorTemplate::boxed();

    let live_engine = LiveTradeEngine::with_raw_operator(
        LiveTradeConfig::default(),
        db,
        key,
        secret,
        passphrase,
        operator,
    )?;

    println!("Initialization OK. Launching `LiveTui`...");

    let live_tui = LiveTui::launch(TuiConfig::default(), None).await?;
    live_tui.couple(live_engine).await?;

    let final_status = live_tui.until_stopped().await;
    println!("`LiveTui` status: {final_status}");

    Ok(())
}
