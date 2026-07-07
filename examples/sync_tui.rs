//! Example demonstrating how to run the sync process using its TUI abstraction.

use std::env;

use dotenvy::dotenv;

use quantoxide::{
    Database,
    error::Result,
    sync::{SyncConfig, SyncEngine, SyncMode},
    tui::{SyncTui, TuiConfig},
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let db_url = env::var("DATABASE_URL").map_err(|_| "`DATABASE_URL` is not set")?;

    println!("Initializing database...");

    let db = Database::new(&db_url).await?;

    println!("Database ready. Initializing `SyncEngine`...");

    let config = SyncConfig::default();
    // How far back to fetch price history data can be configured with:
    // let config = config
    //     .with_price_history_reach_max() // or: .with_price_history_reach(specific_date)
    //     .with_funding_settlement_reach_max(); // or: .with_funding_settlement_reach(specific_date)

    let sync_engine = SyncEngine::new(config, db, SyncMode::Backfill)?;

    println!("Initialization OK. Launching `SyncTui`...");

    let sync_tui = SyncTui::launch(TuiConfig::default(), None).await?;
    sync_tui.couple(sync_engine)?;

    let final_status = sync_tui.until_stopped().await;
    println!("`SyncTui` status: {final_status}");

    Ok(())
}
