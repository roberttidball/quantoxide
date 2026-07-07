//! Example demonstrating direct interaction with the live trading process, without the TUI
//! abstraction.

use std::env;

use dotenvy::dotenv;
use tokio::time::{self, Duration};

use quantoxide::{
    Database,
    error::Result,
    trade::{LiveTradeConfig, LiveTradeEngine, LiveTradeUpdate},
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

    let operator = RawOperatorTemplate::boxed().enable_stdout_logger();

    let live_engine = LiveTradeEngine::with_raw_operator(
        LiveTradeConfig::default(),
        db,
        key,
        secret,
        passphrase,
        operator,
    )?;

    println!("Initialization OK. Starting `LiveTradeEngine`...");

    let mut live_rx = live_engine.update_receiver();

    tokio::spawn(async move {
        loop {
            match live_rx.recv().await {
                Ok(live_update) => match live_update {
                    LiveTradeUpdate::Status(live_status) => {
                        println!("{live_status}");
                    }
                    LiveTradeUpdate::Signal(_) => {
                        // Raw operators don't produce signals
                    }
                    LiveTradeUpdate::ClosedTrade(closed_trade) => {
                        println!("{closed_trade}");
                    }
                    LiveTradeUpdate::ExecutorAction(action) => {
                        println!("{action}");
                    }
                    LiveTradeUpdate::TradingState(trading_state) => {
                        println!("{trading_state}");
                    }
                },
                Err(e) => {
                    eprint!("{:?}", e);
                    break;
                }
            }
        }
    });

    let live_controller = live_engine.start().await?;

    let final_status = live_controller.until_stopped().await;

    // Delay for printing all `live_rx` updates
    time::sleep(Duration::from_millis(100)).await;

    println!("Live trade status: {final_status}");

    Ok(())
}
