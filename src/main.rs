use clap::Parser;
use tokio::sync::mpsc;
use tracing::info;

use std::time::Duration;

mod simu;
use simu::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    let (stop_tx, stop_rx) = mpsc::channel(8);

    let simu = ChargePointSimu::new(
        Duration::from_secs(args.charging_interval as u64),
        args.with_heartbeats,
    );
    let hdl = tokio::spawn(simu.into_task(stop_rx));

    info!("OCPP client simu started");
    tokio::signal::ctrl_c().await?;

    info!("Shutting down...");

    let _ = stop_tx.send(()).await;
    let _ = hdl.await;

    info!("Shut down complete.");
    Ok(())
}

#[derive(Debug, clap::Parser)]
#[command(version, about = "Runs an OCPP client")]
#[command(propagate_version = true)]
pub struct Args {
    #[clap(
        long,
        help = "Interval between charging steps (s)",
        default_value_t = 3
    )]
    pub charging_interval: u32,

    #[clap(long, help = "Whether to send heartbeats")]
    pub with_heartbeats: bool,
}
