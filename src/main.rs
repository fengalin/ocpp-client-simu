use tokio::sync::mpsc;
use tracing::info;

mod simu;
use simu::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let (stop_tx, stop_rx) = mpsc::channel(8);

    let simu = ChargePointSimu::new();
    let hdl = tokio::spawn(simu.into_task(stop_rx));

    info!("OCPP client simu started");
    tokio::signal::ctrl_c().await?;

    info!("Shutting down...");

    let _ = stop_tx.send(()).await;
    let _ = hdl.await;

    info!("Shut down complete.");
    Ok(())
}
