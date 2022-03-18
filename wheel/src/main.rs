use clap::Parser;
use runkv_wheel::error::{err, Result};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value = "etc/wheel.toml")]
    config_file_path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).map_err(err)?;

    Ok(())
}
