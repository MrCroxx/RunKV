use std::fs::read_to_string;

use clap::Parser;
use runkv_exhauster::config::ExhausterConfig;
use runkv_exhauster::error::{config_err, Result};
use runkv_exhauster::{bootstrap_exhauster, build_exhauster};
use tracing::info;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value = "etc/exhauster.toml")]
    config_file_path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();
    info!("args: {:?}", args);

    let config: ExhausterConfig =
        toml::from_str(&read_to_string(&args.config_file_path)?).map_err(config_err)?;
    info!("config: {:?}", config);

    let (exhauster, workers) = build_exhauster(&config).await?;
    bootstrap_exhauster(&config, exhauster, workers).await
}
