use std::fs::read_to_string;

use clap::Parser;
use runkv_rudder::config::RudderConfig;
use runkv_rudder::error::{config_err, err, Result};
use runkv_rudder::{bootstrap_rudder, build_rudder};
use tracing::info;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value = "etc/rudder.toml")]
    config_file_path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).map_err(err)?;

    let args = Args::parse();
    info!("args: {:?}", args);

    let config: RudderConfig =
        toml::from_str(&read_to_string(&args.config_file_path).map_err(err)?)
            .map_err(config_err)?;
    info!("config: {:?}", config);

    let rudder = build_rudder(&config).await?;
    bootstrap_rudder(&config, rudder).await
}
