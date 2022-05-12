#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use std::fs::read_to_string;

use clap::Parser;
use runkv_rudder::config::RudderConfig;
use runkv_rudder::error::{Error, Result};
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
    tracing::subscriber::set_global_default(subscriber).map_err(Error::err)?;

    let args = Args::parse();
    info!("args: {:?}", args);

    let config: RudderConfig =
        toml::from_str(&read_to_string(&args.config_file_path).map_err(Error::err)?)
            .map_err(Error::config_err)?;
    info!("config: {:?}", config);

    let (rudder, workers) = build_rudder(&config).await?;
    bootstrap_rudder(&config, rudder, workers).await
}
