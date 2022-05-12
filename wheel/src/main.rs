#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use std::fs::read_to_string;

use clap::Parser;
use runkv_wheel::config::WheelConfig;
use runkv_wheel::error::{Error, Result};
use runkv_wheel::{bootstrap_wheel, build_wheel};
use tracing::info;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value = "etc/wheel.toml")]
    config_file_path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).map_err(Error::err)?;

    let args = Args::parse();
    info!("args: {:?}", args);

    let config: WheelConfig =
        toml::from_str(&read_to_string(&args.config_file_path).map_err(Error::err)?)
            .map_err(Error::config_err)?;
    info!("config: {:?}", config);

    let (wheel, workers) = build_wheel(&config).await?;
    bootstrap_wheel(&config, wheel, workers).await
}
