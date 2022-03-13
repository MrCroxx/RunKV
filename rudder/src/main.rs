mod error;
pub use error::*;
mod meta;
pub use meta::*;
mod service;
pub use service::*;
mod version_manager;
pub use version_manager::*;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
}
