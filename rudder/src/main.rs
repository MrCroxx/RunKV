mod error;
pub use error::*;
mod meta;
pub use meta::*;
mod service;
pub use service::*;
mod store;
pub use store::*;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
}
