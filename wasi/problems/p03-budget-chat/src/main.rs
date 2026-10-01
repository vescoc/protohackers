use wasi_async::net::TcpListener;
use wasi_async_runtime::Reactor;

use tracing::{info, instrument};

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    address: String,

    #[arg(long, default_value_t = 10000)]
    port: u16,
}

#[instrument]
fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    info!("start");

    let args = Args::parse();

    let result = Reactor::block_on(|_| async move {
        let listener = TcpListener::bind(format!("{}:{}", args.address, args.port)).await?;

        p03_budget_chat::run(listener).await
    });

    info!("done: {result:?}");

    Ok(result?)
}
