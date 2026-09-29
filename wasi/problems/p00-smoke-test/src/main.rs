use wasi::sockets::network::ErrorCode;

use wasi_async::net::TcpListener;
use wasi_async_runtime::{Reactor, block_on};

use tracing::{debug, info, instrument};

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
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

    let result: Result<_, ErrorCode> = block_on(|_| async move {
        let socket = TcpListener::bind(format!("{}:{}", args.address, args.port)).await?;

        loop {
            let (stream, address) = socket.accept().await?;

            debug!("new client: {address:?}");

            Reactor::spawn_in_current(async move {
                let result = p00_smoke_test::run(address, stream).await;
                info!("result: {result:?}");
            })
            .await;
        }
    });

    info!("done: {result:?}");

    Ok(result?)
}
