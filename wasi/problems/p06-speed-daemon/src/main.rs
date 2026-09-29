use clap::Parser;

use wasi_async::net::TcpListener;
use wasi_async_runtime::block_on;

use tracing::info;

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    address: String,

    #[arg(long, default_value_t = 10000)]
    port: u16,
}

fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("start");

    let result = block_on(|_| async move {
        let listener = TcpListener::bind(format!("{}:{}", args.address, args.port)).await?;

        p06_speed_daemon::run(listener).await
    });

    info!("done: {result:?}");

    Ok(result?)
}
