use clap::Parser;
use tokio::net::TcpListener;

use tracing::info;

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    address: String,

    #[arg(long, default_value_t = 10000)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("start");

    let listener = TcpListener::bind(format!("{}:{}", args.address, args.port)).await?;
    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(p01_prime_time::handler(socket));
    }
}
