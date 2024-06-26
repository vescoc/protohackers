use clap::Parser;
use tokio::net::TcpListener;

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

    let listener = TcpListener::bind(format!("{}:{}", args.address, args.port)).await?;
    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(p00_smoke_test::echo(socket));
    }
}
