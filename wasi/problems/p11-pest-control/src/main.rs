use wasi_async::net::TcpListener;
use wasi_async_runtime::Reactor;

use clap::Parser;

use tracing::info;

use p11_pest_control::{DefaultProvider, run};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    address: String,

    #[arg(long, default_value_t = 10000)]
    port: u16,

    #[arg(long, default_value = "pestcontrol.protohackers.com")]
    authority_server_address: String,

    #[arg(long, default_value_t = 20547)]
    authority_server_port: u16,
}

fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("start");

    Reactor::block_on(|_| async move {
        let socket = TcpListener::bind(format!("{}:{}", args.address, args.port)).await?;

        let authority_server_provider =
            DefaultProvider::new(args.authority_server_address, args.authority_server_port);

        Ok(run(socket, authority_server_provider).await?)
    })
}
