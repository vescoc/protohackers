use std::rc::Rc;

use wasi_async::net::TcpListener;

use clap::Parser;

use tracing::info;

use p05_mob_in_the_middle::{BOGUSCOIN, run};

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    address: String,

    #[arg(long, default_value_t = 10000)]
    port: u16,

    #[arg(long, default_value = "chat.protohackers.com")]
    chat_address: String,

    #[arg(long, default_value_t = 16963)]
    chat_port: u16,

    #[arg(long, default_value_t = BOGUSCOIN.to_string())]
    boguscoin: String,
}

fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("start");

    let result = wasi_async_runtime::block_on(|reactor| async move {
        let listener =
            TcpListener::bind(reactor.clone(), format!("{}:{}", args.address, args.port)).await?;

        run(
            reactor,
            listener,
            Rc::new(args.chat_address),
            args.chat_port,
            Rc::new(args.boguscoin),
        )
        .await
    });

    info!("done: {result:?}");

    Ok(result?)
}
