use std::sync::Once;

use wasi_async::net::TcpStream;
use wasi_async_runtime::{Reactor, block_on};

use tracing::info;

use wasi_async::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use wasi_async::net::TcpListener;

#[test]
fn test_session() {
    block_on(async_test_session);
}

async fn async_test_session(reactor: Reactor) {
    let (address, port) = spawn_app(reactor.clone()).await;

    let mut stream = TcpStream::connect(reactor, format!("{address}:{port}"))
        .await
        .expect("cannot connect");
    let (mut read, mut write) = stream.split();

    let payload = b"ciccio cunicio";
    write.write_all(payload).await.unwrap();
    write.flush().await.unwrap();
    info!("done write");

    let data = read.read(1024).await.expect("cannot read");
    info!("done read");

    assert_eq!(payload.as_slice(), data);
}

async fn spawn_app(reactor: Reactor) -> (String, u16) {
    static INIT_TRACING_SUBSCRIBER: Once = Once::new();
    INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);

    let address = "127.0.0.1";

    let listener = TcpListener::bind(reactor.clone(), format!("{address}:0"))
        .await
        .expect("cannot bind");
    let port = listener.local_addr().expect("cannot get local addr").port();

    reactor.spawn(async move {
        loop {
            let (socket, remote_address) = listener.accept().await.expect("cannot accept");

            p00_smoke_test::run(remote_address, socket).await.unwrap();
        }
    });

    info!("spawned app {address}:{port}");

    (address.to_string(), port)
}
