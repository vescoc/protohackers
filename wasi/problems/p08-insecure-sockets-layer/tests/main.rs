use std::sync::Once;

use wasi_async::net::TcpStream;
use wasi_async_runtime::Reactor;

use tracing::info;

use wasi_async::io::{AsyncRead, AsyncWrite};
use wasi_async::net::TcpListener;

#[test]
fn test_session() {
    Reactor::block_on(async_test_session);
}

async fn async_test_session(reactor: Reactor) {
    let (address, port) = spawn_app(reactor).await;

    let mut stream = TcpStream::connect(format!("{address}:{port}"))
        .await
        .expect("cannot connect");
    let (mut read, mut write) = stream.split();

    send(&mut write, "02 7b 05 01 00").await;
    write.flush().await.unwrap();

    send(&mut write, "f2 20 ba 44 18 84 ba aa d0 26 44 a4 a8 7e").await;

    let packet = recv(&mut read).await;
    assert_eq!("72 20 ba d8 78 70 ee", &packet);

    send(&mut write, "6a 48 d6 58 34 44 d6 7a 98 4e 0c cc 94 31").await;

    let packet = recv(&mut read).await;
    assert_eq!("f2 d0 26 c8 a4 d8 7e", &packet);
}

#[allow(clippy::cast_possible_truncation)]
async fn send<W: AsyncWrite>(write: &mut W, packet: &str) {
    for value in packet.split_ascii_whitespace().map(|value| {
        value.chars().fold(0x0, |acc, digit| {
            acc * 16 + digit.to_digit(16).expect("invalid value") as u8
        })
    }) {
        write.write(&[value]).await.unwrap();
    }
}

async fn recv<R: AsyncRead>(read: &mut R) -> String {
    read.read(5000)
        .await
        .unwrap()
        .into_iter()
        .map(|value| format!("{value:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

async fn spawn_app(reactor: Reactor) -> (String, u16) {
    static INIT_TRACING_SUBSCRIBER: Once = Once::new();
    INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);

    let address = "127.0.0.1";

    let listener = TcpListener::bind(format!("{address}:0"))
        .await
        .expect("cannot bind");
    let port = listener.local_addr().expect("cannot get local addr").port();

    reactor.spawn(async move {
        loop {
            let (socket, remote_address) = listener.accept().await.expect("cannot accept");

            p08_insecure_sockets_layer::run(remote_address, socket)
                .await
                .unwrap();
        }
    });

    info!("spawned app {address}:{port}");

    (address.to_string(), port)
}
