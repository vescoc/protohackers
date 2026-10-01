use std::sync::Once;

use tracing::info;

use wasi_async::net::UdpSocket;
use wasi_async_runtime::Reactor;

#[test]
fn test_session() {
    Reactor::block_on(|_| async move {
        let (address, port) = spawn_app().await;

        let socket = UdpSocket::bind("127.0.0.1:0".to_string()).await.unwrap();

        socket.connect(format!("{address}:{port}")).await.unwrap();

        socket.send(b"foo=bar".to_vec()).await.unwrap();

        socket.send(b"foo=bar".to_vec()).await.unwrap();

        socket.send(b"foo".to_vec()).await.unwrap();
        let data = socket.recv().await.unwrap();
        assert_eq!("foo=bar", std::str::from_utf8(&data).unwrap());

        socket.send(b"foo=bar=baz".to_vec()).await.unwrap();

        socket.send(b"foo".to_vec()).await.unwrap();
        let data = socket.recv().await.unwrap();
        assert_eq!("foo=bar=baz", std::str::from_utf8(&data).unwrap());

        socket.send(b"foo=".to_vec()).await.unwrap();

        socket.send(b"foo".to_vec()).await.unwrap();
        let data = socket.recv().await.unwrap();
        assert_eq!("foo=", std::str::from_utf8(&data).unwrap());

        socket.send(b"foo===".to_vec()).await.unwrap();

        socket.send(b"foo".to_vec()).await.unwrap();
        let data = socket.recv().await.unwrap();
        assert_eq!("foo===", std::str::from_utf8(&data).unwrap());

        socket.send(b"=foo".to_vec()).await.unwrap();

        socket.send(b"".to_vec()).await.unwrap();
        let data = socket.recv().await.unwrap();
        assert_eq!("=foo", std::str::from_utf8(&data).unwrap());

        socket.send(b"version=ignored".to_vec()).await.unwrap();

        socket.send(b"version".to_vec()).await.unwrap();
        let data = socket.recv().await.unwrap();
        assert_eq!(
            "version=unusual-database-program 1.0.0",
            std::str::from_utf8(&data).unwrap()
        );
    });
}

async fn spawn_app() -> (String, u16) {
    static INIT_TRACING_SUBSCRIBER: Once = Once::new();
    INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);

    let address = "127.0.0.1";

    let socket = UdpSocket::bind(format!("{address}:0")).await.unwrap();
    let port = socket
        .local_addr()
        .expect("cannot get local address")
        .port();

    Reactor::spawn_in_current(async move {
        p04_unusual_database_program::run(socket).await.unwrap();
    })
    .await;

    info!("spawned app {address}:{port}");

    (address.to_string(), port)
}
