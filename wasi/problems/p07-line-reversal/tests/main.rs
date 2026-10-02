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

        socket.send(b"/connect/12345/".to_vec()).await.unwrap();

        let data = socket.recv().await.unwrap();
        assert_eq!(b"/ack/12345/0/".as_slice(), &data);

        socket.send(b"/data/12345/0/hello/".to_vec()).await.unwrap();

        let data = socket.recv().await.unwrap();
        assert_eq!(b"/ack/12345/5/".as_slice(), &data);

        socket
            .send(b"/data/12345/5/ world\n/".to_vec())
            .await
            .unwrap();

        let data = socket.recv().await.unwrap();
        assert_eq!(b"/ack/12345/12/".as_slice(), &data);

        let data = socket.recv().await.unwrap();
        assert_eq!(b"/data/12345/0/dlrow olleh\n/".as_slice(), &data);

        socket.send(b"/ack/12345/12/".to_vec()).await.unwrap();

        socket
            .send(b"/data/12345/12/pippo pluto/".to_vec())
            .await
            .unwrap();

        let data = socket.recv().await.unwrap();
        assert_eq!(
            b"/ack/12345/23/".as_slice(),
            &data,
            "invalid packet {:?}",
            std::str::from_utf8(&data)
        );

        socket
            .send(b"/data/12345/23/\npaperino/".to_vec())
            .await
            .unwrap();

        let data = socket.recv().await.unwrap();
        assert_eq!(b"/ack/12345/32/".as_slice(), &data);

        let data = socket.recv().await.unwrap();
        assert_eq!(b"/data/12345/12/otulp oppip\n/".as_slice(), &data);

        socket.send(b"/ack/12345/24/".to_vec()).await.unwrap();

        socket.send(b"/data/12345/32/\n/".to_vec()).await.unwrap();
        let data = socket.recv().await.unwrap();
        assert_eq!(b"/ack/12345/33/".as_slice(), &data);

        let data = socket.recv().await.unwrap();
        assert_eq!(b"/data/12345/24/onirepap\n/".as_slice(), &data);

        socket.send(b"/ack/12345/33/".to_vec()).await.unwrap();

        socket.send(b"/data/12345/33/\n/".to_vec()).await.unwrap();

        let data = socket.recv().await.unwrap();
        assert_eq!(b"/ack/12345/34/".as_slice(), &data);

        let data = socket.recv().await.unwrap();
        assert_eq!(b"/data/12345/33/\n/".as_slice(), &data);

        socket.send(b"/close/12345/".to_vec()).await.unwrap();

        let data = socket.recv().await.unwrap();
        assert_eq!(b"/close/12345/".as_slice(), &data);
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
        p07_line_reversal::run(socket).await.unwrap();
    })
    .await;

    info!("spawned app {address}:{port}");

    (address.to_string(), port)
}
