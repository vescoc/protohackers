use std::pin;
use std::sync::Once;
use std::time::Duration;

use tracing::info;

use wasi_async::net::{TcpListener, TcpStream};
use wasi_async::time::timeout;
use wasi_async_runtime::Reactor;

use p10_voracious_code_storage::{
    io::{BufReader, WriteLine},
    run,
};

const TIMEOUT: Duration = Duration::from_millis(100);

#[test]
#[allow(clippy::too_many_lines)]
fn test_session() {
    Reactor::block_on(|_| async {
        let (address, port) = spawn_app().await;

        let mut stream = TcpStream::connect(format!("{address}:{port}"))
            .await
            .unwrap();
        let (read, mut write) = stream.split();

        let mut read = BufReader::new(read);

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        timeout(TIMEOUT, pin::pin!(write.write_line(b"HELP")))
            .await
            .unwrap()
            .unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "OK usage: HELP|GET|PUT|LIST\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        write.write_line(b"LIST /").await.unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "OK 0\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        write.write_line(b"PUT /test").await.unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "ERR usage: PUT file length newline data\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        write.write_line(b"GET").await.unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "ERR usage: GET file [revision]\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        write.write_line(b"GET /test").await.unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "ERR no such file\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        write.write_line(b"PUT test").await.unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "ERR illegal file name\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        write.write_line(b"PUT /dir/test 2").await.unwrap();
        write.write_line(b"A").await.unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "OK r1\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        write.write_line(b"GET /dir").await.unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "ERR no such file\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        write.write_line(b"PUT /.. 2").await.unwrap();
        write.write_line(b"A").await.unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "OK r1\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        write.write_line(b"/LIST").await.unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "ERR illegal method: /LIST\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        write.write_line(b"LIST").await.unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "ERR usage: LIST dir\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        write.write_line(b"LIST /").await.unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "OK 2\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, ".. r1\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "dir/ DIR\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");

        write.write_line(b"PUT //test").await.unwrap();

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "ERR illegal file name\n");

        let buffer = timeout(TIMEOUT, pin::pin!(read.read_line()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer, "READY\n");
    });
}

async fn spawn_app() -> (String, u16) {
    static TRACING_SUBSCRIBER_INIT: Once = Once::new();
    TRACING_SUBSCRIBER_INIT.call_once(tracing_subscriber::fmt::init);

    let address = "127.0.0.1";

    let listener = TcpListener::bind(format!("{address}:0"))
        .await
        .expect("cannot bind app");
    let port = listener
        .local_addr()
        .expect("cannot get local address")
        .port();

    Reactor::spawn_in_current(async move {
        run(listener).await.expect("run failed");
    })
    .await;

    info!("spawned app {address}:{port}");

    (address.to_string(), port)
}
