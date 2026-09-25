use std::sync::Once;

use futures::StreamExt;

use wasi_async::codec::{FramedRead, LinesDecoder};
use wasi_async::io::{AsyncWrite, AsyncWriteExt};
use wasi_async::net::{TcpListener, TcpStream};

use tracing::info;

#[test]
fn test_invalid_number_float() {
    wasi_async_runtime::block_on(|reactor| async move {
        let (address, port) = spawn_app(reactor.clone()).await;

        let mut stream = TcpStream::connect(reactor.clone(), format!("{address}:{port}"))
            .await
            .expect("cannot connect");
        let (read, mut write) = stream.split();

        let payload = br#"{"method":"isPrime","number":3.0}"#;
        write.write_all(payload).await.unwrap();
        write.write(b"\n").await.unwrap();
        write.flush().await.unwrap();

        {
            let mut read = std::pin::pin!(FramedRead::new(read, LinesDecoder::new()).into_stream());

            let line = read.next().await.expect("stream empty").unwrap();

            let response: p01_prime_time::Response = serde_json::from_slice(&line).unwrap();
            assert!(!response.prime);
        }

        stream.close().await.ok();
    });
}

#[test]
fn test_invalid_number() {
    wasi_async_runtime::block_on(|reactor| async move {
        let (address, port) = spawn_app(reactor.clone()).await;

        let mut stream = TcpStream::connect(reactor.clone(), format!("{address}:{port}"))
            .await
            .expect("cannot connect");
        let (read, mut write) = stream.split();
        {
            let mut read = std::pin::pin!(FramedRead::new(read, LinesDecoder::new()).into_stream());

            let payload = br#"{"method":"isPrime","number":"not a number"}"#;
            write.write_all(payload).await.unwrap();
            write.write(b"\n").await.unwrap();
            write.flush().await.unwrap();

            let line = read.next().await.expect("stream empty").unwrap();

            assert_eq!(&line, b"MALFORMED");
        }
        
        stream.close().await.ok();
    });
}

#[test]
fn test_ignore_field() {
    wasi_async_runtime::block_on(|reactor| async move {
        let (address, port) = spawn_app(reactor.clone()).await;

        let mut stream = TcpStream::connect(reactor.clone(), format!("{address}:{port}"))
            .await
            .expect("cannot connect");
        let (read, mut write) = stream.split();
        {
            let mut read = std::pin::pin!(FramedRead::new(read, LinesDecoder::new()).into_stream());

            let payload = br#"{"method":"isPrime","number":3,"ignored":false}"#;
            write.write_all(payload).await.unwrap();
            write.write(b"\n").await.unwrap();
            write.flush().await.unwrap();

            let line = read.next().await.unwrap().unwrap();

            let response: p01_prime_time::Response = serde_json::from_slice(&line).unwrap();
            assert!(response.prime);
        }

        stream.close().await.ok();
    });
}

#[test]
fn test_invalid_message() {
    wasi_async_runtime::block_on(|reactor| async move {
        let (address, port) = spawn_app(reactor.clone()).await;

        let mut stream = TcpStream::connect(reactor.clone(), format!("{address}:{port}"))
            .await
            .expect("cannot connect");
        let (read, mut write) = stream.split();
        {
            let mut read = std::pin::pin!(FramedRead::new(read, LinesDecoder::new()).into_stream());

            let payload = br#"{"method":"bho","number":3}"#;
            write.write_all(payload).await.unwrap();
            write.write(b"\n").await.unwrap();
            write.flush().await.unwrap();

            let line = read.next().await.unwrap().unwrap();

            assert_eq!(&line, b"MALFORMED");
        }

        stream.close().await.ok();
    });
}

#[test]
fn test_invalid_json() {
    wasi_async_runtime::block_on(|reactor| async move {
        let (address, port) = spawn_app(reactor.clone()).await;

        let mut stream = TcpStream::connect(reactor.clone(), format!("{address}:{port}"))
            .await
            .expect("cannot connect");
        let (read, mut write) = stream.split();
        {
            let mut read = std::pin::pin!(FramedRead::new(read, LinesDecoder::new()).into_stream());

            let payload = br#"{"method":"bho","number":3"#;
            write.write_all(payload).await.unwrap();
            write.write(b"\n").await.unwrap();
            write.flush().await.unwrap();

            let line = read.next().await.unwrap().unwrap();

            assert_eq!(&line, b"MALFORMED");
        }
    });
}

#[test]

fn test_is_prime() {
    wasi_async_runtime::block_on(|reactor| async move {
        let (address, port) = spawn_app(reactor.clone()).await;

        let mut stream = TcpStream::connect(reactor.clone(), format!("{address}:{port}"))
            .await
            .expect("cannot connect");
        let (read, mut write) = stream.split();
        {
            let mut read = std::pin::pin!(FramedRead::new(read, LinesDecoder::new()).into_stream());

            let payload = br#"{"method":"isPrime","number":3}"#;
            write.write(payload).await.unwrap();
            write.write(b"\n").await.unwrap();
            write.flush().await.unwrap();

            let line = read.next().await.unwrap().unwrap();

            let response: p01_prime_time::Response = serde_json::from_slice(&line).unwrap();
            assert!(response.prime);
        }

        stream.close().await.ok();
    });
}

#[test]
fn test_not_is_prime() {
    wasi_async_runtime::block_on(|reactor| async move {
        let (address, port) = spawn_app(reactor.clone()).await;

        let mut stream = TcpStream::connect(reactor.clone(), format!("{address}:{port}"))
            .await
            .expect("cannot connect");
        let (read, mut write) = stream.split();
        {
            let mut read = std::pin::pin!(FramedRead::new(read, LinesDecoder::new()).into_stream());

            let payload = br#"{"method":"isPrime","number":10}"#;
            write.write_all(payload).await.unwrap();
            write.write(b"\n").await.unwrap();
            write.flush().await.unwrap();

            let line = read.next().await.unwrap().unwrap();

            let response: p01_prime_time::Response = serde_json::from_slice(&line).unwrap();
            assert!(!response.prime);
        }

        stream.close().await.ok();
    });
}

async fn spawn_app(reactor: wasi_async_runtime::Reactor) -> (String, u16) {
    static INIT_TRACING_SUBSCRIBER: Once = Once::new();
    INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);

    let address = "127.0.0.1";

    let listener = TcpListener::bind(reactor.clone(), format!("{address}:0"))
        .await
        .expect("cannot bind");
    let port = listener
        .local_addr()
        .expect("cannot get local address")
        .port();

    reactor.spawn(async move {
        loop {
            let (stream, remote_address) = listener.accept().await.expect("cannot accept");

            p01_prime_time::run(remote_address, stream).await.ok();
        }
    });

    info!("spawned app {address}:{port}");

    (address.to_string(), port)
}
