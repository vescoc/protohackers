use std::sync::Once;

use futures::StreamExt;

use wasi_async::codec::{ChunksDecoder, FramedRead};
use wasi_async::io::{AsyncWrite, AsyncWriteExt};
use wasi_async::net::{TcpListener, TcpStream};

use tracing::info;

#[test]
fn test_session() {
    wasi_async_runtime::block_on(|reactor| async move {
        let (address, port) = spawn_app(reactor.clone()).await;

        let mut stream = TcpStream::connect(reactor.clone(), format!("{address}:{port}"))
            .await
            .expect("cannot connect");
        let (read, mut write) = stream.split();
        {
            let mut read =
                std::pin::pin!(FramedRead::new(read, ChunksDecoder::<4>::new()).into_stream());

            write
                .write_all([0x49, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x65].as_slice())
                .await
                .unwrap();
            write
                .write_all([0x49, 0x00, 0x00, 0x30, 0x3a, 0x00, 0x00, 0x00, 0x66].as_slice())
                .await
                .unwrap();
            write
                .write_all([0x49, 0x00, 0x00, 0x30, 0x3b, 0x00, 0x00, 0x00, 0x64].as_slice())
                .await
                .unwrap();
            write
                .write_all([0x49, 0x00, 0x00, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x05].as_slice())
                .await
                .unwrap();
            write
                .write_all([0x51, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x40, 0x00].as_slice())
                .await
                .unwrap();
            write.flush().await.unwrap();

            assert_eq!(
                [0x00, 0x00, 0x00, 0x65],
                read.next().await.unwrap().unwrap()
            );
        }

        stream.close().await.unwrap();
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

            p02_means_to_an_end::run(remote_address, stream).await.ok();
        }
    });

    info!("spawned app {address}:{port}");

    (address.to_string(), port)
}
