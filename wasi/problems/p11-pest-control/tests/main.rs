use std::sync::Once;
use std::time::Duration;

use futures::{FutureExt, SinkExt, StreamExt, channel::mpsc, future::FusedFuture};

use tracing::{debug, info, instrument, warn};

use wasi_async::codec::{FramedRead, FramedWrite};
use wasi_async::net::{TcpListener, TcpStream};
use wasi_async::time::timeout;

use wasi_async_runtime::Reactor;

use p11_pest_control::{DefaultProvider, codec::packets, run};

const TIMEOUT: Duration = Duration::from_millis(100);

fn init_tracing_subscriber() {
    static INIT_TRACING_SUBSCRIBER: Once = Once::new();
    INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);
}

#[test]
fn test_session() {
    init_tracing_subscriber();

    Reactor::block_on(|_| async {
        let (authority_server_address, authority_server_port, mut endpoints) =
            spawn_authority_server_app().await;

        let (address, port) = spawn_app(authority_server_address, authority_server_port).await;

        let stream = TcpStream::connect(format!("{address}:{port}"))
            .await
            .unwrap();
        let (read, write) = stream.into_split();
        let mut reader = Box::pin(FramedRead::new(read, packets::PacketCodec::new()).into_stream());
        let mut writer = Box::pin(FramedWrite::new(write, packets::PacketCodec::new()).into_sink());

        writer
            .send(packets::hello::Packet::new().into())
            .await
            .unwrap();
        assert_eq!(
            timeout(TIMEOUT, reader.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            packets::hello::Packet::new().into()
        );

        writer
            .send(
                packets::site_visit::Packet::new(
                    12345,
                    vec![packets::site_visit::Population::new("long-tailed rat", 20)],
                )
                .into(),
            )
            .await
            .unwrap();

        let (mut upstream, mut downstream) =
            timeout(TIMEOUT, endpoints.recv()).await.unwrap().unwrap();
        assert_eq!(
            timeout(TIMEOUT, downstream.recv()).await.unwrap().unwrap(),
            packets::hello::Packet::new().into()
        );

        upstream
            .send(packets::hello::Packet::new().into())
            .await
            .unwrap();

        assert_eq!(
            timeout(TIMEOUT, downstream.recv()).await.unwrap().unwrap(),
            packets::dial_authority::Packet::new(12345).into()
        );
        upstream
            .send(
                packets::target_populations::Packet::new(
                    12345,
                    vec![packets::target_populations::Population::new(
                        "long-tailed rat",
                        0,
                        10,
                    )],
                )
                .into(),
            )
            .await
            .unwrap();

        assert_eq!(
            timeout(TIMEOUT, downstream.recv()).await.unwrap().unwrap(),
            packets::create_policy::Packet::new(
                "long-tailed rat",
                packets::create_policy::PolicyAction::Cull
            )
            .into()
        );

        upstream
            .send(packets::policy_result::Packet::new(123).into())
            .await
            .unwrap();

        writer
            .send(
                packets::site_visit::Packet::new(
                    12345,
                    vec![packets::site_visit::Population::new("long-tailed rat", 10)],
                )
                .into(),
            )
            .await
            .unwrap();

        assert_eq!(
            timeout(TIMEOUT, downstream.recv()).await.unwrap().unwrap(),
            packets::delete_policy::Packet::new(123).into()
        );

        timeout(TIMEOUT, downstream.recv()).await.unwrap_err();
    });
}

async fn spawn_app(authority_server_address: String, authority_server_port: u16) -> (String, u16) {
    let address = "127.0.0.1";

    let authority_server_provider =
        DefaultProvider::new(authority_server_address, authority_server_port);

    let listener = TcpListener::bind(format!("{address}:0"))
        .await
        .expect("cannot bind app");
    let port = listener
        .local_addr()
        .expect("cannot get local address")
        .port();

    Reactor::spawn_in_current(async move {
        run(listener, authority_server_provider)
            .await
            .expect("run failed");
    })
    .await;

    info!("spawned app {address}:{port}");

    (address.to_string(), port)
}

#[instrument]
#[allow(clippy::type_complexity)]
async fn spawn_authority_server_app() -> (
    String,
    u16,
    mpsc::UnboundedReceiver<(
        mpsc::UnboundedSender<packets::Packet>,
        mpsc::UnboundedReceiver<packets::Packet>,
    )>,
) {
    let (mut endpoints_tx, endpoints) = mpsc::unbounded();

    let address = "127.0.0.1";

    let listener = TcpListener::bind(format!("{address}:0"))
        .await
        .expect("cannot bind authority server");
    let port = listener
        .local_addr()
        .expect("cannot get local address for authority server")
        .port();

    Reactor::spawn_in_current(async move {
        loop {
            let (socket, _) = listener.accept().await.expect("cannot accept");

            let (read, write) = socket.into_split();
            let mut reader =
                Box::pin(FramedRead::new(read, packets::PacketCodec::new()).into_stream());
            let mut writer =
                Box::pin(FramedWrite::new(write, packets::PacketCodec::new()).into_sink());

            let (upstream, mut upstream_rx) = mpsc::unbounded();
            let (mut downstream_tx, downstream) = mpsc::unbounded();

            endpoints_tx.send((upstream, downstream)).await.unwrap();

            Reactor::spawn_in_current(async move {
                loop {
                    let mut downstream = reader.next().fuse();
                    let mut upstream = upstream_rx.recv().fuse();
                    futures::select! {
                        packet = downstream => {
                            if let Some(Ok(packet)) = packet {
                                debug!("reader packet: {packet:?}");
                                downstream_tx.send(packet).await.unwrap();
                                debug!("reader packet, sent");
                            } else {
                                warn!("downstream error");
                            }
                        }

                        packet = upstream => {
                            if let Ok(packet) = packet {
                                debug!("writer packet: {packet:?}");
                                writer.send(packet).await.expect("cannot send packet upstream");
                                debug!("writer packet, sent");
                            } else {
                                warn!("upstream error");
                            }
                        }
                    }

                    if downstream.is_terminated() {
                        downstream = reader.next().fuse();
                    }
                    if upstream.is_terminated() {
                        upstream = upstream_rx.recv().fuse();
                    }
                }
            })
            .await;
        }
    })
    .await;

    info!("spawned authority server app {address}:{port}");

    (address.to_string(), port, endpoints)
}
