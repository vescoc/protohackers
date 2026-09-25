use std::sync::Once;
use std::time::Duration;

use futures::{SinkExt, StreamExt};

use tracing::{debug, info};

use wasi_async::codec::{FramedRead, FramedWrite};
use wasi_async::net::{TcpListener, TcpStream};
use wasi_async::time::timeout;

use wasi_async_runtime::{block_on, Reactor};

use p06_speed_daemon::{run, wire};

#[test]
#[allow(clippy::too_many_lines)]
fn test_session() {
    block_on(|reactor| async move {
        let (address, port) = spawn_app(reactor.clone()).await;

        {
            debug!("send IAmCamera 1");
            let mut stream = TcpStream::connect(reactor.clone(), format!("{address}:{port}"))
                .await
                .unwrap();
            let (_, write) = stream.split();
            let mut write = std::pin::pin!(FramedWrite::new(write, wire::PacketCodec).into_sink());

            write
                .send(wire::Packet::IAmCamera {
                    road: 123,
                    mile: 8,
                    limit: 60,
                })
                .await
                .unwrap();

            write
                .send(wire::Packet::Plate {
                    plate: "UN1X".to_string(),
                    timestamp: 0,
                })
                .await
                .unwrap();

            write.flush().await.unwrap();
        }

        {
            debug!("send IAmCamera 2");
            let mut stream = TcpStream::connect(reactor.clone(), format!("{address}:{port}"))
                .await
                .unwrap();
            let (_, write) = stream.split();
            let mut write = std::pin::pin!(FramedWrite::new(write, wire::PacketCodec).into_sink());

            write
                .send(wire::Packet::IAmCamera {
                    road: 123,
                    mile: 9,
                    limit: 60,
                })
                .await
                .unwrap();

            write
                .send(wire::Packet::Plate {
                    plate: "UN1X".to_string(),
                    timestamp: 45,
                })
                .await
                .unwrap();

            write.flush().await.unwrap();
        }

        {
            debug!("send IAmDispatcher");
            let mut stream = TcpStream::connect(reactor.clone(), format!("{address}:{port}"))
                .await
                .unwrap();
            let (read, write) = stream.split();
            let mut read = std::pin::pin!(FramedRead::new(read, wire::PacketCodec).into_stream());
            let mut write = std::pin::pin!(FramedWrite::new(write, wire::PacketCodec).into_sink());

            write
                .send(wire::Packet::IAmDispatcher { roads: vec![123] })
                .await
                .unwrap();
            debug!("sent IAmDispatcher");

            let ticket = timeout(reactor, Duration::from_millis(1000), read.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();

            assert_eq!(
                ticket,
                wire::Packet::Ticket {
                    plate: "UN1X".to_string(),
                    road: 123,
                    mile1: 8,
                    timestamp1: 0,
                    mile2: 9,
                    timestamp2: 45,
                    speed: 8000,
                }
            );
        }
    });
}

// #[test]
// fn test_invalid_message() {
//     block_on(|reactor| async move {
//         let (address, port) = spawn_app().await;

//         let mut stream = TcpStream::connect(&format!("{address}:{port}"))
//             .await
//             .unwrap();
//         let (mut read, mut write) = stream.split();
//         wire::Plate {
//             plate: "UN1X".to_string(),
//             timestamp: 0,
//         }
//         .write_to(&mut write)
//             .await
//             .unwrap();

//         assert_eq!(
//             wire::Error {
//                 msg: "invalid message: 0x20".to_string()
//             },
//             wire::Error::read_from(&mut read)
//                 .await
//                 .unwrap()
//         );

//         if let Ok(r) = timeout(Duration::from_millis(100), read.read_u8()).await {
//             assert!(r.is_err(), "got message");
//         } else {
//             panic!("timeout");
//         }
//     });
// }

// #[test]
// fn test_heartbeat_camera() {
//     block_on(|reactor| async move {
//         let (address, port) = spawn_app().await;

//         let mut stream = TcpStream::connect(&format!("{address}:{port}"))
//             .await
//             .unwrap();
//         let (mut read, mut write) = stream.split();

//         wire::IAmCamera {
//             road: 123,
//             mile: 8,
//             limit: 60,
//         }
//         .write_to(&mut write)
//             .await
//             .unwrap();

//         wire::WantHeartbeat { interval: 1 }
//         .write_to(&mut write)
//             .await
//             .unwrap();

//         for i in 0..10 {
//             if let Ok(Ok(wire::Heartbeat)) = timeout(
//                 Duration::from_millis(1000),
//                 wire::Heartbeat::read_from(&mut read),
//             )
//                 .await
//             {
//                 // ok
//             } else {
//                 panic!("expecting heartbeat: {i}");
//             }
//         }
//     });
// }

// #[test]
// fn test_heartbeat_dispatcher() {
//     block_on(|reactor| async move {
//         let (address, port) = spawn_app().await;

//         let mut stream = TcpStream::connect(&format!("{address}:{port}"))
//             .await
//             .unwrap();
//         let (mut read, mut write) = stream.split();

//         wire::IAmDispatcher { roads: vec![123] }
//         .write_to(&mut write)
//             .await
//             .unwrap();

//         wire::WantHeartbeat { interval: 1 }
//         .write_to(&mut write)
//             .await
//             .unwrap();

//         for i in 0..10 {
//             if let Ok(Ok(wire::Heartbeat)) = timeout(
//                 Duration::from_millis(1000),
//                 wire::Heartbeat::read_from(&mut read),
//             )
//                 .await
//             {
//                 // ok
//             } else {
//                 panic!("expecting heartbeat: {i}");
//             }
//         }
//     });
// }

async fn spawn_app(reactor: Reactor) -> (String, u16) {
    static INIT_TRACING_SUBSCRIBER: Once = Once::new();
    INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);

    let address = "127.0.0.1";

    let listener = TcpListener::bind(reactor.clone(), format!("{address}:0"))
        .await
        .expect("cannot bind app");
    let port = listener
        .local_addr()
        .expect("cannot get local address")
        .port();

    reactor.clone().spawn(async move {
        run(reactor, listener).await.expect("run failed");
    });

    info!("spawned app {address}:{port}");

    (address.to_string(), port)
}
