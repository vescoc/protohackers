use std::pin;
use std::rc::Rc;
use std::sync::Once;
use std::time::Duration;

use tracing_futures::Instrument;

use futures::{SinkExt, StreamExt};

use tracing::{debug, info, info_span};

use wasi_async_runtime::{Reactor, sync::Notify};

use wasi_async::{codec, net, time};

use p09_job_centre::{protocol, run};

const TIMEOUT: Duration = Duration::from_millis(100);

#[test]
fn test_session() {
    Reactor::block_on(|_| {
        async {
            let (address, port) = spawn_app().await;

            let mut stream = net::TcpStream::connect(format!("{address}:{port}"))
                .await
                .unwrap();
            let (read, write) = stream.split();
            let mut read = pin::pin!(
                codec::FramedRead::new(read, protocol::ResponseCodec::new()).into_stream()
            );
            let mut write = pin::pin!(
                codec::FramedWrite::new(write, protocol::RequestCodec::new()).into_sink()
            );

            let job = serde_json::json!({"title": "example-job"});

            write
                .send(protocol::Request::Put {
                    queue: "queue1".to_string(),
                    job: job.clone(),
                    priority: 123,
                })
                .await
                .unwrap();
            write.flush().await.unwrap();

            let Ok(protocol::Response::Id(id)) =
                time::timeout(TIMEOUT, read.next()).await.unwrap().unwrap()
            else {
                panic!("invalid response");
            };

            write
                .send(protocol::Request::Get {
                    queues: vec!["queue1".to_string()],
                    wait: false,
                })
                .await
                .unwrap();
            write.flush().await.unwrap();

            let Ok(protocol::Response::JobRef(job_ref_id, job_ref_job, priority, queue)) =
                time::timeout(TIMEOUT, read.next()).await.unwrap().unwrap()
            else {
                panic!("invalid response")
            };

            assert_eq!(id, job_ref_id);
            assert_eq!(job, job_ref_job);
            assert_eq!(123, priority);
            assert_eq!("queue1", queue);

            write.send(protocol::Request::Abort { id }).await.unwrap();
            write.flush().await.unwrap();

            let Ok(protocol::Response::Ok) =
                time::timeout(TIMEOUT, read.next()).await.unwrap().unwrap()
            else {
                panic!("invalid response")
            };

            write
                .send(protocol::Request::Get {
                    queues: vec!["queue1".to_string()],
                    wait: false,
                })
                .await
                .unwrap();
            write.flush().await.unwrap();

            let Ok(protocol::Response::JobRef(job_ref_id, job_ref_job, priority, queue)) =
                time::timeout(TIMEOUT, read.next()).await.unwrap().unwrap()
            else {
                panic!("invalid response")
            };

            assert_eq!(id, job_ref_id);
            assert_eq!(job, job_ref_job);
            assert_eq!(123, priority);
            assert_eq!("queue1", queue);

            write.send(protocol::Request::Delete { id }).await.unwrap();
            write.flush().await.unwrap();

            let Ok(protocol::Response::Ok) =
                time::timeout(TIMEOUT, read.next()).await.unwrap().unwrap()
            else {
                panic!("invalid response")
            };

            write
                .send(protocol::Request::Get {
                    queues: vec!["queue1".to_string()],
                    wait: false,
                })
                .await
                .unwrap();
            write.flush().await.unwrap();

            let Ok(protocol::Response::NoJob) =
                time::timeout(TIMEOUT, read.next()).await.unwrap().unwrap()
            else {
                panic!("invalid response")
            };
        }
        .instrument(info_span!("test_session"))
    });
}

#[test]
fn test_simple_wait() {
    Reactor::block_on(|_| {
        async {
            let (address, port) = spawn_app().await;

            let test_queue = "simple-wait-queue";

            let barrier = Rc::new(Notify::new());
            let worker = {
                let barrier = Rc::clone(&barrier);
                let address = address.clone();
                Reactor::spawn_in_current(
                    async move {
                        let mut stream = net::TcpStream::connect(format!("{address}:{port}"))
                            .await
                            .unwrap();
                        let (read, write) = stream.split();
                        let mut read = pin::pin!(
                            codec::FramedRead::new(read, protocol::ResponseCodec::new())
                                .into_stream()
                        );
                        let mut write = pin::pin!(
                            codec::FramedWrite::new(write, protocol::RequestCodec::new())
                                .into_sink()
                        );

                        barrier.notify_one();

                        debug!("worker about to get");

                        write
                            .send(protocol::Request::Get {
                                queues: vec![test_queue.to_string()],
                                wait: true,
                            })
                            .await
                            .unwrap();
                        write.flush().await.unwrap();

                        debug!("worker get done");

                        let Ok(protocol::Response::JobRef(_, _, _, queue)) =
                            time::timeout(TIMEOUT, read.next()).await.unwrap().unwrap()
                        else {
                            panic!("invalid response")
                        };

                        assert_eq!(queue, test_queue);
                    }
                    .instrument(info_span!("worker")),
                )
                .await
            };

            barrier.notified().await;

            let mut stream = net::TcpStream::connect(format!("{address}:{port}"))
                .await
                .unwrap();
            let (read, write) = stream.split();
            let mut read = pin::pin!(
                codec::FramedRead::new(read, protocol::ResponseCodec::new()).into_stream()
            );
            let mut write = pin::pin!(
                codec::FramedWrite::new(write, protocol::RequestCodec::new()).into_sink()
            );

            debug!("main about to put");

            let job = serde_json::json!({"title": "example-job"});
            write
                .send(protocol::Request::Put {
                    queue: test_queue.to_string(),
                    job: job.clone(),
                    priority: 123,
                })
                .await
                .unwrap();
            write.flush().await.unwrap();

            debug!("main put done");

            let Ok(protocol::Response::Id(_)) =
                time::timeout(TIMEOUT, read.next()).await.unwrap().unwrap()
            else {
                panic!("invalid response");
            };

            assert_eq!((), time::timeout(TIMEOUT, worker).await.unwrap());
        }
        .instrument(info_span!("test_simple_wait"))
    });
}

async fn spawn_app() -> (String, u16) {
    static TRACING_SUBSCRIBER_INIT: Once = Once::new();
    TRACING_SUBSCRIBER_INIT.call_once(tracing_subscriber::fmt::init);

    let address = "127.0.0.1";

    let listener = net::TcpListener::bind(format!("{address}:0"))
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
