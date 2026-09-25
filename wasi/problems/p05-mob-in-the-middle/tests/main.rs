use std::rc::Rc;
use std::sync::Once;

use futures::StreamExt;

use tracing::{info, info_span};
use tracing_futures::Instrument;

use wasi_async::codec::{FramedRead, LinesDecoder};
use wasi_async::io::AsyncWriteExt;
use wasi_async::net::{TcpListener, TcpStream};
use wasi_async_runtime::{block_on, Reactor};

use p05_mob_in_the_middle::{run, BOGUSCOIN};

#[test]
fn test_session() {
    init_logging();

    block_on(|reactor| async move {
        let (chat_address, chat_port) = spawn_budget_chat_app(reactor.clone()).await;
        let (address, port) = spawn_app(reactor.clone(), chat_address.clone(), chat_port).await;

        let mut stream_alice =
            TcpStream::connect(reactor.clone(), format!("{chat_address}:{port}"))
                .await
                .unwrap();
        let (read_alice, mut write_alice) = stream_alice.split();
        let mut read_alice = std::pin::pin!(FramedRead::new(read_alice, LinesDecoder::new()).into_stream());
        
        write_alice.write_all(b"alice\n").await.unwrap();
        info!("write alice");        

        let result = read_alice.next().await.unwrap().unwrap();
        info!("read alice");        
        assert_eq!(
            std::str::from_utf8(&result).unwrap(),
            "Welcome to budgetchat! What shall I call you?"
        );

        let mut stream_bob = TcpStream::connect(reactor.clone(), format!("{address}:{port}"))
            .await
            .unwrap();
        let (read_bob, mut write_bob) = stream_bob.split();
        let mut read_bob = std::pin::pin!(FramedRead::new(read_bob, LinesDecoder::new()).into_stream());

        let result = read_bob.next().await.unwrap().unwrap();
        info!("read bob");        
        assert_eq!(
            std::str::from_utf8(&result).unwrap(),
            "Welcome to budgetchat! What shall I call you?"
        );

        write_bob.write_all(b"bob\n").await.unwrap();
        info!("write bob");        

        let result = read_bob.next().await.unwrap().unwrap();
        assert_eq!(result, b"* The room contains: alice");

        write_bob
            .write_all(b"Hi alice, please send payment to 7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX\n")
            .await
            .unwrap();

        let _step_0 = read_alice.next().await.unwrap(); // Welcome...
        let _step_1 = read_alice.next().await.unwrap(); // * The room contains...
        let _step_2 = read_alice.next().await.unwrap(); // * join

        let result = read_alice.next().await.unwrap().unwrap();
        assert_eq!(
            std::str::from_utf8(&result).unwrap(),
            "[bob] Hi alice, please send payment to 7YWHMfk9JZe0LM0g1ZauHuiSxhI"
        );
    }.instrument(info_span!("test_session")));
}

fn init_logging() {
    static INIT_TRACING_SUBSCRIBER: Once = Once::new();
    INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);
}

async fn spawn_budget_chat_app(reactor: Reactor) -> (String, u16) {
    let address = "127.0.0.1";

    let listener = TcpListener::bind(reactor.clone(), format!("{address}:0"))
        .await
        .expect("cannot bind budget_chat app");
    let port = listener
        .local_addr()
        .expect("cannot get local address")
        .port();

    reactor.clone().spawn(async move {
        p03_budget_chat::run(reactor, listener)
            .await
            .expect("run failed");
    }.instrument(info_span!("budget_chat_app")));

    info!("spawned budget chat app {address}:{port}");

    (address.to_string(), port)
}

async fn spawn_app(reactor: Reactor, chat_address: String, chat_port: u16) -> (String, u16) {
    let address = "127.0.0.1";

    let listener = TcpListener::bind(reactor.clone(), format!("{address}:0"))
        .await
        .expect("cannot bind main app");
    let port = listener
        .local_addr()
        .expect("cannot get local address")
        .port();

    reactor.clone().spawn(async move {
        run(
            reactor,
            listener,
            Rc::new(chat_address),
            chat_port,
            Rc::new(BOGUSCOIN.to_string()),
        )
        .await
        .expect("run failed");
    }.instrument(info_span!("app")));

    info!("spawned budget chat app {address}:{port}");

    (address.to_string(), port)
}
