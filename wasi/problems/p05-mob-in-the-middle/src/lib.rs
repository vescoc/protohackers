#![doc = include_str!("../README.md")]

use std::borrow::Cow;
use std::rc::Rc;

use futures::{FutureExt, StreamExt};

use tracing::{debug, info, instrument, trace};

use thiserror::Error;

use wasi::io::streams::StreamError;
use wasi::sockets::network;

use wasi_async::codec::{FramedRead, LinesDecoder};
use wasi_async::io::{AsyncWrite, AsyncWriteExt};
use wasi_async::net::{TcpListener, TcpStream};
use wasi_async_runtime::Reactor;

#[allow(warnings)]
mod bindings;

pub const BOGUSCOIN: &str = "7YWHMfk9JZe0LM0g1ZauHuiSxhI";

#[derive(Error, Debug)]
pub enum Error {
    #[error("tcp error {0}")]
    Tcp(#[from] network::ErrorCode),

    #[error("stream error {0}")]
    Stream(#[from] StreamError),
}

/// Run
///
/// # Errors
///
/// - [`Error::Tcp`] -- if there are problems with tcp
/// - [`Error::Stream`] -- if there are problems with the stream
#[instrument(skip(reactor, listener))]
pub async fn run(
    reactor: Reactor,
    listener: TcpListener,
    chat_address: Rc<String>,
    chat_port: u16,
    boguscoin: Rc<String>,
) -> Result<(), Error> {
    loop {
        let (stream, remote_address) = listener.accept().await?;

        info!("client: {remote_address:?}");

        let chat_address = chat_address.clone();
        let boguscoin = boguscoin.clone();
        let c_reactor = reactor.clone();
        reactor.spawn(async move {
            handle(c_reactor, stream, chat_address, chat_port, boguscoin)
                .await
                .ok();
        });
    }
}

// #[instrument(skip_all)]
// async fn handle(
//     reactor: Reactor,
//     mut stream: TcpStream,
//     chat_address: Rc<String>,
//     chat_port: u16,
//     boguscoin: Rc<String>,
// ) -> Result<(), Error> {
//     let transform = |message: &[u8]| -> String {
//         String::from_utf8_lossy(message)
//             .split_ascii_whitespace()
//             .map(|word| {
//                 if word.starts_with('7')
//                     && (26..=35).contains(&word.len())
//                     && word.chars().all(char::is_alphanumeric)
//                 {
//                     Cow::Borrowed(boguscoin.as_str())
//                 } else {
//                     Cow::Borrowed(word)
//                 }
//             })
//             .fold(String::new(), |mut s, word| {
//                 if s.is_empty() {
//                     word.into_owned()
//                 } else {
//                     s.push(' ');
//                     s.push_str(&word);
//                     s
//                 }
//             })
//     };

//     let (client_read, mut client_write) = stream.split();
//     let mut client_read = FramedRead::new(client_read, LinesDecoder::new());

//     let mut chat_stream =
//         TcpStream::connect(reactor.clone(), format!("{chat_address}:{chat_port}")).await?;
//     let (chat_read, mut chat_write) = chat_stream.split();
//     let mut chat_read = FramedRead::new(chat_read, LinesDecoder::new());

//     let mut done_upstream = false;
//     let mut done_downstream = false;

//     while !done_upstream || !done_downstream {
//         let mut actions: Vec<Pin<&mut dyn Future<Output = Action>>> = Vec::with_capacity(2);

//         let client = pin!(client_read.next().map(Action::Client));
//         if !done_upstream {
//             actions.push(client);
//         }

//         let chat = pin!(chat_read.next().map(Action::Chat));
//         if !done_downstream {
//             actions.push(chat);
//         }

//         match actions.race().await {
//             Action::Client(Some(message)) => {
//                 let message = transform(&message?);

//                 trace!("--> {message}");

//                 chat_write.write_all(message.as_bytes()).await?;
//                 chat_write.write(b"\n").await?;
//                 chat_write.flush().await?;
//             }
//             Action::Chat(Some(message)) => {
//                 let message = transform(&message?);

//                 trace!("<-- {message}");

//                 client_write.write_all(message.as_bytes()).await?;
//                 client_write.write(b"\n").await?;
//                 client_write.flush().await?;
//             }
//             Action::Client(None) => {
//                 debug!("--> <CLOSING>");
//                 done_upstream = true;
//                 chat_write.close().await?;
//             }
//             Action::Chat(None) => {
//                 debug!("<-- <CLOSING>");
//                 done_downstream = true;
//                 chat_write.close().await?;
//             }
//         }
//     }

//     Ok(())
// }

#[instrument(skip_all)]
async fn handle(
    reactor: Reactor,
    mut stream: TcpStream,
    chat_address: Rc<String>,
    chat_port: u16,
    boguscoin: Rc<String>,
) -> Result<(), Error> {
    let transform = |message: &[u8]| -> String {
        String::from_utf8_lossy(message)
            .split_ascii_whitespace()
            .map(|word| {
                if word.starts_with('7')
                    && (26..=35).contains(&word.len())
                    && word.chars().all(char::is_alphanumeric)
                {
                    Cow::Borrowed(boguscoin.as_str())
                } else {
                    Cow::Borrowed(word)
                }
            })
            .fold(String::new(), |mut s, word| {
                if s.is_empty() {
                    word.into_owned()
                } else {
                    s.push(' ');
                    s.push_str(&word);
                    s
                }
            })
    };

    let (client_read, mut client_write) = stream.split();
    let mut client_read = std::pin::pin!(FramedRead::new(client_read, LinesDecoder::new()).into_stream());

    let mut chat_stream =
        TcpStream::connect(reactor.clone(), format!("{chat_address}:{chat_port}")).await?;
    let (chat_read, mut chat_write) = chat_stream.split();
    let mut chat_read = std::pin::pin!(FramedRead::new(chat_read, LinesDecoder::new()).into_stream());

    let mut done_upstream = false;
    let mut done_downstream = false;

    let mut client = client_read.next().fuse();
    let mut chat = chat_read.next().fuse();
    while !done_upstream || !done_downstream {
        match futures::future::select(client, chat).await {
            futures::future::Either::Left((message, current_chat)) => {
                if let Some(message) = message {
                    let message = transform(&message?);
                    
                    trace!("--> {message}");

                    chat_write.write_all(message.as_bytes()).await?;
                    chat_write.write(b"\n").await?;
                    chat_write.flush().await?;
                } else {
                    debug!("--> <CLOSING>");
                    done_upstream = true;
                    chat_write.close().await?;
                }

                client = client_read.next().fuse();
                chat = current_chat;
            }
            futures::future::Either::Right((message, current_client)) => {
                if let Some(message) = message {
                    let message = transform(&message?);

                    trace!("<-- {message}");

                    client_write.write_all(message.as_bytes()).await?;
                    client_write.write(b"\n").await?;
                    client_write.flush().await?;
                } else {
                    debug!("<-- <CLOSING>");
                    done_downstream = true;
                    chat_write.close().await?;
                }

                client = current_client;
                chat = chat_read.next().fuse();
            }
        }
    }

    Ok(())
}
