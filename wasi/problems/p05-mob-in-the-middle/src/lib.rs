#![doc = include_str!("../README.md")]

use std::borrow::Cow;
use std::rc::Rc;

use futures::StreamExt;

use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument;

use thiserror::Error;

use wasi::io::streams::StreamError;
use wasi::sockets::network;

use wasi_async::codec::{FramedRead, LinesDecoder};
use wasi_async::io::{AsyncRead, AsyncWriteExt};
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
        reactor
            .spawn(async move {
                handle(c_reactor, stream, chat_address, chat_port, boguscoin)
                    .await
                    .ok();
            })
            .instrument(info_span!("handle"));
    }
}

#[instrument(skip_all)]
async fn handle(
    reactor: Reactor,
    stream: TcpStream,
    chat_address: Rc<String>,
    chat_port: u16,
    boguscoin: Rc<String>,
) -> Result<(), Error> {
    debug!("start handle");

    let chat_stream =
        TcpStream::connect(reactor.clone(), format!("{chat_address}:{chat_port}")).await?;

    debug!("start workers");
    let (client_read, client_write) = stream.into_split();

    let (chat_read, chat_write) = chat_stream.into_split();

    let upstream_boguscoin = boguscoin.clone();
    let upstream_task = reactor
        .spawn(async move {
            handle_upstream(client_read, chat_write, upstream_boguscoin)
                .await
                .ok();
        })
        .instrument(info_span!("upstream_task"));
    let downstream_task = reactor
        .spawn(async move {
            handle_downstream(client_write, chat_read, boguscoin)
                .await
                .ok();
        })
        .instrument(info_span!("downstream_task"));

    debug!("wait workers");
    futures::join!(upstream_task, downstream_task);

    debug!("done handle");

    Ok(())
}

fn transform(message: &[u8], boguscoin: &str) -> String {
    String::from_utf8_lossy(message)
        .split_ascii_whitespace()
        .map(|word| {
            if word.starts_with('7')
                && (26..=35).contains(&word.len())
                && word.chars().all(char::is_alphanumeric)
            {
                Cow::Borrowed(boguscoin)
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
}

#[instrument(skip_all)]
async fn handle_upstream(
    client_read: impl AsyncRead + Unpin,
    mut chat_write: impl AsyncWriteExt,
    boguscoin: Rc<String>,
) -> Result<(), Error> {
    debug!("start handle_upstream");

    let mut client_read =
        std::pin::pin!(FramedRead::new(client_read, LinesDecoder::new()).into_stream());
    while let Some(message) = client_read.next().await {
        let message = transform(&message?, boguscoin.as_str());

        debug!("--> {message}");

        chat_write.write_all(message.as_bytes()).await?;
        chat_write.write(b"\n").await?;
        chat_write.flush().await?;

        debug!("-[done]-> {message}");
    }

    debug!("--> <CLOSING>");
    chat_write.close().await?;

    Ok(())
}

#[instrument(skip_all)]
async fn handle_downstream(
    mut client_write: impl AsyncWriteExt,
    chat_read: impl AsyncRead + Unpin,
    boguscoin: Rc<String>,
) -> Result<(), Error> {
    debug!("start handle_downstream");

    let mut chat_read =
        std::pin::pin!(FramedRead::new(chat_read, LinesDecoder::new()).into_stream());
    while let Some(message) = chat_read.next().await {
        let message = transform(&message?, boguscoin.as_str());

        debug!("<-- {message}");

        client_write.write_all(message.as_bytes()).await?;
        client_write.write(b"\n").await?;
        client_write.flush().await?;

        debug!("<-[done]- {message}");
    }

    debug!("<-- <CLOSING>");
    client_write.close().await?;

    Ok(())
}
