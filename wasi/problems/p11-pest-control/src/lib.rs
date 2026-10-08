#![doc = include_str!("../README.md")]

use std::pin;

use futures::channel::mpsc;

use wasi::io::streams::StreamError;
use wasi::sockets::network::ErrorCode;

use wasi_async_runtime::{SinkWrapper, StreamWrapper, Reactor};

use tracing::{info, instrument};

pub mod actors;
pub mod codec;

use actors::Provider;
use actors::controller::Controller;
use actors::site_visitor::SiteVisitor;
use codec::packets::{Packet, PacketCodec};

use wasi_async::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use wasi_async::net::{TcpListener, TcpStream};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("stream error: {0}")]
    Stream(#[from] StreamError),

    #[error("tcp socket error: {0}")]
    TcpSocket(#[from] ErrorCode),
}

#[derive(Clone, Debug)]
pub struct DefaultProvider {
    address: String,
    port: u16,
}

type PacketEncoderError = <PacketCodec as Encoder<Packet>>::Error;
type PacketDecoderError = <PacketCodec as Decoder>::Error;

impl DefaultProvider {
    #[must_use]
    pub fn new(address: String, port: u16) -> Self {
        Self { address, port }
    }
}

impl Provider for DefaultProvider {
    type Sink = SinkWrapper<Packet, PacketEncoderError>;
    type Stream = StreamWrapper<Result<Packet, PacketDecoderError>>;

    type Error = Error;

    #[instrument]
    async fn connect(&mut self, _: u32) -> Result<(Self::Sink, Self::Stream), Self::Error> {
        let socket = TcpStream::connect(format!("{}:{}", self.address, self.port)).await?;

        info!("new connection to authority server");

        let (read, write) = socket.into_split();
        let reader = FramedRead::new(read, PacketCodec::new()).into_stream();
        let writer = FramedWrite::new(write, PacketCodec::new()).into_sink();

        Ok((
            SinkWrapper(Box::pin(writer)),
            StreamWrapper(Box::pin(reader)),
        ))
    }
}

/// # Errors
#[instrument(skip_all)]
pub async fn run<P: Provider + Clone + 'static>(
    listener: TcpListener,
    authority_server_provider: P,
) -> Result<(), Error> {
    let (site_visits, site_visits_rx) = mpsc::channel(1000);

    let controller = Controller::new(authority_server_provider, site_visits_rx);
    Reactor::spawn_in_current(controller.run()).await;

    loop {
        let (socket, remote_addr) = listener.accept().await?;

        let site_visits = site_visits.clone();
        Reactor::spawn_in_current(async move {
            info!("remote: {remote_addr:?}");

            let (read, write) = socket.into_split();
            let reader = pin::pin!(FramedRead::new(read, PacketCodec::new()).into_stream());
            let writer = pin::pin!(FramedWrite::new(write, PacketCodec::new()).into_sink());

            let site_visitor = SiteVisitor::new(reader, writer, site_visits.clone());

            site_visitor.run().await;
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Once;
    use std::time::Duration;

    pub(crate) const TIMEOUT: Duration = Duration::from_millis(100);

    pub(crate) fn init_tracing_subscriber() {
        static INIT_TRACING_SUBSCRIBER: Once = Once::new();
        INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);
    }
}
