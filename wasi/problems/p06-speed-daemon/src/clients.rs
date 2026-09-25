use std::future::Future;
use std::pin::{pin, Pin};
use std::time::Duration;

use futures::{channel::mpsc, FutureExt, Sink, SinkExt, Stream, StreamExt};

use futures_concurrency::future::Race;

use tracing::{debug, info, instrument, warn};

use wasi::sockets::network::IpSocketAddress;

use wasi_async::codec::{FramedRead, FramedWrite};
use wasi_async::net::TcpStream;
use wasi_async_runtime::Reactor;

use crate::{wire, Cameras, ControllerMessage, Error};

mod camera;
mod dispatcher;
mod heartbeat;

pub use camera::CameraError;

#[derive(Debug)]
enum ClientType {
    Camera { road: u16, mile: u16, limit: u16 },
    Dispatcher { roads: Vec<u16> },
    Unknown,
}

#[instrument(skip(reactor, stream, controller_sender, cameras))]
pub(crate) async fn handle(
    remote_address: IpSocketAddress,
    reactor: Reactor,
    mut stream: TcpStream,
    controller_sender: mpsc::UnboundedSender<ControllerMessage>,
    cameras: Cameras,
) -> Result<(), Error> {
    let mut heartbeat = heartbeat::Heartbeat::new(reactor);

    let (read, write) = stream.split();
    let mut read = std::pin::pin!(FramedRead::new(read, wire::PacketCodec).into_stream());
    let mut write = std::pin::pin!(FramedWrite::new(write, wire::PacketCodec).into_sink());

    let r: Result<(), Error> = async {
        match identify(&mut heartbeat, &mut read, &mut write).await? {
            ClientType::Camera { road, mile, limit } => Ok(camera::handle(
                controller_sender,
                cameras,
                &mut heartbeat,
                &mut read,
                &mut write,
                road,
                mile,
                limit,
            )
            .await?),
            ClientType::Dispatcher { roads } => Ok(dispatcher::handle(
                controller_sender,
                &mut heartbeat,
                &mut read,
                &mut write,
                &roads,
            )
            .await?),
            ClientType::Unknown => {
                warn!("unknown client, done");
                Ok(())
            }
        }
    }
    .await;

    if let Err(err) = r {
        write
            .send(wire::Packet::Error {
                msg: err.to_string(),
            })
            .await
            .ok();
    }
    write.close().await.ok();
    Ok(())
}

type Handler<'a, E> = Pin<&'a mut dyn Future<Output = HandlerResult<E>>>;

enum HandlerResult<E> {
    Read(Option<Result<wire::Packet, E>>),
    Tick(()),
}

#[instrument(skip_all)]
async fn identify<R, W, E>(
    heartbeat: &mut heartbeat::Heartbeat,
    read: &mut R,
    write: &mut W,
) -> Result<ClientType, E>
where
    R: Stream<Item = Result<wire::Packet, E>> + Unpin,
    W: Sink<wire::Packet, Error = E> + Unpin,
    Error: From<E>,
    E: From<wire::Error> + std::error::Error,
{
    debug!("start");

    loop {
        let interval = {
            let setted = heartbeat.is_setted();

            let read_message_future: Handler<E> = pin!(read.next().map(HandlerResult::Read));
            let heartbeat_future: Handler<E> = pin!(heartbeat.tick().map(HandlerResult::Tick));

            let mut fs = vec![read_message_future];
            if setted {
                fs.push(heartbeat_future);
            }

            match fs.race().await {
                HandlerResult::Tick(()) => {
                    debug!("sending heartbeat");
                    write.send(wire::Packet::Heartbeat).await?;
                    None
                }
                HandlerResult::Read(None) => {
                    info!("connection close");
                    break Ok(ClientType::Unknown);
                }
                HandlerResult::Read(Some(Err(err))) => {
                    warn!("got error {err}");
                    break Err(err);
                }
                HandlerResult::Read(Some(Ok(wire::Packet::IAmCamera { road, mile, limit }))) => {
                    debug!("found camera");
                    break Ok(ClientType::Camera { road, mile, limit });
                }
                HandlerResult::Read(Some(Ok(wire::Packet::IAmDispatcher { roads }))) => {
                    debug!("found dispatcher");
                    break Ok(ClientType::Dispatcher { roads });
                }
                HandlerResult::Read(Some(Ok(wire::Packet::WantHeartbeat { interval }))) => {
                    debug!("found heartbeat request");
                    Some(interval)
                }
                HandlerResult::Read(Some(Ok(packet))) => {
                    warn!("invalid packet: {packet:?}");
                    break Err(wire::Error::InvalidMessage(packet.tag()).into());
                }
            }
        };

        if let Some(interval) = interval {
            if heartbeat.is_setted() {
                warn!("heartbeat already setted");
                break Err(wire::Error::InvalidMessage(wire::WANT_HEARTBEAT_TAG).into());
            }
            heartbeat.set_period(Duration::from_millis(u64::from(interval * 100)));
        }
    }
}
