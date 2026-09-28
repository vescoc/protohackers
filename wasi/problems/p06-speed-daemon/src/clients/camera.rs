use std::pin::pin;
use std::time::Duration;

use futures::channel::mpsc::UnboundedSender;
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt};

use futures_concurrency::future::Race;

use tracing::{debug, info, instrument, warn};

use crate::clients::{Handler, HandlerResult, heartbeat};
use crate::{Cameras, ControllerMessage, Error, controller, wire};

#[derive(Error, Debug)]
#[allow(clippy::module_name_repetitions)]
pub enum CameraError {
    #[error("invalid camera")]
    InvalidCamera,
}

#[derive(Debug)]
struct CameraGuard(Cameras, u16);

impl CameraGuard {
    fn new(cameras: Cameras, road: u16, limit: u16) -> Result<Self, CameraError> {
        {
            let mut cameras = cameras.borrow_mut();
            let (l, c) = cameras.entry(road).or_insert((limit, 0));
            if *l != limit {
                return Err(CameraError::InvalidCamera);
            }
            *c += 1;
        }

        debug!("added camera road {road}");

        Ok(Self(cameras, road))
    }
}

impl Drop for CameraGuard {
    fn drop(&mut self) {
        let mut cameras = self.0.borrow_mut();
        if let Some((_, c)) = cameras.get_mut(&self.1) {
            *c -= 1;
            if *c == 0 {
                cameras.remove(&self.1);
                debug!("removed camera road {}", self.1);
            }
        }
    }
}

#[instrument(skip(controller_sender, cameras, heartbeat, read, write))]
#[allow(clippy::too_many_arguments, clippy::similar_names)]
pub(crate) async fn handle<R, W, E>(
    controller_sender: UnboundedSender<ControllerMessage>,
    cameras: Cameras,
    heartbeat: &mut heartbeat::Heartbeat,
    read: &mut R,
    write: &mut W,
    road: u16,
    mile: u16,
    limit: u16,
) -> Result<(), Error>
where
    R: Stream<Item = Result<wire::Packet, E>> + Unpin,
    W: Sink<wire::Packet, Error = E> + Unpin,
    Error: From<E>,
    E: std::error::Error,
{
    debug!("start");

    let _guard = CameraGuard::new(cameras, road, limit)?;

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
                    break Ok(());
                }
                HandlerResult::Read(Some(Err(err))) => {
                    warn!("got error {err}");
                    break Err(err.into());
                }
                HandlerResult::Read(Some(Ok(wire::Packet::Plate { plate, timestamp }))) => {
                    debug!("found plate");
                    controller_sender.unbounded_send(ControllerMessage::Plate(
                        controller::Plate {
                            road,
                            mile,
                            limit,
                            plate,
                            timestamp,
                        },
                    ))?;
                    None
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
