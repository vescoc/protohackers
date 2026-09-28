use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic;
use std::time::Duration;

use futures::channel::mpsc;
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt};

use futures_concurrency::future::Race;

use tracing::{debug, info, instrument, warn};

use crate::clients::heartbeat;
use crate::{ControllerMessage, Error, controller, wire};

#[derive(Debug)]
struct DispatcherGuard(mpsc::UnboundedSender<ControllerMessage>, usize);

impl DispatcherGuard {
    fn new(
        controller_sender: mpsc::UnboundedSender<ControllerMessage>,
        roads: &[u16],
        ticket_sender: mpsc::UnboundedSender<controller::Ticket>,
    ) -> Result<Self, anyhow::Error> {
        static IDS: atomic::AtomicUsize = atomic::AtomicUsize::new(0);

        let id = IDS.fetch_add(1, atomic::Ordering::Relaxed);

        controller_sender.unbounded_send(ControllerMessage::AddDispatcher(
            id,
            roads.iter().copied().collect(),
            ticket_sender,
        ))?;

        debug!("added dispatcher {id}");

        Ok(Self(controller_sender, id))
    }
}

impl Drop for DispatcherGuard {
    fn drop(&mut self) {
        if let Err(err) = self
            .0
            .unbounded_send(ControllerMessage::RemoveDispatcher(self.1))
        {
            warn!("cannot remove dispatcher: {err:?}");
        }
    }
}

type Handler<'a, E> = Pin<&'a mut dyn Future<Output = HandlerResult<E>>>;

enum HandlerResult<E> {
    Read(Option<Result<wire::Packet, E>>),
    Receive(Option<controller::Ticket>),
    Tick(()),
}

#[instrument(skip(controller_sender, heartbeat, read, write))]
pub(crate) async fn handle<R, W, E>(
    controller_sender: mpsc::UnboundedSender<ControllerMessage>,
    heartbeat: &mut heartbeat::Heartbeat,
    read: &mut R,
    write: &mut W,
    roads: &[u16],
) -> Result<(), Error>
where
    R: Stream<Item = Result<wire::Packet, E>> + Unpin,
    W: Sink<wire::Packet, Error = E> + Unpin,
    Error: From<E>,
    E: std::error::Error,
{
    debug!("start");

    let (ticket_sender, mut ticket_receiver) = mpsc::unbounded();

    let guard = DispatcherGuard::new(controller_sender, roads, ticket_sender);

    let r = loop {
        let interval = {
            let setted = heartbeat.is_setted();

            let read_message_future: Handler<E> = pin!(read.next().map(HandlerResult::Read));
            let ticket_receiver_future: Handler<E> =
                pin!(ticket_receiver.next().map(HandlerResult::Receive));
            let heartbeat_future: Handler<E> = pin!(heartbeat.tick().map(HandlerResult::Tick));

            let mut fs = vec![read_message_future, ticket_receiver_future];
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
                HandlerResult::Read(Some(Ok(wire::Packet::WantHeartbeat { interval }))) => {
                    debug!("found heartbeat request");
                    Some(interval)
                }
                HandlerResult::Read(Some(Ok(packet))) => {
                    warn!("invalid packet: {packet:?}");
                    break Err(wire::Error::InvalidMessage(packet.tag()).into());
                }
                HandlerResult::Receive(None) => {
                    warn!("controller connection close");
                    break Ok(());
                }
                HandlerResult::Receive(Some(controller::Ticket {
                    plate,
                    road,
                    mile1,
                    timestamp1,
                    mile2,
                    timestamp2,
                    speed,
                })) => {
                    debug!("got ticket");
                    write
                        .send(wire::Packet::Ticket {
                            plate,
                            road,
                            mile1,
                            timestamp1,
                            mile2,
                            timestamp2,
                            speed,
                        })
                        .await?;
                    None
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
    };

    drop(guard);

    r
}
