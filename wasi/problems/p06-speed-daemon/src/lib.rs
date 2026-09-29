#![doc = include_str!("../README.md")]

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use futures::{StreamExt, channel::mpsc};

use thiserror::Error;

use tracing::{debug, info, instrument, warn};

use wasi::sockets::network;

use wasi_async::net::TcpListener;
use wasi_async_runtime::Reactor;

mod clients;
mod controller;
mod dispatchers;
pub mod wire;

pub use clients::CameraError;

#[allow(warnings)]
mod bindings;

type Cameras = Rc<RefCell<HashMap<u16, (u16, usize)>>>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("tcp socket error {0}")]
    TcpSocket(#[from] network::ErrorCode),

    #[error("dispatchers error {0}")]
    Dispatchers(#[from] dispatchers::Error),

    #[error("wire error {0}")]
    Wire(#[from] wire::Error),

    #[error("invalid camera")]
    Camera(#[from] CameraError),

    #[error("sending error {0}")]
    Send(#[from] mpsc::TrySendError<ControllerMessage>),
}

pub enum ControllerMessage {
    AddDispatcher(
        usize,
        HashSet<u16>,
        mpsc::UnboundedSender<controller::Ticket>,
    ),
    RemoveDispatcher(usize),
    Plate(controller::Plate),
}

/// Main loop
///
/// Listen for clients and handle dispatching tickets.
///
/// # Errors
///
/// * Error when socket returs an error.
///
/// # Panics
#[instrument(skip(listener))]
pub async fn run(listener: TcpListener) -> Result<(), Error> {
    let mut controller = controller::Controller::default();
    let mut dispatchers = dispatchers::Dispatchers::default();

    let cameras = Rc::new(RefCell::new(HashMap::new()));

    let (controller_sender, mut controller_receiver) = mpsc::unbounded();

    Reactor::spawn_in_current(async move {
        loop {
            match controller_receiver.next().await {
                Some(ControllerMessage::AddDispatcher(id, roads, ticket_sender)) => {
                    debug!("adding dispatcher {id} roads {roads:?}");
                    dispatchers
                        .add_dispatcher(id, roads, ticket_sender)
                        .unwrap();
                }
                Some(ControllerMessage::RemoveDispatcher(id)) => {
                    debug!("removing dispatcher {id}");
                    dispatchers.remove_dispatcher(id);
                }
                Some(ControllerMessage::Plate(plate)) => {
                    info!("handling plate: {plate:?}");
                    let tickets = controller.signal(plate);
                    debug!("tickets: {tickets:?}");
                    dispatchers.send_tickets(tickets).unwrap();
                }
                None => {
                    warn!("empty controller message");
                }
            }
        }
    })
    .await;

    loop {
        debug!("waiting client");
        match listener.accept().await {
            Ok((socket, remote_address)) => {
                info!("new client {remote_address:?}");

                let controller_sender = controller_sender.clone();
                let cameras = cameras.clone();
                Reactor::spawn_in_current(async move {
                    let Err(err) =
                        clients::handle(remote_address, socket, controller_sender, cameras).await
                    else {
                        debug!("done client {remote_address:?}");
                        return;
                    };
                    warn!("handle client error {remote_address:?}: {err:?}");
                })
                .await;
            }
            Err(err) => {
                warn!("listener error: {err:?}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    pub(crate) fn init_tracing_subscriber() {
        static INIT_TRACING_SUBSCRIBER: Once = Once::new();
        INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);
    }
}
