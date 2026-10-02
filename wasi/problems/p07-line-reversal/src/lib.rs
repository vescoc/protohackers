#![doc = include_str!("../README.md")]

mod lrcp;
mod session_handler;

use std::collections::HashMap;

use futures::FutureExt;
use futures::channel::mpsc;

use tracing::{debug, info, instrument};

use wasi::sockets::network;

use wasi_async::net::UdpSocket;
use wasi_async_runtime::Reactor;

#[allow(warnings)]
mod bindings;

use lrcp::packets::{Packet, Session, SyncWrite};
use session_handler::SessionHandler;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("socket error {0}")]
    Socket(#[from] network::ErrorCode),
    #[error("upstream channel closed {0}")]
    UpstreamChannelClosed(&'static str),
    #[error("downstream channel closed {0}")]
    DownstreamChannelClosed(&'static str),
}

/// # Panics
/// # Errors
#[instrument(skip_all)]
pub async fn run(socket: UdpSocket) -> Result<(), Error> {
    let reactor = Reactor::current().await;

    let mut session_handlers = HashMap::with_capacity(1024);
    let (downstream_sender, mut downstream_receiver) = mpsc::unbounded();

    let mut receiver = downstream_receiver.recv().fuse();
    let mut packets = Box::pin(socket.recv_from().fuse());
    loop {
        debug!("main loop: sessions {}", session_handlers.len());
        match futures::future::select(receiver, packets).await {
            futures::future::Either::Left((packet, current_packets)) => {
                debug!("new downstream packet");

                let (packet, addr) =
                    packet.map_err(|_| Error::DownstreamChannelClosed("closed"))?;

                let mut data = Vec::new();
                let len = data.write_value(&packet).unwrap();
                assert!(len <= 1000);

                socket.send_to(data, addr).await?;

                receiver = downstream_receiver.recv().fuse();
                packets = current_packets;
            }
            futures::future::Either::Right((packet, current_receiver)) => {
                debug!("new upstream packet");

                let (packet, addr) = packet?;
                match Packet::try_from(packet.as_slice()) {
                    Ok(packet @ Packet::Connect { .. }) => {
                        session_handlers
                            .entry(packet.session())
                            .or_insert_with(|| {
                                SessionHandler::new(
                                    &reactor,
                                    packet.session(),
                                    downstream_sender.clone(),
                                )
                            })
                            .send((packet, addr))
                            .map_err(Error::UpstreamChannelClosed)?;
                    }
                    Ok(packet @ Packet::Data { .. }) => {
                        let session = packet.session();
                        if let Some(session_handler) = session_handlers.get(&session) {
                            session_handler
                                .send((packet, addr))
                                .map_err(Error::UpstreamChannelClosed)?;
                        } else {
                            info!("data: cannot find session {session:?}");

                            close_invalid_session(&socket, &mut session_handlers, session, addr)
                                .await?;
                        }
                    }
                    Ok(packet @ Packet::Ack { .. }) => {
                        let session = packet.session();
                        if let Some(session_handler) = session_handlers.get(&session) {
                            session_handler
                                .send((packet, addr))
                                .map_err(Error::UpstreamChannelClosed)?;
                        } else {
                            info!("ack: cannot find session {session:?}");

                            close_invalid_session(&socket, &mut session_handlers, session, addr)
                                .await?;
                        }
                    }
                    Ok(packet @ Packet::Close { .. }) => {
                        let session = packet.session();
                        if let Some(session_handler) = session_handlers.remove(&session) {
                            session_handler
                                .send((Packet::Close { session }, addr))
                                .map_err(Error::UpstreamChannelClosed)?;
                        } else {
                            info!("close : cannot find session {session:?}");

                            close_invalid_session(&socket, &mut session_handlers, session, addr)
                                .await?;
                        }
                    }
                    Err(err) => {
                        info!("invalid packet: {err}");
                    }
                }

                receiver = current_receiver;
                packets = Box::pin(socket.recv_from().fuse());
            }
        }
    }
}

async fn close_invalid_session(
    socket: &UdpSocket,
    session_handlers: &mut HashMap<Session, SessionHandler>,
    session: Session,
    addr: network::IpSocketAddress,
) -> Result<(), Error> {
    session_handlers.remove(&session);

    let mut data = Vec::new();
    let len = data.write_value(&Packet::Close { session }).unwrap();
    debug_assert!(len <= 1000);

    socket.send_to(data, addr).await?;

    Ok(())
}
