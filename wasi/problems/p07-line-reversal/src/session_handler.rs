use std::iter;
use std::time::Duration;

use bytes::{BufMut, BytesMut};

use tracing::{debug, info, instrument};

use futures::channel::mpsc;

use wasi::sockets::network::IpSocketAddress;

use wasi_async::time::{Elapsed, Instant, timeout};
use wasi_async_runtime::Reactor;

use crate::lrcp::packets::{self, Numeric, Packet, Payload, Session};

const ACK_TIMEOUT: u64 = 3;
const CONNECTION_TIMEOUT: u64 = 60;

pub struct SessionHandler {
    upstream_sender: mpsc::UnboundedSender<(Packet, IpSocketAddress)>,
}

impl SessionHandler {
    pub fn new(
        reactor: &Reactor,
        id: Session,
        downstream_sender: mpsc::UnboundedSender<(Packet, IpSocketAddress)>,
    ) -> Self {
        let (upstream_sender, upstream_receiver) = mpsc::unbounded();

        reactor.spawn(Self::run(id, upstream_receiver, downstream_sender));

        Self { upstream_sender }
    }

    #[instrument(skip(upstream_receiver, downstream_sender))]
    #[allow(clippy::too_many_lines)]
    #[expect(clippy::cast_possible_truncation, reason = "wasm32")]
    async fn run(
        id: Session,
        mut upstream_receiver: mpsc::UnboundedReceiver<(Packet, IpSocketAddress)>,
        downstream_sender: mpsc::UnboundedSender<(Packet, IpSocketAddress)>,
    ) {
        let mut timestamp = Instant::now();
        let mut current_timeout = Duration::from_secs(CONNECTION_TIMEOUT);

        let mut upstream = BytesMut::with_capacity(1024);
        let mut lowermark = 0;

        let mut downstream = BytesMut::with_capacity(1204);
        let mut client_ack = 0;
        let mut current_trasmitted_highmark = 0;
        let mut transmit_data = false;

        let mut receiver = Box::pin(upstream_receiver.recv());

        let mut client_addr = None;
        loop {
            match timeout(current_timeout, receiver).await {
                Err(Elapsed(current_receiver)) => {
                    debug!("timeout {timestamp:?}");

                    let elapsed = timestamp.elapsed();

                    if elapsed >= Duration::from_secs(60) {
                        debug!("expired session: {id:?}");
                        if let Some(addr) = client_addr {
                            downstream_sender
                                .unbounded_send((Packet::Close { session: id }, addr))
                                .ok();
                        }
                        break;
                    }

                    if client_ack < downstream.len() {
                        debug!("waiting ack expired, reset to {client_ack}");
                        transmit_data = true;
                        current_trasmitted_highmark = client_ack;
                    }

                    receiver = current_receiver;
                }
                Ok(packet) => {
                    match packet {
                        Ok((packet, addr)) => {
                            timestamp = Instant::now();

                            debug!("got packet {packet:?}");

                            match packet {
                                Packet::Connect { session } => {
                                    debug_assert_eq!(session, id);

                                    client_addr = Some(addr);

                                    downstream_sender
                                        .unbounded_send((
                                            Packet::Ack {
                                                session,
                                                length: Numeric(0),
                                            },
                                            addr,
                                        ))
                                        .ok();
                                }
                                Packet::Data {
                                    session,
                                    pos: Numeric(pos),
                                    data: Payload(data),
                                } => {
                                    debug_assert_eq!(session, id);

                                    if let Some(len) = upstream.len().checked_sub(pos as usize)
                                        && data.len() >= len
                                    {
                                        upstream.extend_from_slice(&data.as_bytes()[len..]);
                                        while let Some(highmark) = upstream
                                            .iter()
                                            .skip(lowermark)
                                            .position(|c| *c == b'\n')
                                        {
                                            debug!(
                                                "found line: '{}'",
                                                std::str::from_utf8(
                                                    &upstream[lowermark..lowermark + highmark]
                                                )
                                                .unwrap()
                                            );

                                            downstream.reserve(highmark + 1);
                                            for c in upstream
                                                .iter()
                                                .skip(lowermark)
                                                .take(highmark)
                                                .rev()
                                                .chain(iter::once(&b'\n'))
                                            {
                                                downstream.put_u8(*c);
                                            }

                                            lowermark += highmark + 1;
                                        }

                                        if current_trasmitted_highmark < downstream.len() {
                                            transmit_data = true;
                                        }
                                    }

                                    downstream_sender
                                        .unbounded_send((
                                            Packet::Ack {
                                                session,
                                                length: Numeric(upstream.len() as u32),
                                            },
                                            client_addr.expect("no connected"),
                                        ))
                                        .unwrap();
                                }
                                Packet::Ack {
                                    session,
                                    length: Numeric(length),
                                } => {
                                    let length = length as usize;
                                    if length > downstream.len() {
                                        info!(
                                            "ask invalid {length} > {}, close session",
                                            downstream.len()
                                        );
                                        downstream_sender
                                            .unbounded_send((
                                                Packet::Close { session },
                                                client_addr.expect("no connected"),
                                            ))
                                            .ok();
                                        break;
                                    }

                                    if length > client_ack {
                                        client_ack = length;
                                        current_trasmitted_highmark = client_ack;
                                        if current_trasmitted_highmark < downstream.len() {
                                            transmit_data = true;
                                        }
                                    }
                                }
                                Packet::Close { session } => {
                                    debug_assert_eq!(session, id);

                                    debug!("session {session:?} done");

                                    downstream_sender
                                        .unbounded_send((
                                            Packet::Close { session },
                                            client_addr.expect("no connected"),
                                        ))
                                        .ok();

                                    break;
                                }
                            }
                        }
                        Err(err) => {
                            info!("got upstream error: {err:?}");
                        }
                    }

                    receiver = Box::pin(upstream_receiver.recv());
                }
            }

            if transmit_data {
                let length = downstream.len();

                let data = &downstream[current_trasmitted_highmark..];
                for data in data.chunks(packets::BUFFER_SIZE) {
                    let len = data.len();

                    let data = Payload::new(data).expect("line too long!");

                    downstream_sender
                        .unbounded_send((
                            Packet::Data {
                                session: id,
                                pos: Numeric(current_trasmitted_highmark as u32),
                                data,
                            },
                            client_addr.expect("not connected"),
                        ))
                        .ok();

                    current_trasmitted_highmark += len;
                }

                current_trasmitted_highmark = length;
                transmit_data = false;
                current_timeout = Duration::from_secs(ACK_TIMEOUT);
            } else if client_ack < current_trasmitted_highmark {
                current_timeout = Duration::from_secs(ACK_TIMEOUT);
            } else {
                current_timeout = Duration::from_secs(CONNECTION_TIMEOUT);
            }
        }
    }

    pub fn send(&self, packet: (Packet, IpSocketAddress)) -> Result<(), &'static str> {
        self.upstream_sender
            .unbounded_send(packet)
            .map_err(|_| "channel closed")
    }
}
