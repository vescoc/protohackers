use std::mem::ManuallyDrop;

use wasi::sockets::instance_network::instance_network;
use wasi::sockets::network::{self, IpSocketAddress, Ipv4SocketAddress, Ipv6SocketAddress};
use wasi::sockets::udp::{
    IncomingDatagram, IncomingDatagramStream, OutgoingDatagram, OutgoingDatagramStream,
    UdpSocket as WasiUdpSocket,
};
use wasi::sockets::udp_create_socket::create_udp_socket;

use wasi_async_runtime::sync::RwLock;
use wasi_async_runtime::Reactor;

use tracing::{instrument, trace, warn};

use crate::net::{ip_address_family, LocalSocketAddress, ToSocketAddrs};

pub struct UdpSocket {
    reactor: Reactor,
    network: network::Network,
    socket: ManuallyDrop<WasiUdpSocket>,
    inner: RwLock<UdpSocketInner>,
}

enum UdpSocketInner {
    Uninitialized,

    Bind {
        incoming_datagram_stream: IncomingDatagramStream,
        outgoing_datagram_stream: OutgoingDatagramStream,
    },

    Connect {
        incoming_datagram_stream: IncomingDatagramStream,
        outgoing_datagram_stream: OutgoingDatagramStream,
    },
}

impl UdpSocket {
    /// # Errors
    /// # Panics
    #[instrument(skip_all)]
    pub async fn bind(
        reactor: Reactor,
        address: impl ToSocketAddrs,
    ) -> Result<Self, network::ErrorCode> {
        let network = instance_network();

        let socket_address = address.to_socket_addr(&reactor, &network).await?;

        let family = ip_address_family(&socket_address);

        let socket = create_udp_socket(family)?;

        socket.start_bind(&network, socket_address)?;
        loop {
            match socket.finish_bind() {
                Err(network::ErrorCode::WouldBlock) => {
                    reactor.wait_for(socket.subscribe()).await;
                }
                Err(err) => return Err(err),
                Ok(()) => break,
            }
        }

        let (incoming_datagram_stream, outgoing_datagram_stream) = socket.stream(None)?;

        Ok(Self {
            reactor,
            network,
            socket: ManuallyDrop::new(socket),
            inner: RwLock::new(UdpSocketInner::Bind {
                incoming_datagram_stream,
                outgoing_datagram_stream,
            }),
        })
    }

    /// # Errors
    /// # Panics
    #[instrument(skip_all)]
    pub async fn connect(&self, address: impl ToSocketAddrs) -> Result<(), network::ErrorCode> {
        let socket_address = address.to_socket_addr(&self.reactor, &self.network).await?;

        let inner = &mut *self.inner.write().await;
        *inner = UdpSocketInner::Uninitialized;

        let (incoming_datagram_stream, outgoing_datagram_stream) =
            self.socket.stream(Some(socket_address))?;

        *inner = UdpSocketInner::Connect {
            incoming_datagram_stream,
            outgoing_datagram_stream,
        };

        Ok(())
    }

    /// # Errors
    /// # Panics
    pub fn local_addr(&self) -> Result<LocalSocketAddress, network::ErrorCode> {
        match self.socket.local_address()? {
            IpSocketAddress::Ipv4(Ipv4SocketAddress { port, .. })
            | IpSocketAddress::Ipv6(Ipv6SocketAddress { port, .. }) => Ok(LocalSocketAddress(port)),
        }
    }

    /// # Errors
    /// # Panics
    #[instrument(skip_all)]
    pub async fn send(&self, data: Vec<u8>) -> Result<usize, network::ErrorCode> {
        trace!("send {}", data.len());
        if let UdpSocketInner::Connect {
            outgoing_datagram_stream,
            ..
        } = &*self.inner.read().await {
            while outgoing_datagram_stream.check_send()? == 0 {
                self.reactor
                    .wait_for(outgoing_datagram_stream.subscribe())
                    .await;
            }
            let len = data.len();
            if outgoing_datagram_stream.send(&[OutgoingDatagram {
                data,
                remote_address: None,
            }])? == 0
            {
                warn!("???");
                return Err(network::ErrorCode::InvalidState);
            }
            Ok(len)
        } else {
            warn!("invalid state");
            return Err(network::ErrorCode::InvalidState);
        }
    }

    /// # Errors
    /// # Panics
    #[instrument(skip_all)]
    pub async fn send_to(
        &self,
        data: Vec<u8>,
        address: impl ToSocketAddrs,
    ) -> Result<usize, network::ErrorCode> {
        let socket_address = address.to_socket_addr(&self.reactor, &self.network).await?;

        match &*self.inner.read().await {
            UdpSocketInner::Connect {
                outgoing_datagram_stream,
                ..
            }
            | UdpSocketInner::Bind {
                outgoing_datagram_stream,
                ..
            } => {
                while outgoing_datagram_stream.check_send()? == 0 {
                    self.reactor
                        .wait_for(outgoing_datagram_stream.subscribe())
                        .await;
                }
                let len = data.len();
                if outgoing_datagram_stream.send(&[OutgoingDatagram {
                    data,
                    remote_address: Some(socket_address),
                }])? == 0
                {
                    return Err(network::ErrorCode::InvalidState);
                }
                Ok(len)
            }
            UdpSocketInner::Uninitialized => {
                warn!("invalid state");
                return Err(network::ErrorCode::InvalidState);
            }
        }
    }

    /// # Errors
    /// # Panics
    #[instrument(skip_all)]
    pub async fn recv(&self) -> Result<Vec<u8>, network::ErrorCode> {
        match &*self.inner.read().await {
            UdpSocketInner::Connect {
                incoming_datagram_stream,
                ..
            }
            | UdpSocketInner::Bind {
                incoming_datagram_stream,
                ..
            } => {
                trace!("recv");
                loop {
                    if let Some(IncomingDatagram { data, .. }) =
                        incoming_datagram_stream.receive(1)?.pop()
                    {
                        return Ok(data);
                    }
                    
                    self.reactor
                        .wait_for(incoming_datagram_stream.subscribe())
                        .await;
                }
            }
            UdpSocketInner::Uninitialized => {
                warn!("invalid state");
                return Err(network::ErrorCode::InvalidState);
            }
        }
    }

    /// # Errors
    /// # Panics
    #[instrument(skip_all)]
    pub async fn recv_from(&self) -> Result<(Vec<u8>, IpSocketAddress), network::ErrorCode> {
        match &*self.inner.read().await {
            UdpSocketInner::Connect {
                incoming_datagram_stream,
                ..
            }
            | UdpSocketInner::Bind {
                incoming_datagram_stream,
                ..
            } => loop {
                if let Some(IncomingDatagram {
                    data,
                    remote_address,
                }) = incoming_datagram_stream.receive(1)?.pop()
                {
                    return Ok((data, remote_address));
                }

                self.reactor
                    .wait_for(incoming_datagram_stream.subscribe())
                    .await;
            },
            UdpSocketInner::Uninitialized => {
                warn!("invalid state");
                return Err(network::ErrorCode::InvalidState);
            }
        }
    }
}
