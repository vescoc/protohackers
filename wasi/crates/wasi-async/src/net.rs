use wasi::sockets::network::{IpAddressFamily, IpSocketAddress};

pub mod tcp;
pub mod udp;

pub use tcp::{TcpListener, TcpStream};
pub use udp::UdpSocket;

pub struct LocalSocketAddress(u16);

impl LocalSocketAddress {
    #[must_use]
    pub fn port(&self) -> u16 {
        self.0
    }
}

#[allow(private_bounds)]
pub trait ToSocketAddrs: sealed::ToSocketAddrs {}

impl ToSocketAddrs for IpSocketAddress {}
impl ToSocketAddrs for String {}

pub(crate) fn ip_address_family(socket_address: &IpSocketAddress) -> IpAddressFamily {
    match socket_address {
        IpSocketAddress::Ipv4(..) => IpAddressFamily::Ipv4,
        IpSocketAddress::Ipv6(..) => IpAddressFamily::Ipv6,
    }
}

pub(crate) mod sealed {
    use std::future::Future;

    use wasi::sockets::ip_name_lookup::resolve_addresses;
    use wasi::sockets::network::{self, IpAddress, IpSocketAddress, Ipv4SocketAddress, Network};

    use wasi_async_runtime::Reactor;

    use tracing::{instrument, trace, warn};

    #[doc(hidden)]
    pub(crate) trait ToSocketAddrs {
        fn to_socket_addr(
            &self,
            reactor: &Reactor,
            network: &Network,
        ) -> impl Future<Output = Result<IpSocketAddress, network::ErrorCode>>;
    }

    impl ToSocketAddrs for IpSocketAddress {
        #[allow(clippy::unused_async_trait_impl)]
        async fn to_socket_addr(
            &self,
            _: &Reactor,
            _: &Network,
        ) -> Result<IpSocketAddress, network::ErrorCode> {
            Ok(*self)
        }
    }

    impl ToSocketAddrs for String {
        #[instrument(skip_all)]
        async fn to_socket_addr(
            &self,
            reactor: &Reactor,
            network: &Network,
        ) -> Result<IpSocketAddress, network::ErrorCode> {
            let (address, port) = self
                .split_once(':')
                .ok_or(network::ErrorCode::InvalidArgument)?;

            let port = port
                .parse()
                .map_err(|_| network::ErrorCode::InvalidArgument)?;

            let addresses = resolve_addresses(network, address)?;
            loop {
                match addresses.resolve_next_address() {
                    Err(network::ErrorCode::WouldBlock) => {
                        let subscription = addresses.subscribe();
                        trace!("addresses subscribe {subscription:?}");
                        reactor.wait_for(subscription).await;
                    }
                    Ok(Some(IpAddress::Ipv4(address))) => {
                        return Ok(IpSocketAddress::Ipv4(Ipv4SocketAddress { address, port }))
                    }
                    Ok(Some(IpAddress::Ipv6(addr))) => {
                        warn!("ignoring ipv6 address {addr:?}");
                    }
                    Ok(None) => return Err(network::ErrorCode::NameUnresolvable),
                    Err(err) => return Err(err),
                }
            }
        }
    }
}
