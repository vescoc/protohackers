use std::mem::ManuallyDrop;
use std::rc::Rc;

use futures::{stream, Stream};

use tracing::{instrument, trace};

use wasi::sockets::instance_network::instance_network;
use wasi::sockets::network::{ErrorCode, IpSocketAddress, Ipv4SocketAddress, Ipv6SocketAddress};
use wasi::sockets::tcp::{ShutdownType, TcpSocket};
use wasi::sockets::tcp_create_socket::create_tcp_socket;

use wasi::io::streams::{InputStream, OutputStream, StreamError};

use wasi_async_runtime::Reactor;

use crate::io::{AsyncRead, AsyncWrite};
use crate::net::{ip_address_family, LocalSocketAddress, ToSocketAddrs};

pub struct TcpListener {
    reactor: Reactor,
    socket: ManuallyDrop<TcpSocket>,
}

impl TcpListener {
    /// # Errors
    #[instrument(skip_all)]
    pub async fn bind(reactor: Reactor, address: impl ToSocketAddrs) -> Result<Self, ErrorCode> {
        let network = instance_network();

        let socket_address = address.to_socket_addr(&reactor, &network).await?;

        let family = ip_address_family(&socket_address);

        let socket = create_tcp_socket(family)?;

        socket.start_bind(&network, socket_address)?;
        loop {
            match socket.finish_bind() {
                Err(ErrorCode::WouldBlock) => {
                    let subscription = socket.subscribe();
                    trace!("socket subscription finish bind {subscription:?}");
                    reactor.wait_for(subscription).await;
                }
                Err(err) => return Err(err),
                Ok(()) => break,
            }
        }

        socket.start_listen()?;
        loop {
            match socket.finish_listen() {
                Err(ErrorCode::WouldBlock) => {
                    let subscription = socket.subscribe();
                    trace!("socket subscription finish listener {subscription:?}");
                    reactor.wait_for(subscription).await;
                }
                Err(err) => return Err(err),
                Ok(()) => break,
            }
        }

        Ok(Self {
            reactor,
            socket: ManuallyDrop::new(socket),
        })
    }

    #[instrument(skip_all)]
    pub fn into_stream(
        self,
    ) -> impl Stream<Item = Result<(TcpStream, IpSocketAddress), ErrorCode>> {
        stream::unfold(self, |this| async move {
            let reactor = this.reactor.clone();

            let subscription = this.socket.subscribe();

            trace!("into_stream socket subscription {subscription:?}");
            reactor.wait_for(subscription).await;

            match this.socket.accept() {
                Ok((socket, input_stream, output_stream)) => {
                    let address = match socket.remote_address() {
                        Ok(address) => address,
                        Err(err) => return Some((Err(err), this)),
                    };

                    Some((
                        Ok((
                            TcpStream(Some(TcpStreamInner {
                                reactor: this.reactor.clone(),
                                socket: ManuallyDrop::new(socket),
                                input_stream: ManuallyDrop::new(input_stream),
                                output_stream: ManuallyDrop::new(output_stream),
                            })),
                            address,
                        )),
                        this,
                    ))
                }
                Err(err) => Some((Err(err), this)),
            }
        })
    }

    /// # Errors
    #[instrument(skip_all)]
    pub async fn accept(&self) -> Result<(TcpStream, IpSocketAddress), ErrorCode> {
        let subscription = self.socket.subscribe();
        trace!("accept socket subscription {subscription:?}");
        self.reactor.wait_for(subscription).await;

        let (socket, input_stream, output_stream) = self.socket.accept()?;

        let address = socket.remote_address()?;

        Ok((
            TcpStream(Some(TcpStreamInner {
                reactor: self.reactor.clone(),
                socket: ManuallyDrop::new(socket),
                input_stream: ManuallyDrop::new(input_stream),
                output_stream: ManuallyDrop::new(output_stream),
            })),
            address,
        ))
    }

    /// # Errors
    pub fn local_addr(&self) -> Result<LocalSocketAddress, ErrorCode> {
        match self.socket.local_address()? {
            IpSocketAddress::Ipv4(Ipv4SocketAddress { port, .. })
            | IpSocketAddress::Ipv6(Ipv6SocketAddress { port, .. }) => Ok(LocalSocketAddress(port)),
        }
    }
}

struct TcpStreamInner {
    reactor: Reactor,
    // There is a panic in drop, I must manually drop the socket
    socket: ManuallyDrop<TcpSocket>,
    input_stream: ManuallyDrop<InputStream>,
    output_stream: ManuallyDrop<OutputStream>,
}

pub struct TcpStream(Option<TcpStreamInner>);

impl TcpStream {
    /// # Errors
    #[instrument(skip_all)]
    pub async fn connect(
        reactor: Reactor,
        remote_address: impl ToSocketAddrs,
    ) -> Result<Self, ErrorCode> {
        let network = instance_network();

        let socket_address = remote_address.to_socket_addr(&reactor, &network).await?;

        let family = ip_address_family(&socket_address);

        let socket = create_tcp_socket(family)?;

        socket.start_connect(&network, socket_address)?;

        let subscription = socket.subscribe();
        trace!("connect socket subscription {subscription:?}");
        reactor.wait_for(subscription).await;

        let (input_stream, output_stream) = socket.finish_connect()?;

        Ok(Self(Some(TcpStreamInner {
            reactor,
            socket: ManuallyDrop::new(socket),
            input_stream: ManuallyDrop::new(input_stream),
            output_stream: ManuallyDrop::new(output_stream),
        })))
    }

    /// # Panics
    pub fn into_split(mut self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let TcpStreamInner {
            socket,
            reactor,
            input_stream,
            output_stream,
        } = self.0.take().unwrap();

        let socket = Rc::new(socket);
        let read = OwnedReadHalf {
            socket: socket.clone(),
            reactor: reactor.clone(),
            input_stream,
        };
        let write = OwnedWriteHalf {
            socket,
            reactor,
            output_stream,
        };
        (read, write)
    }

    /// # Panics
    pub fn split(&mut self) -> (ReadHalf<'_>, WriteHalf<'_>) {
        let this = self.0.as_mut().unwrap();

        let read = ReadHalf {
            reactor: this.reactor.clone(),
            input_stream: &mut this.input_stream,
        };
        let write = WriteHalf {
            reactor: this.reactor.clone(),
            socket: &this.socket,
            output_stream: &mut this.output_stream,
        };
        (read, write)
    }

    /// # Errors
    /// # Panics
    #[expect(clippy::unused_async_trait_impl, clippy::unused_async)]
    pub async fn close(self) -> Result<(), ErrorCode> {
        if let Some(TcpStreamInner { socket, .. }) = &self.0 {
            socket.shutdown(ShutdownType::Both)
        } else {
            Ok(())
        }
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        if let Some(TcpStreamInner { socket, .. }) = &self.0 {
            socket.shutdown(ShutdownType::Both).ok();
        }
    }
}

pub struct ReadHalf<'a> {
    reactor: Reactor,
    input_stream: &'a mut InputStream,
}

impl AsyncRead for ReadHalf<'_> {
    #[instrument(skip_all)]
    async fn read(&mut self, len: u64) -> Result<Vec<u8>, StreamError> {
        loop {
            let data = self.input_stream.read(len)?;
            if !data.is_empty() || len == 0 {
                return Ok(data);
            }

            let subscription = self.input_stream.subscribe();
            trace!("input stream subscription {subscription:?} len {len}");
            self.reactor.wait_for(subscription).await;

            let data = self.input_stream.read(len)?;
            if !data.is_empty() || len == 0 {
                return Ok(data);
            }
        }
    }
}

pub struct WriteHalf<'a> {
    reactor: Reactor,
    socket: &'a TcpSocket,
    output_stream: &'a mut OutputStream,
}

impl AsyncWrite for WriteHalf<'_> {
    #[instrument(skip_all)]
    #[expect(clippy::cast_possible_truncation)]
    async fn write(&mut self, data: &[u8]) -> Result<u64, StreamError> {
        if data.is_empty() {
            return Ok(0);
        }

        let len = loop {
            let len = self.output_stream.check_write()?;
            if len > 0 {
                break len;
            }
            trace!("cannot write");
            let subscription = self.output_stream.subscribe();
            trace!("socket output stream subscription {subscription:?}");
            self.reactor.wait_for(subscription).await;
        };

        let len = data.len().min(len as usize);

        self.output_stream.write(&data[0..len])?;

        Ok(len as u64)
    }

    #[instrument(skip_all)]
    async fn flush(&mut self) -> Result<(), StreamError> {
        self.output_stream.flush()?;
        while self.output_stream.check_write()? == 0 {
            let subscription = self.output_stream.subscribe();
            trace!("output stream subscription {subscription:?}");
            self.reactor.wait_for(subscription).await;
        }
        Ok(())
    }

    #[expect(clippy::unused_async_trait_impl)]
    async fn close(&mut self) -> Result<(), StreamError> {
        self.socket.shutdown(ShutdownType::Send).ok(); // TODO
        Ok(())
    }
}

impl WriteHalf<'_> {
    /// # Errors
    /// # Panics
    #[instrument(skip_all)]
    pub async fn splice(&mut self, read: &mut ReadHalf<'_>, len: u64) -> Result<u64, StreamError> {
        if len == 0 {
            return Ok(0);
        }

        while self.output_stream.check_write()?.min(len) == 0 {
            let subscription = self.output_stream.subscribe();
            trace!("output stream subscription {subscription:?}");
            self.reactor.wait_for(subscription).await;
        }

        loop {
            let len = self.output_stream.splice(read.input_stream, len)?;
            if len > 0 {
                return Ok(len);
            }
            let subscription = read.input_stream.subscribe();
            trace!("input stream subscription {subscription:?} len {len}");
            self.reactor.wait_for(subscription).await;
        }
    }
}

pub struct OwnedReadHalf {
    socket: Rc<ManuallyDrop<TcpSocket>>,
    reactor: Reactor,
    input_stream: ManuallyDrop<InputStream>,
}

impl Drop for OwnedReadHalf {
    fn drop(&mut self) {
        self.socket.shutdown(ShutdownType::Receive).ok();
    }
}

impl AsyncRead for OwnedReadHalf {
    #[instrument(skip_all)]
    async fn read(&mut self, len: u64) -> Result<Vec<u8>, StreamError> {
        loop {
            let data = self.input_stream.read(len)?;
            if !data.is_empty() || len == 0 {
                return Ok(data);
            }

            let subscription = self.input_stream.subscribe();
            trace!("input stream subscription {subscription:?} len {len}");
            self.reactor.wait_for(subscription).await;

            let data = self.input_stream.read(len)?;
            if data.is_empty() || len == 0 {
                return Ok(data);
            }
        }
    }
}

pub struct OwnedWriteHalf {
    socket: Rc<ManuallyDrop<TcpSocket>>,
    reactor: Reactor,
    output_stream: ManuallyDrop<OutputStream>,
}

impl Drop for OwnedWriteHalf {
    fn drop(&mut self) {
        self.socket.shutdown(ShutdownType::Send).ok();
    }
}

impl AsyncWrite for OwnedWriteHalf {
    #[instrument(skip_all)]
    #[expect(clippy::cast_possible_truncation)]
    async fn write(&mut self, data: &[u8]) -> Result<u64, StreamError> {
        let len = loop {
            let len = self.output_stream.check_write()?;
            if len > 0 {
                break len;
            }
            let subscription = self.output_stream.subscribe();
            trace!("output stream subscription {subscription:?}");
            self.reactor.wait_for(subscription).await;
        };

        let len = data.len().min(len as usize);

        self.output_stream.write(&data[0..len])?;

        Ok(len as u64)
    }

    #[instrument(skip_all)]
    async fn flush(&mut self) -> Result<(), StreamError> {
        self.output_stream.flush()?;
        while self.output_stream.check_write()? == 0 {
            let subscription = self.output_stream.subscribe();
            trace!("output stream subscription {subscription:?}");
            self.reactor.wait_for(subscription).await;
        }
        Ok(())
    }

    #[expect(clippy::unused_async_trait_impl)]
    async fn close(&mut self) -> Result<(), StreamError> {
        self.socket.shutdown(ShutdownType::Send).ok(); // TODO
        Ok(())
    }
}

impl OwnedWriteHalf {
    /// # Errors
    /// # Panics
    #[instrument(skip_all)]
    pub async fn splice(&mut self, read: &mut OwnedReadHalf, len: u64) -> Result<u64, StreamError> {
        if len == 0 {
            return Ok(0);
        }

        while self.output_stream.check_write()?.min(len) == 0 {
            let subscription = self.output_stream.subscribe();
            trace!("ouput stream subscription {subscription:?}");
            self.reactor.wait_for(subscription).await;
        }

        loop {
            let len = self.output_stream.splice(&read.input_stream, len)?;
            if len > 0 {
                return Ok(len);
            }
            let subscription = read.input_stream.subscribe();
            trace!("input stream subscription {subscription:?} len {len}");
            self.reactor.wait_for(subscription).await;
        }
    }
}
