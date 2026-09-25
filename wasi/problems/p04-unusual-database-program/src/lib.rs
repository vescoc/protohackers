#![doc = include_str!("../README.md")]

use std::collections::HashMap;

use wasi::io::streams::StreamError;
use wasi::sockets::network;

use wasi_async::net::UdpSocket;

use thiserror::Error;

use tracing::{debug, instrument};

#[allow(warnings)]
mod bindings;

#[derive(Error, Debug)]
pub enum Error {
    #[error("stream error {0}")]
    Stream(#[from] StreamError),

    #[error("udp socket error {0}")]
    UdpSocket(#[from] network::ErrorCode),
}

/// Solve the problem
///
/// # Errors
///
/// * [`Error`] - errors
#[instrument(skip_all)]
pub async fn run(socket: UdpSocket) -> Result<(), Error> {
    let mut data = HashMap::new();

    loop {
        let (packet, addr) = socket.recv_from().await?;

        let mut iter = packet.iter();
        if let Some(position) = iter.position(|c| *c == b'=') {
            let key = &packet[0..position];
            if key != b"version" {
                let value = iter.as_slice();
                data.insert(key.to_vec(), value.to_vec());
                debug!(
                    "[{:?}] insert key: {:?} value: {:?}",
                    addr,
                    std::str::from_utf8(key),
                    std::str::from_utf8(value)
                );
            }
        } else {
            let key = &packet;
            debug!("[{:?}] retrieve key: {:?}", addr, std::str::from_utf8(key));
            if key == b"version" {
                socket
                    .send_to(b"version=unusual-database-program 1.0.0".to_vec(), addr)
                    .await?;
            } else if let Some(value) = data.get(key) {
                debug!("[{:?}] value: {:?}", addr, std::str::from_utf8(value));

                socket
                    .send_to([key.as_slice(), value].join(b"=".as_slice()), addr)
                    .await?;
            } else {
                debug!("[{:?}] no value", addr);
            }
        }
    }
}
