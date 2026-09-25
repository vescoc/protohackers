#![doc = include_str!("../README.md")]

use wasi::io::streams::StreamError;
use wasi::sockets::network::{ErrorCode, IpSocketAddress};

use thiserror::Error;
use tracing::{debug, info, instrument};

use wasi_async::io::AsyncWrite;
use wasi_async::net::TcpStream;

#[allow(warnings)]
mod bindings;

#[derive(Error, Debug)]
pub enum Error {
    #[error("stream error: {0}")]
    Stream(#[from] StreamError),

    #[error("tcp socket error: {0}")]
    TcpSocket(#[from] ErrorCode),
}

/// # Errors
#[instrument(skip(stream))]
pub async fn run(address: IpSocketAddress, mut stream: TcpStream) -> Result<(), Error> {
    info!("run: {address:?}");

    let (mut read, mut write) = stream.split();
    let r = async move {
        loop {
            let bytes = write.splice(&mut read, 1024).await?;
            if bytes == 0 {
                break;
            }
            write.flush().await?;
            debug!("read-write {bytes} bytes");
        }
        Ok(())
    }
    .await;

    stream.close().await.ok();

    match r {
        Err(Error::Stream(StreamError::Closed)) | Ok(()) => Ok(()),
        Err(e) => Err(e),
    }
}
