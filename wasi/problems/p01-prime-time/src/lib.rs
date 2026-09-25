#![doc = include_str!("../README.md")]

use futures::StreamExt;

use wasi::io::streams::StreamError;
use wasi::sockets::network::{self, IpSocketAddress};

use thiserror::Error;

use tracing::{debug, info, instrument, warn};

use serde::{Deserialize, Serialize};

use wasi_async::codec::{FramedRead, LinesDecoder};
use wasi_async::io::{AsyncWrite, AsyncWriteExt};
use wasi_async::net::TcpStream;

#[allow(warnings)]
mod bindings;

const METHOD: &str = "isPrime";

#[derive(Serialize, Deserialize)]
pub struct Request<'a> {
    pub method: &'a str,
    pub number: serde_json::Number,
}

#[derive(Serialize, Deserialize)]
pub struct Response<'a> {
    pub method: &'a str,
    pub prime: bool,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("stream error: {0}")]
    Stream(#[from] StreamError),

    #[error("tcp socket error: {0}")]
    TcpSocket(#[from] network::ErrorCode),

    #[error("serde json error: {0}")]
    Serde(#[from] serde_json::Error),
}

#[instrument(skip(stream))]
pub async fn run(address: IpSocketAddress, mut stream: TcpStream) -> Result<(), Error> {
    info!("run");

    let (read, mut write) = stream.split();
    let r = async move {
        let mut read = std::pin::pin!(FramedRead::new(read, LinesDecoder::new()).into_stream());

        while let Some(value) = read.next().await {
            let value = value?;
            match check(&value) {
                Ok(is_prime) => {
                    write
                        .write_all(&serde_json::to_vec(&Response {
                            method: METHOD,
                            prime: is_prime,
                        })?)
                        .await?;
                    write.write(b"\n").await?;
                }
                Err(err) => {
                    warn!("invalid request: {err}");
                    write.write_all(err.as_bytes()).await?;
                    write.write(b"\n").await?;
                    break;
                }
            }
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

fn check(buffer: &[u8]) -> Result<bool, &'static str> {
    debug!("buffer: {}", String::from_utf8_lossy(buffer));
    let request = serde_json::from_slice::<Request>(buffer).map_err(|_| "MALFORMED")?;
    match (request.method, request.number.as_u64()) {
        ("isPrime", Some(n)) => {
            let result = primes::is_prime(n);
            debug!("isPrime for {n}: {result}");
            Ok(result)
        }
        ("isPrime", None) => {
            debug!("isPrime for a non u64 number");
            Ok(false)
        }
        _ => Err("MALFORMED"),
    }
}
