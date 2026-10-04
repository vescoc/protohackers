#![doc = include_str!("../README.md")]

use std::iter;

use wasi::io::streams::StreamError;
use wasi::sockets::network::{ErrorCode, IpSocketAddress};

use wasi_async::io::{AsyncRead, AsyncWriteExt};
use wasi_async::net::TcpStream;

use bytes::{Buf, BytesMut};

use thiserror::Error;
use tracing::{debug, info, instrument};

const BUFFER_SIZE: usize = 5000;

const CIPHER_IGNORE_INVALID_N: bool = true;

#[derive(Error, Debug)]
pub enum Error {
    #[error("stream error: {0}")]
    Stream(#[from] StreamError),

    #[error("tcp socket error: {0}")]
    TcpSocket(#[from] ErrorCode),

    #[error("cipher error: {0}")]
    Cipher(#[from] CipherError),
}

/// # Errors
#[instrument(skip(stream))]
pub async fn run(address: IpSocketAddress, mut stream: TcpStream) -> Result<(), Error> {
    info!("run: {address:?}");

    let (mut read, mut write) = stream.split();
    let r = async move {
        let mut server = Server::new(&mut read, &mut write);

        let (cipher, _) = server.parse_cipher().await?;
        debug!("cipher: {cipher:?}");

        server.handle_request(&cipher).await?;

        Ok(())
    }
    .await;

    stream.close().await.ok();

    match r {
        Err(Error::Stream(StreamError::Closed)) => {
            debug!("remote close");
            Ok(())
        }
        Ok(()) => Ok(()),
        Err(e) => Err(e),
    }
}

struct Server<'a, R, W> {
    upstream: &'a mut R,
    downstream: &'a mut W,
    buffer: BytesMut,
}

impl<'a, R, W> Server<'a, R, W> {
    fn new(upstream: &'a mut R, downstream: &'a mut W) -> Self {
        Self {
            upstream,
            downstream,
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }
}

impl<R, W> Server<'_, R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWriteExt + Unpin,
{
    /// Parse the first bytes of the request for get the [`Cipher`]
    ///
    /// # Errors
    ///
    /// - stream errors
    /// - cipher parsing errors
    async fn parse_cipher(&mut self) -> Result<(Cipher, usize), Error> {
        loop {
            if let Some(end_cipher_pos) = self.buffer.iter().position(|c| *c == 0x00) {
                let cipher = Cipher::try_from(&self.buffer[..end_cipher_pos])?;

                self.buffer.advance(end_cipher_pos + 1);

                return Ok((cipher, end_cipher_pos + 1));
            }

            let bytes = self.upstream.read(80).await?;
            self.buffer.extend_from_slice(&bytes);
        }
    }

    #[instrument(skip(self))]
    async fn handle_request(&mut self, cipher: &Cipher) -> Result<(), Error> {
        let mut upstream_index = 0;

        for value in self.buffer.iter_mut() {
            *value = cipher.decode(upstream_index, *value);
            upstream_index += 1;
        }

        let mut downstream_index = 0;
        loop {
            // parse current decoded buffer
            while let Some(newline) = self.buffer.iter().position(|c| *c == b'\n') {
                let downstream = if let Some(toy) = max_toy(&self.buffer[..newline]) {
                    toy.iter()
                        .copied()
                        .chain(iter::once(b'\n'))
                        .enumerate()
                        .map(|(position, value)| cipher.encode(position + downstream_index, value))
                        .collect::<Vec<_>>()
                } else {
                    iter::once(b'\n')
                        .enumerate()
                        .map(|(position, value)| cipher.encode(position + downstream_index, value))
                        .collect::<Vec<_>>()
                };

                self.downstream.write_all(&downstream).await?;
                downstream_index += downstream.len();

                self.buffer.advance(newline + 1);
            }

            // read bytes and add to buffer the bytes decoded
            let mut buffer = self.upstream.read(BUFFER_SIZE as u64).await?;
            for c in &mut buffer.iter_mut() {
                *c = cipher.decode(upstream_index, *c);
                upstream_index += 1;
            }

            self.buffer.extend_from_slice(&buffer);
        }
    }
}

fn max_toy(buffer: &[u8]) -> Option<&[u8]> {
    buffer
        .split(|c| *c == b',')
        .map(|toy| {
            let mut value = 0u32;
            for c in toy {
                if c.is_ascii_digit() {
                    value = value * 10 + u32::from(c - b'0');
                } else {
                    break;
                }
            }

            (value, toy)
        })
        .max()
        .map(|(_, toy)| toy)
}

#[derive(Debug)]
enum Operation {
    Reversebits,
    Xor(u8),
    Xorpos,
    Add(u8),
    Addpos,
}

impl Operation {
    #[expect(clippy::cast_possible_truncation, reason = "by spec")]
    fn encode(&self, position: usize, value: u8) -> u8 {
        match self {
            Operation::Reversebits => value.reverse_bits(),
            Operation::Xor(n) => value ^ n,
            Operation::Xorpos => ((position ^ usize::from(value)) % 256) as u8,
            Operation::Add(n) => value.wrapping_add(*n),
            Operation::Addpos => ((usize::from(value).wrapping_add(position)) % 256) as u8,
        }
    }

    #[expect(clippy::cast_possible_truncation, reason = "by spec")]
    fn decode(&self, position: usize, value: u8) -> u8 {
        match self {
            Operation::Reversebits => value.reverse_bits(),
            Operation::Xor(n) => value ^ n,
            Operation::Xorpos => ((position ^ usize::from(value)) % 256) as u8,
            Operation::Add(n) => value.wrapping_sub(*n),
            Operation::Addpos => ((usize::from(value).wrapping_sub(position)) % 256) as u8,
        }
    }
}

#[derive(Debug)]
pub struct Cipher {
    operations: Vec<Operation>,
}

impl Cipher {
    fn encode(&self, position: usize, value: u8) -> u8 {
        self.operations
            .iter()
            .fold(value, |value, operation| operation.encode(position, value))
    }

    fn decode(&self, position: usize, value: u8) -> u8 {
        self.operations
            .iter()
            .rev()
            .fold(value, |value, operation| operation.decode(position, value))
    }
}

#[derive(Error, Debug)]
pub enum CipherError {
    #[error("invalid cipher spec")]
    InvalidSpec(u8),

    #[error("invalid N")]
    InvalidN,

    #[error("eof")]
    EOF,

    #[error("invalid cipher")]
    InvalidCipher(Cipher),
}

impl TryFrom<&[u8]> for Cipher {
    type Error = CipherError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        let mut operations = Vec::with_capacity(80);

        let mut cursor = data.iter();
        while let Some(value) = cursor.next() {
            match value {
                0x01 => operations.push(Operation::Reversebits),
                0x02 => operations.push(Operation::Xor(if CIPHER_IGNORE_INVALID_N {
                    cursor.next().copied().ok_or(CipherError::EOF)?
                } else {
                    cursor.next().ok_or(CipherError::EOF).and_then(|n| {
                        if *n == 0 {
                            Err(CipherError::InvalidN)
                        } else {
                            Ok(*n)
                        }
                    })?
                })),
                0x03 => operations.push(Operation::Xorpos),
                0x04 => operations.push(Operation::Add(if CIPHER_IGNORE_INVALID_N {
                    cursor.next().copied().ok_or(CipherError::EOF)?
                } else {
                    cursor.next().ok_or(CipherError::EOF).and_then(|n| {
                        if *n == 0 {
                            Err(CipherError::InvalidN)
                        } else {
                            Ok(*n)
                        }
                    })?
                })),
                0x05 => operations.push(Operation::Addpos),
                _ => return Err(CipherError::InvalidSpec(*value)),
            }
        }

        let cipher = Cipher { operations };
        if b"hello world!"
            .iter()
            .enumerate()
            .any(|(position, &c)| c != cipher.encode(position, c))
        {
            Ok(cipher)
        } else {
            Err(CipherError::InvalidCipher(cipher))
        }
    }
}
