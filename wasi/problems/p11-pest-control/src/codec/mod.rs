use std::marker::PhantomData;
use std::ops::ControlFlow;

use bytes::{Bytes, BytesMut};

use thiserror::Error;

use wasi::io::streams::StreamError;

pub mod packets;

#[derive(Error, Debug)]
pub enum Error {
    #[error("unknown packet: 0x{0:02x}")]
    UnknownPacket(u8),

    #[error("invalid packet")]
    InvalidPacket,

    #[error("invalid checksum")]
    InvalidChecksum,

    #[error("stream error: {0}")]
    StreamError(#[from] StreamError),
}

struct Parser<'a>(&'a [u8]);

impl<'a> Parser<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self(data)
    }

    fn read_u8(&mut self) -> u8 {
        let (r, rem) = self
            .0
            .split_first()
            .expect("invalid state, is data validated?");
        self.0 = rem;
        *r
    }

    fn read_u32(&mut self) -> u32 {
        let r = u32::from_be_bytes([self.0[0], self.0[1], self.0[2], self.0[3]]);
        self.0 = &self.0[4..];
        r
    }

    fn read_str(&mut self) -> &'a str {
        let len = u32::from_be_bytes([self.0[0], self.0[1], self.0[2], self.0[3]]) as usize;

        let r = std::str::from_utf8(&self.0[4..4 + len]).expect("invalid utf8");

        self.0 = &self.0[len + 4..];

        r
    }
}

pub trait RawPacketDecoder {
    type Decoded<'a>;

    fn decode(data: &[u8]) -> Self::Decoded<'_>;
}

#[derive(Debug, PartialEq)]
pub struct RawPacket<D> {
    data: Bytes,
    _marker: PhantomData<D>,
}

impl<D> RawPacket<D> {
    fn new(data: Bytes) -> Self {
        Self {
            data,
            _marker: PhantomData,
        }
    }
}

impl<D: RawPacketDecoder> RawPacket<D> {
    pub fn decode(&self) -> D::Decoded<'_> {
        D::decode(self.data.as_ref())
    }
}

struct Validator<'a> {
    data: &'a mut BytesMut,
    cursor: usize,
    length: Option<usize>,
}

impl<'a> Validator<'a> {
    fn new(data: &'a mut BytesMut) -> Self {
        Self {
            data,
            cursor: 0,
            length: None,
        }
    }

    fn validate_u8<P>(&mut self) -> ControlFlow<Result<Option<P>, Error>, u8> {
        if let Some(length) = self.length
            && self.cursor + 1 > length
        {
            return ControlFlow::Break(Err(Error::InvalidPacket));
        }

        if self.data.len() > self.cursor {
            let r = self.data[self.cursor];
            self.cursor += 1;
            ControlFlow::Continue(r)
        } else {
            self.data
                .reserve((self.data.capacity() - self.data.len()).min(1));
            ControlFlow::Break(Ok(None))
        }
    }

    fn validate_type<P>(&mut self) -> ControlFlow<Result<Option<P>, Error>, u8> {
        self.validate_u8()
    }

    fn validate_length<P>(&mut self) -> ControlFlow<Result<Option<P>, Error>, usize> {
        let length = self.validate_u32()? as usize;
        self.length = Some(length);

        ControlFlow::Continue(length)
    }

    fn validate_str<P>(&mut self) -> ControlFlow<Result<Option<P>, Error>, &str> {
        let len = self.validate_u32()? as usize;

        if let Some(length) = self.length
            && self.cursor + len > length
        {
            return ControlFlow::Break(Err(Error::InvalidPacket));
        }

        if self.data.len() > self.cursor + len {
            let r = std::str::from_utf8(&self.data[self.cursor..self.cursor + len])
                .expect("invalid utf8");
            self.cursor += len;
            ControlFlow::Continue(r)
        } else {
            self.data
                .reserve((self.data.capacity() - self.data.len()).min(len));
            ControlFlow::Break(Ok(None))
        }
    }

    fn validate_u32<P>(&mut self) -> ControlFlow<Result<Option<P>, Error>, u32> {
        if let Some(length) = self.length
            && self.cursor + 4 > length
        {
            return ControlFlow::Break(Err(Error::InvalidPacket));
        }

        if self.data.len() > self.cursor + 4 {
            let r = u32::from_be_bytes([
                self.data[self.cursor],
                self.data[self.cursor + 1],
                self.data[self.cursor + 2],
                self.data[self.cursor + 3],
            ]);
            self.cursor += 4;
            ControlFlow::Continue(r)
        } else {
            self.data
                .reserve((self.data.capacity() - self.data.len()).min(4));
            ControlFlow::Break(Ok(None))
        }
    }

    fn validate_checksum<P>(&mut self) -> ControlFlow<Result<Option<P>, Error>, u8> {
        let checksum = self.validate_u8()?;
        let Some(length) = self.length else {
            return ControlFlow::Break(Err(Error::InvalidPacket));
        };
        if self
            .data
            .iter()
            .take(length)
            .fold(0_u8, |a, b| a.wrapping_add(*b))
            != 0
        {
            return ControlFlow::Break(Err(Error::InvalidChecksum));
        }
        ControlFlow::Continue(checksum)
    }

    fn raw_packet<D: RawPacketDecoder>(&mut self) -> Result<RawPacket<D>, Error> {
        let Some(length) = self.length else {
            return Err(Error::InvalidPacket);
        };

        if length != self.cursor {
            return Err(Error::InvalidPacket);
        }

        let bytes = self.data.split_to(length).freeze();

        Ok(RawPacket::new(bytes))
    }
}

struct Writer(Vec<u8>);

impl Writer {
    fn new(t: u8) -> Self {
        Self(vec![t, 0, 0, 0, 0])
    }

    #[allow(clippy::cast_possible_truncation)]
    fn write_str(&mut self, value: &str) {
        self.write_u32(value.len() as u32);
        self.0.extend_from_slice(value.as_bytes());
    }

    fn write_u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }

    fn write_u8(&mut self, value: u8) {
        self.0.push(value);
    }

    #[allow(clippy::cast_possible_truncation)]
    fn finalize(mut self) -> Vec<u8> {
        let len = self.0.len() + 1;
        if let Some(value) = self.0.get_mut(1..5).as_mut() {
            for (a, b) in value.iter_mut().zip((len as u32).to_be_bytes()) {
                *a = b;
            }
        }

        let checksum = 0_u8.wrapping_sub(self.0.iter().fold(0, |acc, a| acc.wrapping_add(*a)));

        self.0.push(checksum);

        self.0
    }
}
