use std::ops::ControlFlow;

use bytes::BytesMut;

use crate::codec::{Error, Parser, RawPacketDecoder, Validator, Writer, packets};

#[derive(Debug, PartialEq)]
pub struct Packet {
    pub message: String,
}

impl Packet {
    pub(crate) fn write_packet(&self) -> Vec<u8> {
        let mut writer = Writer::new(0x51);

        writer.write_str(&self.message);

        writer.finalize()
    }

    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct PacketDecoder;

impl RawPacketDecoder for PacketDecoder {
    type Decoded<'a> = Packet;

    fn decode(data: &[u8]) -> Self::Decoded<'_> {
        let mut parser = Parser::new(data);

        parser.read_u8();
        parser.read_u32();
        let message = parser.read_str();

        Packet::new(message)
    }
}

pub(crate) fn read_packet(src: &mut BytesMut) -> Result<Option<packets::Packet>, Error> {
    let mut validator = Validator::new(src);

    if let ControlFlow::Break(b) = validator.validate_type() {
        return b;
    }

    if let ControlFlow::Break(b) = validator.validate_length() {
        return b;
    }

    if let ControlFlow::Break(b) = validator.validate_str() {
        return b;
    }

    if let ControlFlow::Break(b) = validator.validate_checksum() {
        return b;
    }

    let raw_packet = validator.raw_packet::<PacketDecoder>()?;

    Ok(Some(raw_packet.decode().into()))
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt, TryStreamExt};

    use crate::codec::packets::PacketCodec;
    use crate::tests::init_tracing_subscriber;

    use wasi_async::codec::{FramedRead, FramedWrite};
    use wasi_async_runtime::Reactor;

    use super::*;

    #[test]
    fn test_read() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let data = [
                0x51, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x00, 0x00, 0x03, 0x62, 0x61, 0x64, 0x78,
            ]
            .as_slice();
            let mut reader = Box::pin(FramedRead::new(data, PacketCodec::new()).into_stream());

            let packets::Packet::Error(raw_packet) = reader.try_next().await.unwrap().unwrap()
            else {
                panic!("invalid packet");
            };

            assert_eq!(Packet::new("bad"), raw_packet);
        });
    }

    #[test]
    fn test_write() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let mut buffer = vec![];
            {
                let mut writer =
                    Box::pin(FramedWrite::new(&mut buffer, PacketCodec::new()).into_sink());

                writer.send(Packet::new("bad").into()).await.unwrap();
            }

            let data = [
                0x51, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x00, 0x00, 0x03, 0x62, 0x61, 0x64, 0x78,
            ]
            .as_slice();

            assert_eq!(data, buffer);
        });
    }
}
