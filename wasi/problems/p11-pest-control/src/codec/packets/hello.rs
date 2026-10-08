use std::ops::ControlFlow;

use bytes::BytesMut;

use crate::codec::{Error, Parser, RawPacketDecoder, Validator, Writer, packets};

pub const PESTCONTROL_PROTOCOL: &str = "pestcontrol";
pub const PESTCONTROL_VERSION: u32 = 1;

#[derive(Debug, PartialEq)]
pub struct Packet {
    pub protocol: String,
    pub version: u32,
}

impl Packet {
    pub(crate) fn write_packet(&self) -> Vec<u8> {
        let mut writer = Writer::new(0x50);

        writer.write_str(&self.protocol);
        writer.write_u32(self.version);

        writer.finalize()
    }

    #[must_use]
    pub fn new() -> Self {
        Self {
            protocol: PESTCONTROL_PROTOCOL.to_string(),
            version: PESTCONTROL_VERSION,
        }
    }
}

impl Default for Packet {
    fn default() -> Self {
        Self::new()
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
        let protocol = parser.read_str().to_owned();
        let version = parser.read_u32();

        Packet { protocol, version }
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

    if let ControlFlow::Break(b) = validator.validate_u32() {
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

    use wasi_async::codec::{FramedRead, FramedWrite};
    use wasi_async_runtime::Reactor;

    use crate::codec::packets::PacketCodec;
    use crate::tests::init_tracing_subscriber;

    use super::*;

    #[test]
    fn test_read() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let data = [
                0x50, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x0b, 0x70, 0x65, 0x73, 0x74, 0x63,
                0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x01, 0xce,
            ]
            .as_slice();
            let mut reader = Box::pin(FramedRead::new(data, PacketCodec::new()).into_stream());

            let packets::Packet::Hello(raw_packet) = reader.try_next().await.unwrap().unwrap()
            else {
                panic!("invalid packet");
            };

            assert_eq!(Packet::new(), raw_packet);
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

                writer.send(Packet::new().into()).await.unwrap();
            }

            let data = [
                0x50, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x0b, 0x70, 0x65, 0x73, 0x74, 0x63,
                0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x01, 0xce,
            ]
            .as_slice();

            assert_eq!(data, buffer);
        });
    }
}
