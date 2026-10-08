use std::ops::ControlFlow;

use bytes::BytesMut;

use crate::codec::{Error, Parser, RawPacketDecoder, Validator, Writer, packets};

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum PolicyAction {
    Conserve,
    Cull,
}

impl From<PolicyAction> for u8 {
    fn from(action: PolicyAction) -> Self {
        match action {
            PolicyAction::Conserve => 0xa0,
            PolicyAction::Cull => 0x90,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Packet {
    pub species: String,
    pub action: PolicyAction,
}

impl Packet {
    pub(crate) fn write_packet(&self) -> Vec<u8> {
        let mut writer = Writer::new(0x55);

        writer.write_str(&self.species);
        writer.write_u8(self.action.into());

        writer.finalize()
    }

    #[must_use]
    pub fn new(species: impl Into<String>, action: PolicyAction) -> Self {
        Self {
            species: species.into(),
            action,
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
        let species = parser.read_str().to_owned();
        let action = if parser.read_u8() == 0x90 {
            PolicyAction::Cull
        } else {
            PolicyAction::Conserve
        };

        Packet::new(species, action)
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

    // species
    if let ControlFlow::Break(b) = validator.validate_str() {
        return b;
    }

    // action
    match validator.validate_u8() {
        ControlFlow::Break(b) => return b,
        ControlFlow::Continue(0x90 | 0xa0) => {}
        ControlFlow::Continue(_) => return Err(Error::InvalidPacket),
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
                0x55, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0xa0, 0xc0,
            ]
            .as_slice();
            let mut reader = Box::pin(FramedRead::new(data, PacketCodec::new()).into_stream());

            let packets::Packet::CreatePolicy(raw_packet) =
                reader.try_next().await.unwrap().unwrap()
            else {
                panic!("invalid packet");
            };

            assert_eq!(Packet::new("dog", PolicyAction::Conserve), raw_packet);
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

                writer
                    .send(Packet::new("dog", PolicyAction::Conserve).into())
                    .await
                    .unwrap();
            }

            let data = [
                0x55, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0xa0, 0xc0,
            ]
            .as_slice();

            assert_eq!(data, buffer);
        });
    }
}
