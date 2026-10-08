use std::ops::ControlFlow;

use bytes::BytesMut;

use crate::codec::{Error, Parser, RawPacketDecoder, Validator, Writer, packets};

#[derive(Debug, PartialEq)]
pub struct Packet {
    pub site: u32,
    pub populations: Vec<Population>,
}

impl Packet {
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn write_packet(&self) -> Vec<u8> {
        let mut writer = Writer::new(0x58);

        writer.write_u32(self.site);
        writer.write_u32(self.populations.len() as u32);
        for Population { species, count } in &self.populations {
            writer.write_str(species);
            writer.write_u32(*count);
        }

        writer.finalize()
    }

    #[must_use]
    pub fn new(site: u32, populations: Vec<Population>) -> Self {
        Self { site, populations }
    }
}

#[derive(Debug, PartialEq)]
pub struct Population {
    pub species: String,
    pub count: u32,
}

impl Population {
    pub fn new(species: impl Into<String>, count: u32) -> Self {
        Self {
            species: species.into(),
            count,
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
        let site = parser.read_u32();

        let len = parser.read_u32() as usize;
        let mut populations = Vec::with_capacity(len);
        for _ in 0..len {
            let species = parser.read_str();
            let count = parser.read_u32();

            populations.push(Population::new(species, count));
        }

        Packet::new(site, populations)
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

    // site
    if let ControlFlow::Break(b) = validator.validate_u32() {
        return b;
    }

    // array length
    let len = match validator.validate_u32() {
        ControlFlow::Break(b) => return b,
        ControlFlow::Continue(len) => len,
    };

    for _ in 0..len {
        // species
        if let ControlFlow::Break(b) = validator.validate_str() {
            return b;
        }

        // count
        if let ControlFlow::Break(b) = validator.validate_u32() {
            return b;
        }
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
                0x58, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x02, 0x00,
                0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03,
                0x72, 0x61, 0x74, 0x00, 0x00, 0x00, 0x05, 0x8c,
            ]
            .as_slice();
            let mut reader = Box::pin(FramedRead::new(data, PacketCodec::new()).into_stream());

            let packets::Packet::SiteVisit(raw_packet) = reader.try_next().await.unwrap().unwrap()
            else {
                panic!("invalid packet");
            };

            assert_eq!(
                Packet {
                    site: 12345,
                    populations: vec![
                        Population {
                            species: "dog".to_string(),
                            count: 1,
                        },
                        Population {
                            species: "rat".to_string(),
                            count: 5,
                        },
                    ],
                },
                raw_packet
            );
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
                    .send(
                        Packet {
                            site: 12345,
                            populations: vec![
                                Population {
                                    species: "dog".to_string(),
                                    count: 1,
                                },
                                Population {
                                    species: "rat".to_string(),
                                    count: 5,
                                },
                            ],
                        }
                        .into(),
                    )
                    .await
                    .unwrap();
            }

            let data = [
                0x58, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x02, 0x00,
                0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03,
                0x72, 0x61, 0x74, 0x00, 0x00, 0x00, 0x05, 0x8c,
            ]
            .as_slice();

            assert_eq!(data, buffer);
        });
    }
}
