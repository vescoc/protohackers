use std::mem;

use bytes::{Buf, BufMut, BytesMut};

use wasi::io::streams::StreamError;

use wasi_async::codec::{Decoder, Encoder};

pub const ERROR_TAG: u8 = 0x10;
pub const PLATE_TAG: u8 = 0x20;
pub const TICKET_TAG: u8 = 0x21;
pub const WANT_HEARTBEAT_TAG: u8 = 0x40;
pub const HEARTBEAT_TAG: u8 = 0x41;
pub const I_AM_CAMERA_TAG: u8 = 0x80;
pub const I_AM_DISPATCHER_TAG: u8 = 0x81;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("stream error")]
    StreamError(#[from] StreamError),

    #[error("invalid message: 0x{0:2x}")]
    InvalidMessage(u8),
}

#[derive(Debug, PartialEq)]
pub enum Packet {
    Error {
        msg: String,
    },

    Plate {
        plate: String,
        timestamp: u32,
    },

    Ticket {
        plate: String,
        road: u16,
        mile1: u16,
        timestamp1: u32,
        mile2: u16,
        timestamp2: u32,
        speed: u16,
    },

    WantHeartbeat {
        interval: u32,
    },

    Heartbeat,

    IAmCamera {
        road: u16,
        mile: u16,
        limit: u16,
    },

    IAmDispatcher {
        roads: Vec<u16>,
    },
}

impl Packet {
    #[must_use]
    pub fn tag(&self) -> u8 {
        match self {
            Packet::Error { .. } => ERROR_TAG,
            Packet::Plate { .. } => PLATE_TAG,
            Packet::Ticket { .. } => TICKET_TAG,
            Packet::WantHeartbeat { .. } => WANT_HEARTBEAT_TAG,
            Packet::Heartbeat => HEARTBEAT_TAG,
            Packet::IAmCamera { .. } => I_AM_CAMERA_TAG,
            Packet::IAmDispatcher { .. } => I_AM_DISPATCHER_TAG,
        }
    }
}

trait GetDataType: Buf {
    fn get_str_d(&mut self) -> Option<String> {
        if self.remaining() < mem::size_of::<u8>() {
            return None;
        }

        let len = self.get_u8() as usize;
        if self.remaining() < len {
            return None;
        }

        let mut buffer = [0; 256];
        self.copy_to_slice(&mut buffer[..len]);

        Some(String::from_utf8_lossy(&buffer[..len]).into_owned())
    }

    fn get_u8_d(&mut self) -> Option<u8> {
        if self.remaining() < mem::size_of::<u8>() {
            return None;
        }

        Some(self.get_u8())
    }

    fn get_u16_d(&mut self) -> Option<u16> {
        if self.remaining() < mem::size_of::<u16>() {
            return None;
        }

        Some(self.get_u16())
    }

    fn get_u32_d(&mut self) -> Option<u32> {
        if self.remaining() < mem::size_of::<u32>() {
            return None;
        }

        Some(self.get_u32())
    }
}

impl<T: Buf> GetDataType for T {}

trait PutDataType: BufMut {
    #[allow(clippy::cast_possible_truncation)]
    fn put_str_d(&mut self, msg: &str) {
        let bytes = msg.as_bytes();

        assert!(bytes.len() < 256);

        self.put_u8(bytes.len() as u8);
        self.put_slice(bytes);
    }

    fn put_u8_d(&mut self, value: u8) {
        self.put_u8(value);
    }

    fn put_u16_d(&mut self, value: u16) {
        self.put_u16(value);
    }

    fn put_u32_d(&mut self, value: u32) {
        self.put_u32(value);
    }
}

impl<T: BufMut> PutDataType for T {}

macro_rules! some {
    ($e:expr_2021) => {{
        let Some(value) = $e else {
            return Ok(None);
        };
        value
    }};
}

pub struct PacketCodec;

impl Decoder for PacketCodec {
    type Item = Packet;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut data = BytesMut::from(src.chunk());
        let r = match some!(data.get_u8_d()) {
            ERROR_TAG => Ok(Some(Packet::Error {
                msg: some!(data.get_str_d()),
            })),
            PLATE_TAG => Ok(Some(Packet::Plate {
                plate: some!(data.get_str_d()),
                timestamp: some!(data.get_u32_d()),
            })),
            TICKET_TAG => Ok(Some(Packet::Ticket {
                plate: some!(data.get_str_d()),
                road: some!(data.get_u16_d()),
                mile1: some!(data.get_u16_d()),
                timestamp1: some!(data.get_u32_d()),
                mile2: some!(data.get_u16_d()),
                timestamp2: some!(data.get_u32_d()),
                speed: some!(data.get_u16_d()),
            })),
            WANT_HEARTBEAT_TAG => Ok(Some(Packet::WantHeartbeat {
                interval: some!(data.get_u32_d()),
            })),
            HEARTBEAT_TAG => Ok(Some(Packet::Heartbeat)),
            I_AM_CAMERA_TAG => Ok(Some(Packet::IAmCamera {
                road: some!(data.get_u16_d()),
                mile: some!(data.get_u16_d()),
                limit: some!(data.get_u16_d()),
            })),
            I_AM_DISPATCHER_TAG => {
                let len = some!(data.get_u8_d()) as usize;
                let mut roads = Vec::with_capacity(len);
                for _ in 0..len {
                    roads.push(some!(data.get_u16_d()));
                }
                Ok(Some(Packet::IAmDispatcher { roads }))
            }
            c => Err(Error::InvalidMessage(c)),
        };

        if let Ok(Some(_)) = r {
            src.advance(src.len() - data.len());
            r
        } else {
            src.reserve(1);
            r
        }
    }
}

impl Encoder<Packet> for PacketCodec {
    type Error = Error;

    #[allow(clippy::cast_possible_truncation)]
    fn encode(&mut self, packet: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match packet {
            Packet::Error { msg } => {
                dst.put_u8_d(ERROR_TAG);
                dst.put_str_d(&msg);
            }
            Packet::Plate { plate, timestamp } => {
                dst.put_u8_d(PLATE_TAG);
                dst.put_str_d(&plate);
                dst.put_u32_d(timestamp);
            }
            Packet::Ticket {
                plate,
                road,
                mile1,
                timestamp1,
                mile2,
                timestamp2,
                speed,
            } => {
                dst.put_u8_d(TICKET_TAG);
                dst.put_str_d(&plate);
                dst.put_u16_d(road);
                dst.put_u16_d(mile1);
                dst.put_u32_d(timestamp1);
                dst.put_u16_d(mile2);
                dst.put_u32_d(timestamp2);
                dst.put_u16_d(speed);
            }
            Packet::WantHeartbeat { interval } => {
                dst.put_u8_d(WANT_HEARTBEAT_TAG);
                dst.put_u32_d(interval);
            }
            Packet::Heartbeat => {
                dst.put_u8_d(HEARTBEAT_TAG);
            }
            Packet::IAmCamera { road, mile, limit } => {
                dst.put_u8_d(I_AM_CAMERA_TAG);
                dst.put_u16_d(road);
                dst.put_u16_d(mile);
                dst.put_u16_d(limit);
            }
            Packet::IAmDispatcher { roads } => {
                dst.put_u8_d(I_AM_DISPATCHER_TAG);
                dst.put_u8_d(roads.len() as u8);
                for value in roads {
                    dst.put_u16_d(value);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use wasi_async::codec::{FramedRead, FramedWrite};
    use wasi_async_runtime::block_on;

    use futures::{SinkExt, StreamExt};

    use crate::tests::init_tracing_subscriber;

    use super::*;

    #[test]
    #[allow(non_snake_case)]
    fn test_read_Plate() {
        block_on(|_| async move {
            let buffer = [0x20, 0x04, 0x55, 0x4e, 0x31, 0x58, 0x00, 0x00, 0x03, 0xe8];
            let mut stream = std::pin::pin!(FramedRead::new(buffer.as_slice(), PacketCodec).into_stream());

            assert_eq!(
                Packet::Plate {
                    plate: "UN1X".to_string(),
                    timestamp: 1000
                },
                stream.next().await.unwrap().unwrap(),
            );
        });
    }

    #[test]
    #[allow(non_snake_case)]
    fn test_write_Plate() {
        block_on(|_| async move {
            let mut buffer = vec![];
            {
                let mut write = std::pin::pin!(FramedWrite::new(&mut buffer, PacketCodec).into_sink());

                write
                    .send(Packet::Plate {
                        plate: "UN1X".to_string(),
                        timestamp: 1000,
                    })
                    .await
                    .unwrap();
            }
            
            assert_eq!(
                &buffer,
                [0x20, 0x04, 0x55, 0x4e, 0x31, 0x58, 0x00, 0x00, 0x03, 0xe8].as_slice(),
            );
        });
    }

    #[test]
    #[allow(non_snake_case)]
    fn test_read_IAmCamera() {
        block_on(|_| async move {
            let buffer = [0x80, 0x00, 0x42, 0x00, 0x64, 0x00, 0x3c];
            let mut stream = std::pin::pin!(FramedRead::new(buffer.as_slice(), PacketCodec).into_stream());

            assert_eq!(
                Packet::IAmCamera {
                    road: 66,
                    mile: 100,
                    limit: 60
                },
                stream.next().await.unwrap().unwrap(),
            );
        });
    }

    #[test]
    #[allow(non_snake_case)]
    fn test_write_IAmCamera() {
        block_on(|_| async move {
            let mut buffer = vec![];
            {
                let mut write = std::pin::pin!(FramedWrite::new(&mut buffer, PacketCodec).into_sink());

                write
                    .send(Packet::IAmCamera {
                        road: 66,
                        mile: 100,
                        limit: 60,
                    })
                    .await
                    .unwrap();
            }
            
            assert_eq!(
                &buffer,
                [0x80, 0x00, 0x42, 0x00, 0x64, 0x00, 0x3c].as_slice()
            );
        });
    }

    #[test]
    #[allow(non_snake_case)]
    fn test_read_IAmDispatcher() {
        init_tracing_subscriber();

        block_on(|_| async move {
            let buffer = [0x81, 0x01, 0x00, 0x42];
            let mut stream = std::pin::pin!(FramedRead::new(buffer.as_slice(), PacketCodec).into_stream());

            assert_eq!(
                Packet::IAmDispatcher { roads: vec![66] },
                stream.next().await.unwrap().unwrap(),
            );
        });
    }

    #[test]
    #[allow(non_snake_case)]
    fn test_write_IAmDispatcher() {
        init_tracing_subscriber();

        block_on(|_| async move {
            let mut buffer = vec![];
            {
                let mut write = std::pin::pin!(FramedWrite::new(&mut buffer, PacketCodec).into_sink());

                write
                    .send(Packet::IAmDispatcher { roads: vec![66] })
                    .await
                    .unwrap();
            }
            
            assert_eq!(&buffer, [0x81, 0x01, 0x00, 0x42].as_slice());
        });
    }

    #[test]
    #[allow(non_snake_case)]
    fn test_read_WantHeartbeat() {
        block_on(|_| async move {
            let buffer = [0x40, 0x00, 0x00, 0x00, 0x0a];
            let mut stream = std::pin::pin!(FramedRead::new(buffer.as_slice(), PacketCodec).into_stream());

            assert_eq!(
                Packet::WantHeartbeat { interval: 10 },
                stream.next().await.unwrap().unwrap(),
            );
        });
    }

    #[test]
    #[allow(non_snake_case)]
    fn test_write_WantHeartbeat() {
        block_on(|_| async move {
            let mut buffer = vec![];
            {
                let mut write = std::pin::pin!(FramedWrite::new(&mut buffer, PacketCodec).into_sink());

                write
                    .send(Packet::WantHeartbeat { interval: 10 })
                    .await
                    .unwrap();
            }
            
            assert_eq!(&buffer, [0x40, 0x00, 0x00, 0x00, 0x0a].as_slice());
        });
    }

    #[test]
    #[allow(non_snake_case)]
    fn test_write_Error() {
        block_on(|_| async move {
            let mut buffer = vec![];
            {
                let mut write = std::pin::pin!(FramedWrite::new(&mut buffer, PacketCodec).into_sink());

                write
                    .send(Packet::Error {
                        msg: "bad".to_string(),
                    })
                    .await
                    .unwrap();
            }
            
            assert_eq!(buffer, vec![0x10, 0x03, 0x62, 0x61, 0x64]);
        });
    }

    #[test]
    #[allow(non_snake_case)]
    fn test_read_Error() {
        init_tracing_subscriber();

        block_on(|_| async move {
            let buffer = [0x10, 0x03, 0x62, 0x61, 0x64];
            let mut stream = std::pin::pin!(FramedRead::new(buffer.as_slice(), PacketCodec).into_stream());

            assert_eq!(
                Packet::Error {
                    msg: "bad".to_string(),
                },
                stream.next().await.unwrap().unwrap(),
            );
        });
    }

    #[test]
    #[allow(non_snake_case, clippy::unreadable_literal)]
    fn test_write_Ticket() {
        block_on(|_| async move {
            let mut buffer = vec![];
            {
                let mut write = std::pin::pin!(FramedWrite::new(&mut buffer, PacketCodec).into_sink());

                write
                    .send(Packet::Ticket {
                        plate: "UN1X".to_string(),
                        road: 66,
                        mile1: 100,
                        timestamp1: 123456,
                        mile2: 110,
                        timestamp2: 123816,
                        speed: 10000,
                    })
                    .await
                    .unwrap();
            }
            
            assert_eq!(
                buffer,
                vec![
                    0x21, 0x04, 0x55, 0x4e, 0x31, 0x58, 0x00, 0x42, 0x00, 0x64, 0x00, 0x01, 0xe2,
                    0x40, 0x00, 0x6e, 0x00, 0x01, 0xe3, 0xa8, 0x27, 0x10
                ]
            );
        });
    }

    #[test]
    #[allow(non_snake_case, clippy::unreadable_literal)]
    fn test_read_Ticket() {
        block_on(|_| async move {
            let buffer = [
                0x21, 0x04, 0x55, 0x4e, 0x31, 0x58, 0x00, 0x42, 0x00, 0x64, 0x00, 0x01, 0xe2, 0x40,
                0x00, 0x6e, 0x00, 0x01, 0xe3, 0xa8, 0x27, 0x10,
            ];
            let mut stream = std::pin::pin!(FramedRead::new(buffer.as_slice(), PacketCodec).into_stream());

            assert_eq!(
                Packet::Ticket {
                    plate: "UN1X".to_string(),
                    road: 66,
                    mile1: 100,
                    timestamp1: 123456,
                    mile2: 110,
                    timestamp2: 123816,
                    speed: 10000,
                },
                stream.next().await.unwrap().unwrap(),
            );
        });
    }

    #[test]
    #[allow(non_snake_case)]
    fn test_write_Heartbeat() {
        block_on(|_| async move {
            let mut buffer = vec![];
            {
                let mut write = std::pin::pin!(FramedWrite::new(&mut buffer, PacketCodec).into_sink());

                write.send(Packet::Heartbeat).await.unwrap();
            }
            
            assert_eq!(buffer, vec![0x41]);
        });
    }

    #[test]
    #[allow(non_snake_case)]
    fn test_read_Heartbeat() {
        block_on(|_| async move {
            let buffer = [0x41];
            let mut stream = std::pin::pin!(FramedRead::new(buffer.as_slice(), PacketCodec).into_stream());

            assert_eq!(Packet::Heartbeat, stream.next().await.unwrap().unwrap());
        });
    }
}
