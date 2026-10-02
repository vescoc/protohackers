use std::io;

pub const BUFFER_SIZE: usize = 1000
    - PACKET_DATA_PREFIX.len()
    - (2 * PACKET_FIELD_SEPARATOR.len())
    - PACKET_POSTFIX.len()
    - (2 * MAX_NUMERIC_LENGTH)
    - 50;

#[allow(unused)]
const MAX_NUMERIC_LENGTH: usize = 10;

const PACKET_POSTFIX: &[u8; 1] = b"/";
const PACKET_FIELD_SEPARATOR: &[u8; 1] = b"/";
const PACKET_CONNECT_PREFIX: &[u8; 9] = b"/connect/";
const PACKET_DATA_PREFIX: &[u8; 6] = b"/data/";
const PACKET_ACK_PREFIX: &[u8; 5] = b"/ack/";
const PACKET_CLOSE_PREFIX: &[u8; 7] = b"/close/";

pub trait SyncWrite<T>: io::Write {
    /// Write the associate value
    ///
    /// # Errors
    ///
    /// * error if there are some problems
    fn write_value(&mut self, value: &T) -> Result<usize, io::Error>;
}

#[derive(Debug, PartialEq, Copy, Clone, Hash, Eq)]
pub struct Numeric(pub(crate) u32);

impl Numeric {
    fn parse(mut buffer: &[u8]) -> Result<(Self, &[u8]), PacketError> {
        let mut result = 0;
        loop {
            match buffer.split_first() {
                Some((value, b)) if value.is_ascii_digit() => {
                    result = result * 10 + u32::from(value - b'0');
                    if result >= 2_147_483_648 {
                        return Err(PacketError::NumericOverflow);
                    }
                    buffer = b;
                }
                Some((b'/', _)) => {
                    return Ok((Numeric(result), buffer));
                }
                _ => {
                    return Err(PacketError::InvalidPacket);
                }
            }
        }
    }
}

impl<W: io::Write> SyncWrite<Numeric> for W {
    #[allow(clippy::cast_possible_truncation)]
    fn write_value(&mut self, Numeric(value): &Numeric) -> io::Result<usize> {
        let mut len = 0;
        let mut buffer = [0; 10];
        let mut value = *value;
        loop {
            let (d, r) = (value / 10, value % 10);
            value = d;
            buffer[len] = r as u8 + b'0';
            len += 1;
            if value == 0 {
                break;
            }
        }

        for i in (0..len).rev() {
            if self.write(&buffer[i..=i])? == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "overflow"));
            }
        }

        Ok(len)
    }
}

pub type Session = Numeric;
pub type Position = Numeric;
pub type Length = Numeric;

#[derive(Debug, PartialEq, Clone)]
pub struct Payload(pub(crate) String);

impl Payload {
    pub(crate) fn new(buffer: &[u8]) -> Option<Payload> {
        if buffer.is_empty() {
            None
        } else {
            let mut len = 0;
            let mut payload = String::with_capacity(BUFFER_SIZE);
            for b in buffer {
                match b {
                    b'\\' | b'/' => len += 2,
                    _ => len += 1,
                }

                if len >= BUFFER_SIZE {
                    break;
                }

                payload.push(*b as char);
            }

            Some(Payload(payload))
        }
    }

    fn parse(mut buffer: &[u8]) -> Result<(Self, &[u8]), PacketError> {
        let mut result = String::new();
        let mut skip = false;
        loop {
            match buffer.split_first() {
                Some((b'\\', b)) if !skip => {
                    skip = true;
                    buffer = b;
                }
                Some((b'/', _)) if !skip => {
                    return Ok((Payload(result), buffer));
                }
                Some((c @ (b'\\' | b'/'), b)) if skip => {
                    result.push(char::from(*c));
                    skip = false;
                    buffer = b;
                }
                Some((c, b)) if !skip => {
                    result.push(char::from(*c));
                    buffer = b;
                }
                _ => {
                    return Err(PacketError::InvalidPacket);
                }
            }
        }
    }

    #[allow(clippy::cast_possible_truncation, unused)]
    pub(crate) fn write(&self, buffer: &mut Vec<u8>, skip: u32) -> u32 {
        for b in self.0.bytes().skip(skip as usize) {
            buffer.push(b);
        }

        self.0.len() as u32 - skip
    }
}

impl<W: io::Write> SyncWrite<Payload> for W {
    #[allow(clippy::cast_possible_truncation)]
    fn write_value(&mut self, Payload(value): &Payload) -> io::Result<usize> {
        let mut len = 0;
        for b in value.bytes() {
            match b {
                b'/' | b'\\' => {
                    self.write_all(&[b'\\', b])?;
                    len += 2;
                }
                c => {
                    self.write_all(&[c])?;
                    len += 1;
                }
            }
        }

        Ok(len)
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum PacketError {
    #[error("invalid packet")]
    InvalidPacket,

    #[error("unknown packet")]
    UnknownPacket,

    #[error("numeric overflow")]
    NumericOverflow,
    // #[error("overflow")]
    // Overflow,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Packet {
    Connect {
        session: Session,
    },
    Data {
        session: Session,
        pos: Position,
        data: Payload,
    },
    Ack {
        session: Session,
        length: Length,
    },
    Close {
        session: Session,
    },
}

impl Packet {
    #[must_use]
    pub fn session(&self) -> Numeric {
        match self {
            Packet::Connect { session }
            | Packet::Close { session }
            | Packet::Data { session, .. }
            | Packet::Ack { session, .. } => *session,
        }
    }
}

impl<W: io::Write> SyncWrite<Packet> for W {
    fn write_value(&mut self, packet: &Packet) -> Result<usize, io::Error> {
        match packet {
            Packet::Connect { session } => {
                self.write_all(PACKET_CONNECT_PREFIX)?;
                let len = self.write_value(session)?;
                self.write_all(PACKET_POSTFIX)?;
                Ok(PACKET_CONNECT_PREFIX.len() + len + PACKET_POSTFIX.len())
            }
            Packet::Close { session } => {
                self.write_all(PACKET_CLOSE_PREFIX)?;
                let len = self.write_value(session)?;
                self.write_all(PACKET_POSTFIX)?;
                Ok(PACKET_CLOSE_PREFIX.len() + len + PACKET_POSTFIX.len())
            }
            Packet::Ack { session, length } => {
                self.write_all(PACKET_ACK_PREFIX)?;
                let mut len = self.write_value(session)?;
                self.write_all(PACKET_FIELD_SEPARATOR)?;
                len += self.write_value(length)?;
                self.write_all(PACKET_POSTFIX)?;

                Ok(PACKET_ACK_PREFIX.len()
                    + len
                    + PACKET_FIELD_SEPARATOR.len()
                    + PACKET_POSTFIX.len())
            }
            Packet::Data { session, pos, data } => {
                self.write_all(PACKET_DATA_PREFIX)?;
                let mut len = self.write_value(session)?;
                self.write_all(PACKET_FIELD_SEPARATOR)?;
                len += self.write_value(pos)?;
                self.write_all(PACKET_FIELD_SEPARATOR)?;
                len += self.write_value(data)?;
                self.write_all(PACKET_POSTFIX)?;

                Ok(PACKET_DATA_PREFIX.len()
                    + len
                    + PACKET_FIELD_SEPARATOR.len() * 2
                    + PACKET_POSTFIX.len())
            }
        }
    }
}

impl TryFrom<&[u8]> for Packet {
    type Error = PacketError;

    fn try_from(buffer: &[u8]) -> Result<Self, Self::Error> {
        if buffer.ends_with(b"/") && buffer.len() < 1000 {
            if buffer.starts_with(PACKET_CONNECT_PREFIX) {
                let (session, buffer) = Numeric::parse(&buffer[PACKET_CONNECT_PREFIX.len()..])?;
                if buffer != b"/" {
                    return Err(PacketError::InvalidPacket);
                }

                return Ok(Packet::Connect { session });
            } else if buffer.starts_with(PACKET_DATA_PREFIX) {
                let (session, buffer) = Numeric::parse(&buffer[PACKET_DATA_PREFIX.len()..])?;
                let (pos, buffer) = Numeric::parse(&buffer[1..])?;
                let (data, buffer) = Payload::parse(&buffer[1..])?;
                if buffer != b"/" {
                    return Err(PacketError::InvalidPacket);
                }

                return Ok(Packet::Data { session, pos, data });
            } else if buffer.starts_with(PACKET_ACK_PREFIX) {
                let (session, buffer) = Numeric::parse(&buffer[PACKET_ACK_PREFIX.len()..])?;
                let (length, buffer) = Numeric::parse(&buffer[1..])?;
                if buffer != b"/" {
                    return Err(PacketError::InvalidPacket);
                }

                return Ok(Packet::Ack { session, length });
            } else if buffer.starts_with(PACKET_CLOSE_PREFIX) {
                let (session, buffer) = Numeric::parse(&buffer[PACKET_CLOSE_PREFIX.len()..])?;
                if buffer != b"/" {
                    return Err(PacketError::InvalidPacket);
                }

                return Ok(Packet::Close { session });
            }
        }

        Err(PacketError::UnknownPacket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::unreadable_literal)]
    fn test_read_connect() {
        let buffer = b"/connect/1234567/".as_slice();

        assert_eq!(
            Packet::Connect {
                session: Numeric(1234567)
            },
            Packet::try_from(buffer).unwrap()
        );
    }

    #[test]
    #[allow(clippy::unreadable_literal)]
    fn test_write_connect() {
        let mut buffer = [0_u8; 1024];
        let mut b = buffer.as_mut_slice();

        let len = b
            .write_value(&Packet::Connect {
                session: Numeric(1234567),
            })
            .unwrap();

        assert_eq!(b"/connect/1234567/".as_slice(), &buffer[0..len]);
    }

    #[test]
    #[allow(clippy::unreadable_literal)]
    fn test_read_ack() {
        let buffer = b"/ack/1234567/1024/".as_slice();

        assert_eq!(
            Packet::Ack {
                session: Numeric(1234567),
                length: Numeric(1024)
            },
            Packet::try_from(buffer).unwrap()
        );
    }

    #[test]
    #[allow(clippy::unreadable_literal)]
    fn test_write_ack() {
        let mut buffer = vec![];

        let len = buffer
            .write_value(&Packet::Ack {
                session: Numeric(1234567),
                length: Numeric(1024),
            })
            .unwrap();

        assert_eq!(b"/ack/1234567/1024/".as_slice(), &buffer[0..len]);
    }

    #[test]
    #[allow(clippy::unreadable_literal)]
    fn test_read_data() {
        let buffer = b"/data/1234567/0/hello/".as_slice();
        assert_eq!(
            Packet::Data {
                session: Numeric(1234567),
                pos: Numeric(0),
                data: Payload("hello".to_string())
            },
            Packet::try_from(buffer).unwrap()
        );

        let buffer = br"/data/1234568/0/\//".as_slice();
        assert_eq!(
            Packet::Data {
                session: Numeric(1234568),
                pos: Numeric(0),
                data: Payload(r"/".to_string())
            },
            Packet::try_from(buffer).unwrap()
        );
    }

    #[test]
    #[allow(clippy::unreadable_literal)]
    fn test_write_data() {
        let mut buffer = vec![];

        let len = buffer
            .write_value(&Packet::Data {
                session: Numeric(1234567),
                pos: Numeric(0),
                data: Payload("hello".to_string()),
            })
            .unwrap();
        assert_eq!(b"/data/1234567/0/hello/".as_slice(), &buffer[0..len]);

        buffer.clear();
        let len = buffer
            .write_value(&Packet::Data {
                session: Numeric(1234568),
                pos: Numeric(0),
                data: Payload("/".to_string()),
            })
            .unwrap();
        assert_eq!(br"/data/1234568/0/\//".as_slice(), &buffer[0..len]);
    }

    #[test]
    #[allow(clippy::unreadable_literal)]
    fn test_read_close() {
        let buffer = b"/close/1234567/".as_slice();

        assert_eq!(
            Packet::Close {
                session: Numeric(1234567)
            },
            Packet::try_from(buffer).unwrap()
        );
    }

    #[test]
    #[allow(clippy::unreadable_literal)]
    fn test_write_close() {
        let mut buffer = vec![];

        let len = buffer
            .write_value(&Packet::Close {
                session: Numeric(1234567),
            })
            .unwrap();

        assert_eq!(b"/close/1234567/".as_slice(), &buffer[0..len]);
    }

    #[test]
    fn test_read_invalid_connect() {
        let buffer = b"/connect/".as_slice();

        assert_eq!(Packet::try_from(buffer), Err(PacketError::InvalidPacket));
    }
}
