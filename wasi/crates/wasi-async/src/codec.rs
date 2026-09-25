use std::marker;

use futures::{Sink, Stream, sink, stream};

use tracing::{error, instrument, trace, warn};

use wasi::io::streams::StreamError;

use bytes::BytesMut;

use crate::io::{AsyncRead, AsyncWrite};

const INITIAL_CAPACITY: usize = 1024 * 8;

pub trait Decoder {
    type Item;
    type Error: From<StreamError>;

    /// # Errors
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error>;

    /// # Errors
    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.decode(src) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Err(StreamError::Closed.into()),
            Err(e) => Err(e),
        }
    }
}

pub trait Encoder<Item> {
    type Error: From<StreamError>;

    /// # Errors
    fn encode(&mut self, item: Item, dst: &mut BytesMut) -> Result<(), Self::Error>;
}

pub struct FramedRead<R, D> {
    read: R,
    decoder: D,
}

impl<R, D> FramedRead<R, D> {
    pub fn new(read: R, decoder: D) -> Self {
        Self {
            read,
            decoder,
        }
    }
}

impl<R: AsyncRead + Unpin, D: Decoder + Unpin> FramedRead<R, D> {
    /// # Errors
    /// # Panics
    #[instrument(skip_all)]
    pub fn into_stream(self) -> impl Stream<Item = Result<D::Item, D::Error>> {
        stream::unfold(
            (self.read, self.decoder, BytesMut::new(), false),
            |(mut reader, mut decoder, mut buffer, mut eof)| async move {
                loop {
                    if eof {
                        let yielded = Self::handle_eof(&mut decoder, &mut buffer)?;
                        return Some((yielded, (reader, decoder, buffer, eof)));
                    }

                    trace!("decode {}", buffer.len());
                    match decoder.decode(&mut buffer) {
                        Ok(Some(value)) => {
                            return Some((Ok(value), (reader, decoder, buffer, eof)));
                        }
                        Err(e) => {
                            return Some((Err(e), (reader, decoder, buffer, eof)));
                        }
                        Ok(None) => {}
                    }

                    let len = buffer.capacity().max(1) as u64;
                    let (data, current_eof) = {
                        trace!("read {len}/{}", buffer.len());
                        match reader.read(len).await {
                            Ok(data) => (Some(data), false),
                            Err(StreamError::Closed) => (None, true),
                            Err(e) => return Some((Err(e.into()), (reader, decoder, buffer, eof))),
                        }
                    };

                    if current_eof {
                        eof = true;
                        continue;
                    }

                    let data = data.unwrap();

                    trace!("extend slice {}", data.len());
                    buffer.extend_from_slice(&data);
                }
            },
        )
    }

    #[instrument(skip_all)]
    fn handle_eof(decoder: &mut D, buffer: &mut BytesMut) -> Option<Result<D::Item, D::Error>> {
        if buffer.is_empty() {
            trace!("buffer is empty");
            return None;
        }

        match decoder.decode_eof(buffer) {
            Ok(None) => {
                error!("decoder eof returned Ok(None)");
                Some(Err(StreamError::Closed.into()))
            }
            Ok(Some(v)) => {
                trace!("some data");
                Some(Ok(v))
            }
            Err(e) => {
                trace!("error");
                Some(Err(e))
            }
        }
    }
}

pub struct FramedWrite<W, Item, E> {
    write: W,
    encoder: E,
    _item: marker::PhantomData<Item>,
}

impl<W, Item, E> FramedWrite<W, Item, E> {
    pub fn new(write: W, encoder: E) -> Self {
        Self {
            write,
            encoder,
            _item: marker::PhantomData,
        }
    }
}

impl<W: AsyncWrite + Unpin, E: Encoder<Item> + Unpin, Item> FramedWrite<W, Item, E> {
    /// # Errors
    /// # Panics
    #[instrument(skip_all)]
    #[expect(clippy::cast_possible_truncation, reason = "can happen")]
    pub fn into_sink(self) -> impl Sink<Item, Error = E::Error> {
        sink::unfold(
            (self.write, self.encoder, BytesMut::with_capacity(INITIAL_CAPACITY)),
            |(mut writer, mut encoder, mut buffer), item| async move {
                encoder.encode(item, &mut buffer)?;
                
                assert!(!buffer.is_empty());
                
                while !buffer.is_empty() {
                    let n = writer.write(&buffer).await?;
                    trace!("sent {n} bytes");
                    let _ = buffer.split_to(n as usize);
                }

                writer.flush().await?;

                Ok((writer, encoder, buffer))
            },
        )
    }
}

#[derive(Debug)]
pub struct LinesDecoder {
    from_index: usize,
}

impl LinesDecoder {
    #[must_use]
    pub fn new() -> Self {
        Self { from_index: 0 }
    }
}

impl Default for LinesDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for LinesDecoder {
    type Item = Vec<u8>;
    type Error = StreamError;

    #[instrument]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(index) = src.iter().skip(self.from_index).position(|v| *v == b'\n') {
            trace!("found new line at {index}");

            let line = src.split_to(self.from_index + index + 1);

            let mut v = Vec::with_capacity(line.len() - 1);
            v.extend_from_slice(&line[..line.len() - 1]);

            self.from_index = 0;

            Ok(Some(v))
        } else {
            self.from_index = src.len();
            src.reserve(1);

            trace!("searching");

            Ok(None)
        }
    }
}

#[derive(Debug)]
pub struct ChunksDecoder<const SIZE: usize>;

impl<const SIZE: usize> ChunksDecoder<SIZE> {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl<const SIZE: usize> Default for ChunksDecoder<SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const SIZE: usize> Decoder for ChunksDecoder<SIZE> {
    type Item = [u8; SIZE];
    type Error = StreamError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < SIZE {
            Ok(None)
        } else {
            let chunk = src.split_to(SIZE);
            Ok(Some(chunk[..].try_into().unwrap()))
        }
    }
}
