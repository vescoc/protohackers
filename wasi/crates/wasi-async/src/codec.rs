use std::future::Future;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};

use futures::{ready, Sink, Stream};

use tracing::{error, instrument, trace, warn};

use wasi::io::streams::StreamError;

use bytes::BytesMut;

use crate::io::{AsyncRead, AsyncWrite};

const INITIAL_CAPACITY: usize = 1024 * 8;

pub trait Decoder {
    type Item;
    type Error: From<StreamError>;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error>;

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

    fn encode(&mut self, item: Item, dst: &mut BytesMut) -> Result<(), Self::Error>;
}

pub struct FramedRead<R, D> {
    read: R,
    decoder: D,
    buffer: BytesMut,
    eof: bool,
}

impl<R, D> FramedRead<R, D> {
    pub fn new(read: R, decoder: D) -> Self {
        Self {
            read,
            decoder,
            buffer: BytesMut::new(),
            eof: false,
        }
    }
}

impl<R: AsyncRead + Unpin, D: Decoder + Unpin> FramedRead<R, D>
where
    Self: Stream<Item = Result<D::Item, D::Error>>,
{
    #[instrument(skip_all)]
    fn handle_eof(&mut self) -> Poll<Option<<Self as Stream>::Item>> {
        if self.buffer.is_empty() {
            trace!("buffer is empty");
            return Poll::Ready(None);
        }

        match self.decoder.decode_eof(&mut self.buffer) {
            Ok(None) => {
                error!("decoder eof returned Ok(None)");
                Poll::Ready(Some(Err(StreamError::Closed.into())))
            }
            Ok(Some(v)) => {
                trace!("some data");
                Poll::Ready(Some(Ok(v)))
            }
            Err(e) => {
                trace!("error");
                Poll::Ready(Some(Err(e)))
            }
        }
    }
}

impl<R: AsyncRead + Unpin, D: Decoder + Unpin> Stream for FramedRead<R, D> {
    type Item = Result<D::Item, D::Error>;

    #[instrument(skip_all)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if this.eof {
                return this.handle_eof();
            }

            trace!("decode {}", this.buffer.len());
            match this.decoder.decode(&mut this.buffer) {
                Ok(Some(value)) => return Poll::Ready(Some(Ok(value))),
                Err(e) => return Poll::Ready(Some(Err(e))),
                Ok(None) => {}
            }

            let read = &mut this.read;
            let len = this.buffer.capacity().max(1) as u64;
            let (data, eof) = {
                trace!("read {len}/{}", this.buffer.len());
                let f = pin!(read.read(len));
                match f.poll(cx) {
                    Poll::Ready(Ok(data)) => (Some(data), false),
                    Poll::Ready(Err(StreamError::Closed)) => (None, true),
                    Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                    Poll::Pending => return Poll::Pending,
                }
            };

            if eof {
                this.eof = true;
                continue;
            }

            let data = data.unwrap();

            trace!("extend slice {}", data.len());
            this.buffer.extend_from_slice(&data);
        }
    }
}

pub struct FramedWrite<W, E> {
    write: W,
    encoder: E,
    buffer: BytesMut,
    backpressure_boundary: usize,
}

impl<W, E> FramedWrite<W, E> {
    pub fn new(write: W, encoder: E) -> Self {
        Self {
            write,
            encoder,
            buffer: BytesMut::with_capacity(INITIAL_CAPACITY),
            backpressure_boundary: INITIAL_CAPACITY,
        }
    }
}

impl<W: AsyncWrite + Unpin, E: Encoder<Item> + Unpin, Item> Sink<Item> for FramedWrite<W, E> {
    type Error = E::Error;

    #[instrument(skip_all)]
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        if self.buffer.len() >= self.backpressure_boundary {
            self.as_mut().poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    #[instrument(skip_all)]
    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        let this = self.get_mut();
        trace!("send {}", this.buffer.len());
        this.encoder.encode(item, &mut this.buffer)
    }

    #[instrument(skip_all)]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        trace!("buffer len: {}", this.buffer.len());

        while !this.buffer.is_empty() {
            let n = {
                let write = pin!(this.write.write(&this.buffer));
                match write.poll(cx) {
                    Poll::Ready(Ok(n)) => n,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err.into())),
                    Poll::Pending => return Poll::Pending,
                }
            };

            trace!("sent {n} bytes");

            let _ = this.buffer.split_to(n as usize);
        }

        let flush = pin!(this.write.flush());
        match flush.poll(cx) { Poll::Ready(Err(err)) => {
            Poll::Ready(Err(err.into()))
        } _ => {
            Poll::Ready(Ok(()))
        }}
    }

    #[instrument(skip_all)]
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        trace!("close");

        ready!(self.as_mut().poll_flush(cx))?;

        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub struct LinesDecoder {
    from_index: usize,
}

impl LinesDecoder {
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
