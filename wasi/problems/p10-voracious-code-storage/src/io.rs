use bytes::BytesMut;

use wasi::io::streams::StreamError;

use wasi_async::io::{AsyncRead, AsyncWriteExt};

pub struct BufReader<R> {
    reader: R,
    buffer: BytesMut,
}

impl<R> BufReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: BytesMut::with_capacity(1024),
        }
    }
}

impl<R: AsyncRead> BufReader<R> {
    /// # Errors
    pub async fn read_line(&mut self) -> Result<String, StreamError> {
        loop {
            if let Some(position) = self.buffer.iter().position(|c| *c == b'\n') {
                return Ok(
                    String::from_utf8_lossy(&self.buffer.split_to(position + 1)).into_owned()
                );
            }

            let buffer = self.reader.read(1024).await?;
            self.buffer.extend_from_slice(&buffer);
        }
    }
}

impl<R: AsyncRead> AsyncRead for BufReader<R> {
    #[allow(clippy::cast_possible_truncation)]
    async fn read(&mut self, len: u64) -> Result<Vec<u8>, StreamError> {
        if !self.buffer.is_empty() {
            return Ok(self
                .buffer
                .split_to(self.buffer.len().min(len as usize))
                .to_vec());
        }

        self.reader.read(len).await
    }
}

pub trait WriteLine: AsyncWriteExt + Unpin {
    fn write_line(&mut self, line: &[u8]) -> impl Future<Output = Result<(), StreamError>> {
        async move {
            self.write_all(line).await?;
            self.write(b"\n").await?;
            self.flush().await
        }
    }
}

impl<T: AsyncWriteExt + Unpin> WriteLine for T {}
