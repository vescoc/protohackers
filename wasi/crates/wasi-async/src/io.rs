use std::future::{self, Future};

use wasi::io::streams::StreamError;

pub trait AsyncRead {
    fn read(&mut self, len: u64) -> impl Future<Output = Result<Vec<u8>, StreamError>>;
}

pub trait AsyncWrite {
    fn write(&mut self, data: &[u8]) -> impl Future<Output = Result<u64, StreamError>>;

    fn flush(&mut self) -> impl Future<Output = Result<(), StreamError>>;

    fn close(&mut self) -> impl Future<Output = Result<(), StreamError>>;
}

pub trait AsyncWriteExt: AsyncWrite {
    #[expect(clippy::cast_possible_truncation, reason = "can happen")]
    fn write_all(&mut self, mut data: &[u8]) -> impl Future<Output = Result<(), StreamError>> {
        async move {
            while !data.is_empty() {
                let len = self.write(data).await?;
                assert!(len > 0, "write_all len zero");

                if len as usize == data.len() {
                    break;
                }

                data = &data[len as usize..];
            }

            Ok(())
        }
    }
}

impl<T: AsyncWrite> AsyncWriteExt for T {}

impl AsyncRead for &[u8] {
    #[expect(clippy::cast_possible_truncation, reason = "can happen")]
    fn read(&mut self, len: u64) -> impl Future<Output = Result<Vec<u8>, StreamError>> {
        let len = len as usize;
        let len = len.min(self.len());
        let r = self[0..len].to_vec();
        *self = &self[len..];
        future::ready(Ok(r))
    }
}

impl AsyncWrite for &mut Vec<u8> {
    #[expect(clippy::unused_async_trait_impl)]
    async fn write(&mut self, data: &[u8]) -> Result<u64, StreamError> {
        self.extend_from_slice(data);
        Ok(data.len() as u64)
    }

    #[expect(clippy::unused_async_trait_impl)]
    async fn flush(&mut self) -> Result<(), StreamError> {
        Ok(())
    }

    #[expect(clippy::unused_async_trait_impl)]
    async fn close(&mut self) -> Result<(), StreamError> {
        Ok(())
    }
}
