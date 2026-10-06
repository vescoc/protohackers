use wasi::io::streams::StreamError;

use wasi_async::io::{AsyncRead, AsyncWrite};

use tracing::instrument;

use super::{Data, Entry, Error, Path, Vcs, data_is_valid};

pub type RevisionId = usize;

pub struct Revision(pub(crate) RevisionId);

impl From<RevisionId> for Revision {
    fn from(value: RevisionId) -> Self {
        Self(value)
    }
}

#[derive(Debug)]
pub struct ReadRevision {
    pub(crate) data: Data,
    pub(crate) index: u64,
}

impl ReadRevision {
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl AsyncRead for ReadRevision {
    #[instrument(skip(self))]
    #[allow(clippy::cast_possible_truncation)]
    async fn read(&mut self, len: u64) -> Result<Vec<u8>, StreamError> {
        let len = len.min(self.data.len() as u64 - self.index);
        let result = self.data[self.index as usize..self.index as usize + len as usize].to_vec();
        self.index += len;
        Ok(result)
    }
}

pub struct WriteRevision<'a> {
    pub(crate) vcs: &'a Vcs,
    pub(crate) path: &'a Path,
    pub(crate) data: Data,
}

impl WriteRevision<'_> {
    /// # Errors
    #[instrument(skip(self))]
    pub(crate) async fn commit(self) -> Result<RevisionId, Error> {
        if !data_is_valid(&self.data) {
            return Err(Error::InvalidData);
        }

        let mut root_entries = self.vcs.0.write().await;

        let file = Entry::file_mut(&mut root_entries, self.path);

        file.insert_revision(self.data)
    }
}

impl AsyncWrite for WriteRevision<'_> {
    fn write(&mut self, data: &[u8]) -> impl Future<Output = Result<u64, StreamError>> {
        self.data.extend_from_slice(data);

        std::future::ready(Ok(data.len() as u64))
    }

    fn flush(&mut self) -> impl Future<Output = Result<(), StreamError>> {
        std::future::ready(Ok(()))
    }

    fn close(&mut self) -> impl Future<Output = Result<(), StreamError>> {
        std::future::ready(Ok(()))
    }
}
