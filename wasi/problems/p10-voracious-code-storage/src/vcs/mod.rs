use wasi_async_runtime::sync::RwLock;

use tracing::instrument;

use thiserror::Error;

mod entry;
mod path;
mod revision;

pub use entry::{Data, List, ListEntry};
pub use path::Path;
pub use revision::{ReadRevision, Revision, RevisionId, WriteRevision};

use entry::Entry;

#[derive(Error, Debug, PartialEq)]
pub enum Error {
    #[error("file not found")]
    FileNotFound,

    #[error("dir not found")]
    DirNotFound,

    #[error("revision not found")]
    RevisionNotFound,

    #[error("invalid path")]
    InvalidPath,

    #[error("invalid data")]
    InvalidData,
}

fn data_is_valid(data: &[u8]) -> bool {
    data.iter()
        .all(|b| b.is_ascii_graphic() || b.is_ascii_whitespace())
}

pub struct Vcs(RwLock<Vec<Entry>>);

impl Vcs {
    #[must_use]
    pub fn new() -> Self {
        Self(RwLock::new(vec![]))
    }

    /// # Errors
    #[instrument(skip(self, path))]
    pub async fn list<'a, P>(
        &'a self,
        path: P,
    ) -> Result<List<impl Iterator<Item = ListEntry>>, Error>
    where
        P: TryInto<&'a Path, Error = Error>,
    {
        let path = path.try_into()?;

        let root_entries = self.0.read().await;

        let dir = Entry::dir(&root_entries, path)?;
        let mut v = dir
            .iter()
            .map(
                |Entry {
                     name,
                     subdirs,
                     revisions,
                 }| {
                    if revisions.is_empty() {
                        ListEntry::Dir(name.clone(), subdirs.len())
                    } else {
                        ListEntry::File(name.clone(), revisions.len())
                    }
                },
            )
            .collect::<Vec<_>>();
        v.sort();

        Ok(List {
            len: v.len(),
            iter: v.into_iter(),
        })
    }

    /// # Errors
    #[instrument(skip(self, path))]
    pub async fn get_current_revision<'a, P>(&'a self, path: P) -> Result<ReadRevision, Error>
    where
        P: TryInto<&'a Path, Error = Error>,
    {
        let path = path.try_into()?;

        let root_entries = self.0.read().await;

        let file = Entry::file(&root_entries, path)?;

        Ok(ReadRevision {
            data: file.get_current_revision()?.clone(),
            index: 0,
        })
    }

    /// # Errors
    #[instrument(skip(self, path, revision))]
    pub async fn get_revision<'a, P, R>(
        &'a self,
        path: P,
        revision: R,
    ) -> Result<ReadRevision, Error>
    where
        P: TryInto<&'a Path, Error = Error>,
        R: Into<Revision>,
    {
        let path = path.try_into()?;

        let root_entries = self.0.read().await;

        let file = Entry::file(&root_entries, path)?;

        Ok(ReadRevision {
            data: file.get_revision(revision.into().0)?.clone(),
            index: 0,
        })
    }

    /// # Errors
    pub fn put<'a, P>(&'a self, path: P) -> Result<WriteRevision<'a>, Error>
    where
        P: TryInto<&'a Path, Error = Error>,
    {
        let path = path.try_into()?;

        Ok(WriteRevision {
            vcs: self,
            path,
            data: vec![],
        })
    }
}

impl Default for Vcs {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use wasi::io::streams::StreamError;

    use wasi_async::io::{AsyncRead, AsyncWriteExt};
    use wasi_async_runtime::Reactor;

    use super::*;

    trait ReadToEnd: AsyncRead {
        async fn read_to_end(&mut self) -> Result<Vec<u8>, StreamError> {
            let mut buffer = vec![];
            loop {
                match self.read(4096).await {
                    Ok(mut tmp) => {
                        if tmp.is_empty() {
                            break Ok(buffer);
                        }
                        buffer.append(&mut tmp);
                    }
                    Err(StreamError::Closed) => return Ok(buffer),
                    Err(e) => return Err(e),
                }
            }
        }
    }

    impl<R: AsyncRead> ReadToEnd for R {}

    fn init_tracing_subscriber() {
        static INIT_TRACING_SUBSCRIBER: Once = Once::new();
        INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);
    }

    #[test]
    fn test_simple_list() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let vcs = Vcs::new();

            assert_eq!(vcs.list("/").await.unwrap().len(), 0);
        });
    }

    #[test]
    fn test_simple_put() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let vcs = Vcs::new();

            let mut write = vcs.put("/test").unwrap();

            assert_eq!(vcs.list("/").await.unwrap().len(), 0);

            write.write_all(b"Hello World!").await.unwrap();

            assert_eq!(vcs.list("/").await.unwrap().len(), 0);

            write.commit().await.unwrap();

            assert_eq!(vcs.list("/").await.unwrap().len(), 1);
            assert_eq!(
                vcs.list("/").await.unwrap().next().unwrap(),
                ListEntry::File("test".to_string(), 1)
            );
        });
    }

    #[test]
    fn test_same_put() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let vcs = Vcs::new();

            let mut write = vcs.put("/test").unwrap();
            write.write_all(b"Hello World!").await.unwrap();
            let r = write.commit().await.unwrap();

            let mut write = vcs.put("/test").unwrap();
            write.write_all(b"Hello World!").await.unwrap();
            assert_eq!(write.commit().await.unwrap(), r);

            assert_eq!(vcs.list("/").await.unwrap().len(), 1);
            assert_eq!(
                vcs.list("/").await.unwrap().next().unwrap(),
                ListEntry::File("test".to_string(), 1)
            );
        });
    }

    #[test]
    fn test_simple_get_current_revision() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let vcs = Vcs::new();

            let mut write = vcs.put("/test").unwrap();

            write.write_all(b"Hello World!").await.unwrap();
            write.commit().await.unwrap();

            let mut read = vcs.get_current_revision("/test").await.unwrap();

            let buffer = read.read_to_end().await.unwrap();

            assert_eq!(b"Hello World!".as_slice(), &buffer);
        });
    }

    #[test]
    fn test_simple_get_current_revision_2() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let vcs = Vcs::new();

            let mut write = vcs.put("/test").unwrap();

            write.write_all(b"Hello World! 1").await.unwrap();
            write.commit().await.unwrap();

            let mut write = vcs.put("/test").unwrap();

            write.write_all(b"Hello World! 2").await.unwrap();
            write.commit().await.unwrap();

            let mut read = vcs.get_current_revision("/test").await.unwrap();

            let buffer = read.read_to_end().await.unwrap();

            assert_eq!(b"Hello World! 2".as_slice(), &buffer);
        });
    }

    #[test]
    fn test_simple_get_current_revision_1() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let vcs = Vcs::new();

            let mut write = vcs.put("/test").unwrap();

            write.write_all(b"Hello World! 1").await.unwrap();
            write.commit().await.unwrap();

            let mut write = vcs.put("/test").unwrap();

            write.write_all(b"Hello World! 2").await.unwrap();
            write.commit().await.unwrap();

            let mut read = vcs.get_revision("/test", 0).await.unwrap();

            let buffer = read.read_to_end().await.unwrap();

            assert_eq!(b"Hello World! 1".as_slice(), &buffer);

            assert_eq!(
                vcs.list("/").await.unwrap().collect::<Vec<_>>(),
                vec![ListEntry::File("test".to_string(), 2)]
            );
        });
    }

    #[test]
    fn test_simple_get_current_revision_invalid() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let vcs = Vcs::new();

            let mut write = vcs.put("/test").unwrap();
            write.write_all(b"Hello World! 1").await.unwrap();
            write.commit().await.unwrap();

            let mut write = vcs.put("/test").unwrap();
            write.write_all(b"Hello World! 2").await.unwrap();
            write.commit().await.unwrap();

            assert_eq!(
                Error::RevisionNotFound,
                vcs.get_revision("/test", 3).await.unwrap_err()
            );
        });
    }

    #[test]
    fn test_nexted_put() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let vcs = Vcs::new();

            let mut write = vcs.put("/dir/test").unwrap();

            assert_eq!(vcs.list("/").await.unwrap().len(), 0);

            write.write_all(b"Hello World!").await.unwrap();

            assert_eq!(vcs.list("/").await.unwrap().len(), 0);

            write.commit().await.unwrap();

            assert_eq!(vcs.list("/").await.unwrap().len(), 1);
            assert_eq!(
                vcs.list("/").await.unwrap().next().unwrap(),
                ListEntry::Dir("dir".to_string(), 1)
            );

            assert_eq!(vcs.list("/dir").await.unwrap().len(), 1);
            assert_eq!(
                vcs.list("/dir").await.unwrap().next().unwrap(),
                ListEntry::File("test".to_string(), 1)
            );
        });
    }

    #[test]
    fn test_nexted_put_valid() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let vcs = Vcs::new();

            let mut write = vcs.put("/dir/test").unwrap();
            write.write_all(b"Hello World!").await.unwrap();
            write.commit().await.unwrap();

            let mut write = vcs.put("/dir").unwrap();
            write.write_all(b"Hello World!").await.unwrap();
            assert_eq!(Ok(0), write.commit().await);
        });
    }
}
