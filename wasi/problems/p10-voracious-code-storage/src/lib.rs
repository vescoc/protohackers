#![doc = include_str!("../README.md")]

use std::rc::Rc;

use wasi::io::streams::StreamError;
use wasi::sockets::network::ErrorCode;

use wasi_async::io::{AsyncRead, AsyncWriteExt};
use wasi_async::net::{TcpListener, TcpStream};
use wasi_async_runtime::Reactor;

use tracing::{debug, info, instrument};

pub mod io;
pub mod vcs;

use io::{BufReader, WriteLine};

use vcs::{ListEntry, Path, Vcs};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("stream error: {0}")]
    Stream(#[from] StreamError),

    #[error("tcp socket error: {0}")]
    TcpSocket(#[from] ErrorCode),
}

/// # Errors
#[instrument(skip(listener))]
pub async fn run(listener: TcpListener) -> Result<(), Error> {
    let mut client_id = 0;
    let vcs = Rc::new(Vcs::new());

    loop {
        let (stream, remote_addr) = listener.accept().await?;

        info!("remote: {remote_addr:?}");

        Reactor::spawn_in_current(handle_client(client_id, Rc::clone(&vcs), stream)).await;

        client_id += 1;
    }
}

async fn copy_n<'a, R, W>(
    reader: &'a mut R,
    writer: &'a mut W,
    mut size: usize,
) -> Result<usize, StreamError>
where
    R: AsyncRead + Unpin + ?Sized,
    W: AsyncWriteExt + Unpin + ?Sized,
{
    let original_size = size;
    let mut written_size = 0;
    while size > 0 {
        let data = reader.read(size as u64).await?;
        let len = data.len();
        assert!(len > 0 && len <= size);
        writer.write_all(&data).await?;
        size -= len;
        written_size += len;
    }

    writer.flush().await?;

    assert_eq!(written_size, original_size);

    Ok(written_size)
}

/// # Errors
#[allow(clippy::too_many_lines)]
#[instrument(skip(stream, vcs))]
async fn handle_client(client_id: usize, vcs: Rc<Vcs>, mut stream: TcpStream) -> Result<(), Error> {
    debug!("start {client_id}");

    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);

    loop {
        writer.write_line(b"READY").await?;

        let buffer = reader.read_line().await?;
        if buffer.is_empty() {
            break;
        }
        let buffer = buffer.trim();

        debug!("request: {buffer}");

        let mut parts = buffer.split_whitespace();
        if let Some(command) = parts.next() {
            if command.eq_ignore_ascii_case("HELP") {
                writer.write_line(b"OK usage: HELP|GET|PUT|LIST").await?;
            } else if command.eq_ignore_ascii_case("LIST") {
                match (parts.next(), parts.next()) {
                    (Some(dir), None) => {
                        if let Ok(list) = vcs.list(dir).await {
                            writer
                                .write_line(format!("OK {}", list.len()).as_bytes())
                                .await?;
                            for entry in list {
                                match entry {
                                    ListEntry::Dir(name, _) => {
                                        writer
                                            .write_line(format!("{name}/ DIR").as_bytes())
                                            .await?;
                                    }
                                    ListEntry::File(name, releases) => {
                                        writer
                                            .write_line(format!("{name} r{releases}").as_bytes())
                                            .await?;
                                    }
                                }
                            }
                        } else {
                            writer.write_line(b"ERR no such file").await?;
                        }
                    }

                    _ => writer.write_line(b"ERR usage: LIST dir").await?,
                }
            } else if command.eq_ignore_ascii_case("PUT") {
                match (parts.next(), parts.next(), parts.next()) {
                    (Some(path), Some(size), None) => {
                        if let Ok(size) = size.parse::<usize>() {
                            if let Ok(mut write) = vcs.put(path) {
                                copy_n(&mut reader, &mut write, size).await?;
                                if let Ok(revision) = write.commit().await {
                                    debug!("PUT {path} {revision} -> {size}");
                                    writer
                                        .write_line(format!("OK r{}", revision + 1).as_bytes())
                                        .await?;
                                } else {
                                    debug!("PUT invalid file {path} {size} <commit>");
                                    writer.write_line(b"ERR invalid file").await?;
                                }
                            } else {
                                debug!("PUT invalid file {path} {size} <put>");
                                writer.write_line(b"ERR invalid file").await?;
                            }
                        } else {
                            debug!("PUT invalid size {path} {size}");
                            writer.write_line(b"ERR invalid size").await?;
                        }
                    }

                    (Some(path), None, None) => {
                        if Path::new(path.as_bytes()).is_ok() {
                            writer
                                .write_line(b"ERR usage: PUT file length newline data")
                                .await?;
                        } else {
                            writer.write_line(b"ERR illegal file name").await?;
                        }
                    }

                    _ => {
                        writer
                            .write_line(b"ERR usage: PUT file length newline data")
                            .await?;
                    }
                }
            } else if command.eq_ignore_ascii_case("GET") {
                match (parts.next(), parts.next(), parts.next()) {
                    (Some(path), Some(revision), None) => {
                        if let Some(revision) = revision.strip_prefix('r') {
                            if let Ok(revision) = revision.parse::<usize>() {
                                if revision > 0 {
                                    let revision = revision - 1;
                                    if let Ok(mut read) = vcs.get_revision(path, revision).await {
                                        let len = read.len();
                                        debug!("GET {path} {revision} -> {len}");
                                        writer.write_line(format!("OK {len}").as_bytes()).await?;
                                        copy_n(&mut read, &mut writer, len).await?;
                                    } else {
                                        debug!("GET no such file {path} {revision}");
                                        writer.write_line(b"ERR no such file").await?;
                                    }
                                } else {
                                    debug!("GET invalid revision spec {path} {revision}");
                                    writer.write_line(b"ERR invalid revision spec").await?;
                                }
                            } else {
                                debug!("GET invalid revision spec {path} {revision}");
                                writer.write_line(b"ERR invalid revision spec").await?;
                            }
                        } else {
                            debug!("GET invalid revision spec {path} {revision}");
                            writer.write_line(b"ERR invalid revision spec").await?;
                        }
                    }

                    (Some(path), None, None) => {
                        if let Ok(mut read) = vcs.get_current_revision(path).await {
                            let len = read.len();
                            writer.write_line(format!("OK {len}").as_bytes()).await?;
                            copy_n(&mut read, &mut writer, len).await?;
                        } else {
                            writer.write_line(b"ERR no such file").await?;
                        }
                    }

                    _ => writer.write_line(b"ERR usage: GET file [revision]").await?,
                }
            } else {
                debug!("illegal method: {command}");
                writer
                    .write_line(format!("ERR illegal method: {command}").as_bytes())
                    .await?;
            }
        }
    }

    debug!("done {client_id}");

    Ok(())
}
