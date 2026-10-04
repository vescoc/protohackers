#![doc = include_str!("../README.md")]

use std::pin;

use futures::{SinkExt, StreamExt};

use wasi::io::streams::StreamError;
use wasi::sockets::network::ErrorCode;

use wasi_async::{
    codec,
    net::{TcpListener, TcpStream},
};
use wasi_async_runtime::Reactor;

use thiserror::Error;
use tracing::{debug, info, instrument, warn};

mod job_centre;
pub mod protocol;

use job_centre::{JobCentre, JobRef, Worker};
use protocol::{Request, Response};

#[derive(Error, Debug)]
pub enum Error {
    #[error("stream error: {0}")]
    Stream(#[from] StreamError),

    #[error("tcp socket error: {0}")]
    TcpSocket(#[from] ErrorCode),

    #[error("response error: {0}")]
    Response(#[from] protocol::ResponseError),
}

/// # Errors
#[instrument(skip(listener))]
pub async fn run(listener: TcpListener) -> Result<(), Error> {
    let job_centre = JobCentre::new();

    loop {
        let (stream, remote_addr) = listener.accept().await?;

        info!("run start: {remote_addr:?}");

        let worker = job_centre.make_worker();
        Reactor::spawn_in_current(handle_client(stream, worker)).await;
    }
}

/// # Errors
#[instrument(skip(stream, worker))]
async fn handle_client(mut stream: TcpStream, mut worker: Worker) -> Result<(), Error> {
    debug!("start client");

    let (read, write) = stream.split();
    let mut read =
        pin::pin!(codec::FramedRead::new(read, protocol::RequestCodec::new()).into_stream());
    let mut write =
        pin::pin!(codec::FramedWrite::new(write, protocol::ResponseCodec::new()).into_sink());

    while let Some(request) = read.next().await {
        debug!("request: start {request:?}");
        match request {
            Ok(Request::Put {
                queue,
                job,
                priority,
            }) => {
                let id = worker.put(queue, job, priority);
                write.send(Response::Id(id)).await?;
                write.flush().await?;
            }

            Ok(Request::Get { queues, wait }) => {
                match worker.get(&queues, wait).await {
                    Ok(JobRef(id, job, priority, queue)) => {
                        write
                            .send(Response::JobRef(id, job, priority, queue))
                            .await?;
                    }
                    Err(_) => {
                        write.send(Response::NoJob).await?;
                    }
                }
                write.flush().await?;
            }

            Ok(Request::Delete { id }) => {
                if worker.delete(id).is_ok() {
                    write.send(Response::Ok).await?;
                } else {
                    write.send(Response::NoJob).await?;
                }
                write.flush().await?;
            }

            Ok(Request::Abort { id }) => {
                if worker.abort(id).is_ok() {
                    write.send(Response::Ok).await?;
                } else {
                    write.send(Response::NoJob).await?;
                }
                write.flush().await?;
            }

            Err(err) => {
                warn!("error: {err}");
                write
                    .send(Response::Error("invalid request".to_string()))
                    .await?;
                write.flush().await?;
            }
        }

        debug!("request: done");
    }

    Ok(())
}
