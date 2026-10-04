use std::cmp;
use std::io;

use wasi::io::streams::StreamError;

use wasi_async::codec::{Decoder, Encoder};

use bytes::{Buf, BufMut, BytesMut};

use thiserror::Error;

use serde_json::Value;

#[derive(Error, Debug)]
pub enum RequestError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("stream error: {0}")]
    Stream(#[from] StreamError),

    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("max line length exceeded")]
    MaxLineLengthExceeded,

    #[error("field not found: {0}")]
    FieldNotFound(&'static str),

    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

#[derive(Error, Debug)]
pub enum ResponseError {
    #[error("stream error: {0}")]
    Stream(#[from] StreamError),

    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, PartialEq)]
pub enum Request {
    Put {
        queue: String,
        job: Value,
        priority: u32,
    },

    Get {
        queues: Vec<String>,
        wait: bool,
    },

    Delete {
        id: u32,
    },

    Abort {
        id: u32,
    },
}

/// A simple [`Decoder`] and [`Encoder`] implementation that splits up
/// data in [`Request`].
pub struct RequestCodec {
    /// The maximum length for a given line.
    max_length: usize,

    /// Stored index of the next index to examine for a `\n`
    /// character.
    next_index: usize,

    /// Are we currently discarding the remainder of a line which was
    /// over the length limit?
    is_discarding: bool,
}

impl RequestCodec {
    #[must_use]
    pub fn new() -> Self {
        Self {
            max_length: usize::MAX,
            next_index: 0,
            is_discarding: false,
        }
    }

    #[must_use]
    #[allow(unused)]
    pub fn new_with_max_length(max_length: usize) -> Self {
        Self {
            max_length,
            ..Self::new()
        }
    }

    #[must_use]
    #[allow(unused)]
    pub fn max_length(&self) -> usize {
        self.max_length
    }
}

impl Decoder for RequestCodec {
    type Item = Request;
    type Error = RequestError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            let read_to = cmp::min(self.max_length.saturating_add(1), src.len());

            let newline_offset = src[self.next_index..read_to]
                .iter()
                .position(|b| *b == b'\n');

            match (self.is_discarding, newline_offset) {
                (true, Some(offset)) => {
                    src.advance(offset + self.next_index + 1);
                    self.is_discarding = false;
                    self.next_index = 0;
                }

                (true, None) => {
                    src.advance(read_to);
                    self.next_index = 0;
                    if src.is_empty() {
                        return Ok(None);
                    }
                }

                (false, Some(offset)) => {
                    let newline_index = offset + self.next_index;
                    self.next_index = 0;
                    let line = src.split_to(newline_index + 1);
                    let line = &line[..line.len() - 1];

                    let value: Value = serde_json::from_slice(line)?;

                    let request = match value.get("request") {
                        Some(Value::String(v)) if v == "put" => Request::Put {
                            queue: serde_json::from_value(
                                value
                                    .get("queue")
                                    .ok_or(RequestError::FieldNotFound("queue"))?
                                    .clone(),
                            )?,
                            job: value
                                .get("job")
                                .ok_or(RequestError::FieldNotFound("job"))?
                                .clone(),
                            priority: serde_json::from_value(
                                value
                                    .get("pri")
                                    .ok_or(RequestError::FieldNotFound("pri"))?
                                    .clone(),
                            )?,
                        },

                        Some(Value::String(v)) if v == "get" => Request::Get {
                            queues: serde_json::from_value(
                                value
                                    .get("queues")
                                    .ok_or(RequestError::FieldNotFound("queues"))?
                                    .clone(),
                            )?,
                            wait: {
                                if let Some(wait) = value.get("wait") {
                                    serde_json::from_value(wait.clone())?
                                } else {
                                    false
                                }
                            },
                        },

                        Some(Value::String(v)) if v == "delete" => Request::Delete {
                            id: serde_json::from_value(
                                value
                                    .get("id")
                                    .ok_or(RequestError::FieldNotFound("id"))?
                                    .clone(),
                            )?,
                        },

                        Some(Value::String(v)) if v == "abort" => Request::Abort {
                            id: serde_json::from_value(
                                value
                                    .get("id")
                                    .ok_or(RequestError::FieldNotFound("id"))?
                                    .clone(),
                            )?,
                        },

                        Some(Value::String(v)) => {
                            return Err(RequestError::InvalidRequest(v.clone()));
                        }

                        Some(_) => {
                            return Err(RequestError::InvalidRequest(
                                "invalid field type".to_string(),
                            ));
                        }

                        None => return Err(RequestError::FieldNotFound("request")),
                    };

                    return Ok(Some(request));
                }

                (false, None) if src.len() > self.max_length => {
                    self.is_discarding = true;
                    return Err(RequestError::MaxLineLengthExceeded);
                }

                (false, None) => {
                    self.next_index = read_to;
                    return Ok(None);
                }
            }
        }
    }
}

impl Encoder<Request> for RequestCodec {
    type Error = RequestError;

    fn encode(&mut self, request: Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let data = match request {
            Request::Put {
                queue,
                job,
                priority,
            } => serde_json::json!({
                "request": "put",
                "queue": queue,
                "job": job,
                "pri": priority,
            }),

            Request::Get { queues, wait } => serde_json::json!({
                "request": "get",
                "queues": queues,
                "wait": wait,
            }),

            Request::Delete { id } => serde_json::json!({
                "request": "delete",
                "id": id,
            }),

            Request::Abort { id } => serde_json::json!({
                "request": "abort",
                "id": id,
            }),
        };
        let data = serde_json::to_vec(&data)?;
        if data.len() > self.max_length {
            return Err(RequestError::MaxLineLengthExceeded);
        }

        dst.reserve(data.len() + 1);
        dst.put(&*data);
        dst.put_u8(b'\n');

        Ok(())
    }
}

impl Default for RequestCodec {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, PartialEq)]
pub enum Response {
    Ok,
    Id(u32),
    JobRef(u32, Value, u32, String),
    Error(String),
    NoJob,
}

pub struct ResponseCodec {
    next_index: usize,
}

impl ResponseCodec {
    #[must_use]
    pub fn new() -> Self {
        Self { next_index: 0 }
    }
}

impl Default for ResponseCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for ResponseCodec {
    type Item = Response;
    type Error = ResponseError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let newline_offset = src[self.next_index..].iter().position(|b| *b == b'\n');
        if let Some(offset) = newline_offset {
            let newline_index = self.next_index + offset;
            self.next_index = 0;
            let line = src.split_to(newline_index + 1);
            let line = &line[..line.len() - 1];

            let value: Value = serde_json::from_slice(line)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
            let response = match value.get("status") {
                Some(Value::String(v)) if v == "ok" => match value.get("id") {
                    Some(id @ Value::Number(_)) => match value.get("queue") {
                        Some(queue @ Value::String(_)) => Response::JobRef(
                            serde_json::from_value(id.clone()).map_err(|_| {
                                io::Error::new(io::ErrorKind::InvalidData, "invalid field id")
                            })?,
                            value
                                .get("job")
                                .ok_or_else(|| {
                                    io::Error::new(io::ErrorKind::InvalidData, "invalid field job")
                                })?
                                .clone(),
                            serde_json::from_value(
                                value
                                    .get("pri")
                                    .ok_or_else(|| {
                                        io::Error::new(io::ErrorKind::InvalidData, "no field pri")
                                    })?
                                    .clone(),
                            )
                            .map_err(|_| {
                                io::Error::new(io::ErrorKind::InvalidData, "invalid field job")
                            })?,
                            serde_json::from_value(queue.clone()).map_err(|_| {
                                io::Error::new(io::ErrorKind::InvalidData, "invalid field job")
                            })?,
                        ),

                        _ => Response::Id(serde_json::from_value(id.clone()).map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "invalid field id")
                        })?),
                    },

                    _ => Response::Ok,
                },

                Some(Value::String(v)) if v == "error" => {
                    if let Some(error) = value.get("error") {
                        Response::Error(error.to_string())
                    } else {
                        Response::Error(String::from("<EMPTY ERROR MESSAGE>"))
                    }
                }

                Some(Value::String(v)) if v == "no-job" => Response::NoJob,

                _ => {
                    return Err(
                        io::Error::new(io::ErrorKind::InvalidData, "no status field").into(),
                    );
                }
            };

            Ok(Some(response))
        } else {
            self.next_index = src.len();
            Ok(None)
        }
    }
}

impl Encoder<Response> for ResponseCodec {
    type Error = ResponseError;

    fn encode(&mut self, response: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let data = match response {
            Response::Ok => serde_json::to_vec(&serde_json::json!({
                "status": "ok"
            })),

            Response::Id(id) => serde_json::to_vec(&serde_json::json!({
                "status": "ok",
                "id": id,
            })),

            Response::JobRef(id, job, priority, queue) => serde_json::to_vec(&serde_json::json!({
                "status": "ok",
                "id": id,
                "job": job,
                "pri": priority,
                "queue": queue,
            })),

            Response::Error(msg) => serde_json::to_vec(&serde_json::json!({
                "status": "error",
                "error": msg
            })),

            Response::NoJob => serde_json::to_vec(&serde_json::json!({
                "status": "no-job"
            })),
        };

        let data = data.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "serde error"))?;

        dst.reserve(data.len() + 1);
        dst.put(&*data);
        dst.put_u8(b'\n');

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt, TryStreamExt};

    use wasi_async::codec::{FramedRead, FramedWrite};
    use wasi_async_runtime::Reactor;

    use super::*;

    #[test]
    fn test_write_response_ok() {
        Reactor::block_on(|_| async {
            let mut buffer = vec![];
            {
                let mut write =
                    std::pin::pin!(FramedWrite::new(&mut buffer, ResponseCodec::new()).into_sink());

                write.send(Response::Ok).await.unwrap();
            }

            assert_eq!(
                [br#"{"status":"ok"}"#.as_slice(), b"\n".as_slice()].concat(),
                buffer
            );
        });
    }

    #[test]
    fn test_write_response_error() {
        Reactor::block_on(|_| async {
            let mut buffer = vec![];

            {
                let mut write =
                    std::pin::pin!(FramedWrite::new(&mut buffer, ResponseCodec::new()).into_sink());
                write
                    .send(Response::Error("prova".to_string()))
                    .await
                    .unwrap();
            }

            let ok = std::str::from_utf8(
                &[
                    br#"{"status":"error","error":"prova"}"#.as_slice(),
                    b"\n".as_slice(),
                ]
                .concat(),
            ) == std::str::from_utf8(&buffer)
                || std::str::from_utf8(
                    &[
                        br#"{"error":"prova","status":"error"}"#.as_slice(),
                        b"\n".as_slice(),
                    ]
                    .concat(),
                ) == std::str::from_utf8(&buffer);
            assert!(ok);
        });
    }

    #[test]
    fn test_write_response_no_job() {
        Reactor::block_on(|_| async {
            let mut buffer = vec![];
            {
                let mut write =
                    std::pin::pin!(FramedWrite::new(&mut buffer, ResponseCodec::new()).into_sink());

                write.send(Response::NoJob).await.unwrap();
            }

            assert_eq!(
                [br#"{"status":"no-job"}"#.as_slice(), b"\n".as_slice()].concat(),
                buffer
            );
        });
    }

    #[test]
    fn test_read_request_put() {
        Reactor::block_on(|_| async {
            let buffer = [
                br#"{"request":"put","queue":"queue1","job":{"name":"job1"},"pri":123}"#.as_slice(),
                b"\n".as_slice(),
            ]
            .concat();

            let mut read =
                std::pin::pin!(FramedRead::new(&*buffer, RequestCodec::new()).into_stream());

            assert_eq!(
                read.try_next().await.unwrap().unwrap(),
                Request::Put {
                    queue: "queue1".to_string(),
                    job: serde_json::json!({"name":"job1"}),
                    priority: 123,
                }
            );
        });
    }

    #[test]
    fn test_read_request_get() {
        Reactor::block_on(|_| async {
            let buffer = [
                br#"{"request":"get","queues":["queue1","queue2"],"wait":true}"#.as_slice(),
                b"\n".as_slice(),
            ]
            .concat();

            let mut read =
                std::pin::pin!(FramedRead::new(&*buffer, RequestCodec::new()).into_stream());

            assert_eq!(
                read.try_next().await.unwrap().unwrap(),
                Request::Get {
                    queues: vec!["queue1".to_string(), "queue2".to_string()],
                    wait: true,
                }
            );
        });
    }

    #[test]
    fn test_read_request_delete() {
        Reactor::block_on(|_| async {
            let buffer = [
                br#"{"request":"delete","id":123}"#.as_slice(),
                b"\n".as_slice(),
            ]
            .concat();

            let mut read =
                std::pin::pin!(FramedRead::new(&*buffer, RequestCodec::new()).into_stream());

            assert_eq!(
                read.try_next().await.unwrap().unwrap(),
                Request::Delete { id: 123 }
            );
        });
    }

    #[test]
    fn test_read_request_abort() {
        Reactor::block_on(|_| async {
            let buffer = [
                br#"{"request":"abort","id":123}"#.as_slice(),
                b"\n".as_slice(),
            ]
            .concat();

            let mut read =
                std::pin::pin!(FramedRead::new(&*buffer, RequestCodec::new()).into_stream());

            assert_eq!(
                read.try_next().await.unwrap().unwrap(),
                Request::Abort { id: 123 }
            );
        });
    }
}
