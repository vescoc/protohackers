use std::fmt::Write;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};

use futures::{channel::mpsc, stream::FusedStream, Sink, SinkExt, Stream, StreamExt, TryStreamExt};

use tracing::{debug, error, info, instrument, trace};

use wasi::io::streams::StreamError;

use wasi_async::codec::{Encoder, FramedRead, FramedWrite, LinesDecoder};
use wasi_async::net::TcpStream;

use crate::{ClientMessage, Error, Id, ServerMessage};

struct WaitingWelcome;
struct Joining;
struct Joined;
struct Chatting;

struct Client<'a, R, W, T> {
    id: Id,
    read: R,
    write: W,
    server: &'a mut mpsc::UnboundedSender<ClientMessage>,
    #[allow(clippy::struct_field_names)]
    client: mpsc::UnboundedReceiver<ServerMessage>,
    _state: PhantomData<T>,
}

impl<'a, R, W, T> Client<'a, R, W, T> {
    fn into<D>(self) -> Client<'a, R, W, D> {
        Client {
            id: self.id,
            read: self.read,
            write: self.write,
            server: self.server,
            client: self.client,
            _state: PhantomData,
        }
    }
}

impl<'a, R, W> Client<'a, R, W, WaitingWelcome>
where
    R: Stream<Item = Result<String, StreamError>>,
    W: Sink<String, Error = StreamError> + Unpin,
{
    #[instrument(skip_all)]
    async fn welcome(mut self) -> Result<Option<Client<'a, R, W, Joining>>, Error> {
        debug!("state: waiting welcome message");
        match self.client.next().await {
            Some(ServerMessage::Welcome) => {
                debug!("got welcome message");
                self.write
                    .send("Welcome to budgetchat! What shall I call you?".to_string())
                    .await?;
                Ok(Some(self.into()))
            }
            None => Ok(None),
            message => unreachable!("invalid server message {:?}", message),
        }
    }
}

impl<'a, R, W> Client<'a, R, W, Joining>
where
    R: Stream<Item = Result<String, StreamError>> + Unpin,
    W: Sink<String, Error = StreamError> + Unpin,
{
    #[instrument(skip_all)]
    async fn joining(mut self) -> Result<Option<Client<'a, R, W, Joined>>, Error> {
        debug!("state: joining");
        match self.read.next().await {
            Some(Ok(username)) => {
                debug!("got username");
                self.server
                    .send(ClientMessage::SetUsername(self.id, username))
                    .await?;
            }
            _ => return Ok(None),
        }

        match self.client.next().await {
            Some(ServerMessage::UsernameAccepted) => {
                self.server.send(ClientMessage::Joined(self.id)).await?;
                Ok(Some(self.into()))
            }
            Some(ServerMessage::UsernameInvalid) => {
                self.write.send("Invalid username".to_string()).await?;
                Err(Error::InvalidUsername)
            }
            None => Ok(None),
            message => unreachable!("invalid server message {:?}", message),
        }
    }
}

impl<'a, R, W> Client<'a, R, W, Joined>
where
    R: Stream<Item = Result<String, StreamError>>,
    W: Sink<String, Error = StreamError> + Unpin,
{
    #[instrument(skip(self))]
    async fn joined(mut self) -> Result<Option<Client<'a, R, W, Chatting>>, Error> {
        debug!("state: joined");
        match self.client.next().await {
            Some(ServerMessage::Users(users)) => {
                let mut message = String::new();
                message.push_str("* The room contains:");
                if !users.is_empty() {
                    message.push(' ');
                    let mut iter = users.iter();
                    message.push_str(iter.next().unwrap());
                    for user in iter {
                        message.push_str(", ");
                        message.push_str(user);
                    }
                }

                self.write.send(message).await?;
                Ok(Some(self.into()))
            }
            None => Ok(None),
            message => unreachable!("invalid server message {:?}", message),
        }
    }
}

impl<R, W> Client<'_, R, W, Chatting>
where
    R: Stream<Item = Result<String, StreamError>> + Unpin + FusedStream,
    W: Sink<String, Error = StreamError> + Unpin,
{
    #[instrument(skip(self))]
    async fn chatting(mut self) -> Result<(), Error> {
        loop {
            trace!("state: main loop");
            futures::select_biased! {
                message = self.client.next() => {
                    trace!("got server message: {message:?}");
                    match message {
                        Some(ServerMessage::AnnounceUser(user)) => {
                            let mut message = String::new();
                            write!(&mut message, "* {} has entered the room", *user).ok();
                            self.write.send(message).await?;
                        }
                        Some(ServerMessage::Message(user, msg)) => {
                            let mut message = String::new();
                            write!(&mut message, "[{}] {}", *user, *msg).ok();
                            self.write.send(message).await?;
                        }
                        Some(ServerMessage::Disconnected(user)) => {
                            let mut message = String::new();
                            write!(&mut message, "* {} has left the room", *user).ok();
                            self.write.send(message).await?;
                        }
                        None => break,
                        message => unreachable!("invalid server message {:?}", message),
                    }
                }

                segment = self.read.next() => {
                    trace!("got client message: {segment:?}");
                    match segment {
                        Some(Ok(message)) => {
                            self.server.unbounded_send(ClientMessage::Message(self.id, Arc::new(message))).unwrap();
                        }
                        _ => break,
                    }
                }
            }
        }
        debug!("done chatting");

        Ok(())
    }
}

/// Client iterations.
///
/// Listen for a client and run the chat.
///
/// # Errors:
/// * Error when socket returns an error.
#[instrument(skip(stream, server, client))]
pub(crate) async fn handle_client(
    id: Id,
    mut stream: TcpStream,
    mut server: mpsc::UnboundedSender<ClientMessage>,
    client: mpsc::UnboundedReceiver<ServerMessage>,
) {
    info!("start {id}");

    let (read, write) = stream.split();
    let client = Client {
        id,
        read: FramedRead::new(read, LinesDecoder::new())
            .map_ok(|line| String::from_utf8_lossy(&line).into_owned())
            .fuse(),
        write: FramedWrite::new(write, LinesEncoder),
        server: &mut server,
        client,
        _state: PhantomData::<WaitingWelcome>,
    };

    let client = match client.welcome().await {
        Ok(Some(client)) => client,
        Ok(None) => {
            server.unbounded_send(ClientMessage::Disconnect(id)).ok();
            return;
        }
        Err(err) => {
            error!("error {:?}", err);
            server.unbounded_send(ClientMessage::Disconnect(id)).ok();
            return;
        }
    };

    let client = match client.joining().await {
        Ok(Some(client)) => client,
        Ok(None) => {
            server.unbounded_send(ClientMessage::Disconnect(id)).ok();
            return;
        }
        Err(err) => {
            error!("error {:?}", err);
            server.unbounded_send(ClientMessage::Disconnect(id)).ok();
            return;
        }
    };

    let client = match client.joined().await {
        Ok(Some(client)) => client,
        Ok(None) => {
            server.unbounded_send(ClientMessage::Disconnect(id)).ok();
            return;
        }
        Err(err) => {
            error!("error {:?}", err);
            server.unbounded_send(ClientMessage::Disconnect(id)).ok();
            return;
        }
    };

    if let Err(err) = client.chatting().await {
        error!("error {:?}", err);
        server.unbounded_send(ClientMessage::Disconnect(id)).ok();
    } else {
        debug!("state: done");
        server.unbounded_send(ClientMessage::Disconnect(id)).ok();
    }

    info!("done {id}");
}

struct LinesEncoder;

impl Encoder<String> for LinesEncoder {
    type Error = StreamError;

    fn encode(&mut self, line: String, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(line.as_bytes());
        dst.put_u8(b'\n');
        Ok(())
    }
}
