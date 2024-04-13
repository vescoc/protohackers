//! Speed daemon
//!
//! Motorists on Freedom Island drive as fast as they like. Sadly,
//! this has led to a large number of crashes, so the islanders have
//! agreed to impose speed limits. The speed limits will be enforced
//! via an average speed check: Automatic Number Plate Recognition
//! cameras will be installed at various points on the road
//! network. The islanders will use a computer system to collect the
//! data, detect cars travelling in excess of the speed limit, and
//! send tickets to be dispatched to the drivers. The islanders can't
//! agree on which one of them should write the software, so they've
//! engaged an external contractor to do it: that's where you come in.
//!
//! # Overview
//!
//! You need to build a server to coordinate enforcement of average
//! speed limits on the Freedom Island road network.
//!
//! Your server will handle two types of client: cameras and ticket
//! dispatchers.
//!
//! Clients connect over TCP and speak a protocol using a binary
//! format. Make sure you support at least 150 simultaneous clients.
//!
//! ## Cameras
//!
//! Each camera is on a specific road, at a specific location, and has
//! a specific speed limit. Each camera provides this information when
//! it connects to the server.
//!
//! Cameras report each number plate that they observe, along with the
//! timestamp that they observed it.
//!
//! Timestamps are exactly the same as Unix timestamps (counting
//! seconds since 1st of January 1970), except that they are unsigned.
//!
//! ## Ticket dispatchers
//!
//! Each ticket dispatcher is responsible for some number of
//! roads. When the server finds that a car was detected at 2 points
//! on the same road with an average speed in excess of the speed
//! limit (speed = distance / time), it will find the responsible
//! ticket dispatcher and send it a ticket for the offending car, so
//! that the ticket dispatcher can perform the necessary legal
//! rituals.
//!
//! ## Roads
//!
//! Each road in the network is identified by a number from 0 to
//! 65535. A single road has the same speed limit at every point on
//! the road. Positions on the roads are identified by the number of
//! miles from the start of the road. Remarkably, all speed cameras
//! are positioned at exact integer numbers of miles from the start of
//! the road.
//!
//! ## Cars
//!
//! Each car has a specific number plate represented as an uppercase
//! alphanumeric string.
//!
//! # Data types
//!
//! The protocol uses a binary data format, with the following primitive types:
//! > `u8`, `u16`, `u32`
//!
//! These types represent unsigned integers of 8-bit, 16-bit, and
//! 32-bit size, respectively. They are transmitted in network
//! byte-order (big endian).
//!
//! ### Examples:
//!
//! ```raw
//! Type | Hex data    | Value
//! -------------------------------
//! u8   |          20 |         32
//! u8   |          e3 |        227
//! u16  |       00 20 |         32
//! u16  |       12 45 |       4677
//! u16  |       a8 23 |      43043
//! u32  | 00 00 00 20 |         32
//! u32  | 00 00 12 45 |       4677
//! u32  | a6 a9 b5 67 | 2796139879
//! ```
//!
//! ## str
//!
//! A string of characters in a length-prefixed format. A str is
//! transmitted as a single u8 containing the string's length (0 to
//! 255), followed by that many bytes of u8, in order, containing
//! ASCII character codes.
//!
//! ### Examples:
//!
//! ```raw
//! Type | Hex data                   | Value
//! ----------------------------------------------
//! str  | 00                         | ""
//! str  | 03 66 6f 6f                | "foo"
//! str  | 08 45 6C 62 65 72 65 74 68 | "Elbereth"
//! ```
//!
//! # Message types
//!
//! Each message starts with a single u8 specifying the message
//! type. This is followed by the message contents, as detailed below.
//!
//! Field names are not transmitted. You know which field is which by
//! the order they are in.
//!
//! There is no message delimiter. Messages are simply concatenated
//! together with no padding. The 2nd message starts with the byte
//! that comes immediately after the final byte of the 1st message,
//! and so on.
//!
//! In the examples shown below, the hexadecimal data is broken across
//! several lines to aid comprehension, but of course in the real
//! protocol there is no such distinction.
//!
//! It is an error for a client to send the server a message with any
//! message type value that is not listed below with "Client->Server".
//!
//! ## 0x10: Error (Server->Client)
//!
//! Fields:
//!
//! - msg: `str`
//!
//! When the client does something that this protocol specification
//! declares "an error", the server must send the client an
//! appropriate Error message and immediately disconnect that client.
//!
//! ### Examples:
//!
//! ```raw
//! Hexadecimal:                            Decoded:
//! 10                                      Error{
//! 03 62 61 64                                 msg: "bad"
//!                                         }
//!
//! 10                                      Error{
//! 0b 69 6c 6c 65 67 61 6c 20 6d 73 67         msg: "illegal msg"
//!                                         }
//! ```
//!
//! ## 0x20: Plate (Client->Server)
//!
//! Fields:
//!
//! - plate: `str`
//! - timestamp: `u32`
//!
//! This client has observed the given number plate at its location,
//! at the given timestamp. Cameras can send observations in any order
//! they like, and after any delay they like, so you won't necessarily
//! receive observations in the order that they were made. This means
//! a later Plate message may correspond to an earlier observation
//! (with lower timestamp) even if they're both from the same
//! camera. You need to take observation timestamps from the Plate
//! message. Ignore your local system clock.
//!
//! It is an error for a client that has not identified itself as a
//! camera (see `IAmCamera` below) to send a Plate message.
//!
//! ### Examples:
//!
//! ```raw
//! Hexadecimal:                Decoded:
//! 20                          Plate{
//! 04 55 4e 31 58                  plate: "UN1X",
//! 00 00 03 e8                     timestamp: 1000
//!                             }
//!
//! 20                          Plate{
//! 07 52 45 30 35 42 4b 47         plate: "RE05BKG",
//! 00 01 e2 40                     timestamp: 123456
//!                             }
//! ```
//!
//! ## 0x21: Ticket (Server->Client)
//!
//! Fields:
//!
//! - plate: `str`
//! - road: `u16`
//! - mile1: `u16`
//! - timestamp1: `u32`
//! - mile2: `u16`
//! - timestamp2: `u32`
//! - speed: `u16` (100x miles per hour)
//!
//! When the server detects that a car's average speed exceeded the
//! speed limit between 2 observations, it generates a Ticket message
//! detailing the number plate of the car (plate), the road number of
//! the cameras (road), the positions of the cameras (mile1, mile2),
//! the timestamps of the observations (timestamp1, timestamp2), and
//! the inferred average speed of the car multiplied by 100, and
//! expressed as an integer (speed).
//!
//! mile1 and timestamp1 must refer to the earlier of the 2
//! observations (the smaller timestamp), and mile2 and timestamp2
//! must refer to the later of the 2 observations (the larger
//! timestamp).
//!
//! The server sends the ticket to a dispatcher for the corresponding
//! road.
//!
//! ### Examples:
//!
//! ```raw
//! Hexadecimal:            Decoded:
//! 21                      Ticket{
//! 04 55 4e 31 58              plate: "UN1X",
//! 00 42                       road: 66,
//! 00 64                       mile1: 100,
//! 00 01 e2 40                 timestamp1: 123456,
//! 00 6e                       mile2: 110,
//! 00 01 e3 a8                 timestamp2: 123816,
//! 27 10                       speed: 10000,
//!                         }
//!
//! 21                      Ticket{
//! 07 52 45 30 35 42 4b 47     plate: "RE05BKG",
//! 01 70                       road: 368,
//! 04 d2                       mile1: 1234,
//! 00 0f 42 40                 timestamp1: 1000000,
//! 04 d3                       mile2: 1235,
//! 00 0f 42 7c                 timestamp2: 1000060,
//! 17 70                       speed: 6000,
//!                         }
//! ```
//!
//! ## 0x40: `WantHeartbeat` (Client->Server)
//!
//! Fields:
//!
//! - interval: `u32` (deciseconds)
//!
//! Request heartbeats.
//!
//! The server must now send Heartbeat messages to this client at the
//! given interval, which is specified in "deciseconds", of which
//! there are 10 per second. (So an interval of "25" would mean a
//! Heartbeat message every 2.5 seconds). The heartbeats help to
//! assure the client that the server is still functioning, even in
//! the absence of any other communication.
//!
//! An interval of 0 deciseconds means the client does not want to
//! receive heartbeats (this is the default setting).
//!
//! It is an error for a client to send multiple `WantHeartbeat`
//! messages on a single connection.
//!
//! ### Examples:
//!
//! ```raw
//! Hexadecimal:    Decoded:
//! 40              `WantHeartbeat`{
//! 00 00 00 0a         interval: 10
//!                 }
//!
//! 40              `WantHeartbeat`{
//! 00 00 04 db         interval: 1243
//!                 }
//!
//! ## 0x41: Heartbeat (Server->Client)
//!
//! No fields.
//!
//! Sent to a client at the interval requested by the client.
//!
//! ### Example:
//!
//! ```raw
//! Hexadecimal:    Decoded:
//! 41              Heartbeat{}
//! ```
//!
//! ## 0x80: `IAmCamera` (Client->Server)
//!
//! Fields:
//!
//! - road: `u16`
//! - mile: `u16`
//! - limit: `u16` (miles per hour)
//!
//! This client is a camera. The road field contains the road number
//! that the camera is on, mile contains the position of the camera,
//! relative to the start of the road, and limit contains the speed
//! limit of the road, in miles per hour.
//!
//! It is an error for a client that has already identified itself as
//! either a camera or a ticket dispatcher to send an `IAmCamera`
//! message.
//!
//! ### Examples:
//!
//! ```raw
//! Hexadecimal:    Decoded:
//! 80              `IAmCamera`{
//! 00 42               road: 66,
//! 00 64               mile: 100,
//! 00 3c               limit: 60,
//!                 }
//!
//! 80              `IAmCamera`{
//! 01 70               road: 368,
//! 04 d2               mile: 1234,
//! 00 28               limit: 40,
//!                 }
//! ```
//!
//! ## 0x81: `IAmDispatcher` (Client->Server)
//!
//! Fields:
//!
//! - numroads: `u8`
//! - roads: `[u16]` (array of `u16`)
//!
//! This client is a ticket dispatcher. The numroads field says how
//! many roads this dispatcher is responsible for, and the roads field
//! contains the road numbers.
//!
//! It is an error for a client that has already identified itself as
//! either a camera or a ticket dispatcher to send an `IAmDispatcher`
//! message.
//!
//! ### Examples:
//!
//! ```raw
//! Hexadecimal:    Decoded:
//! 81              `IAmDispatcher`{
//! 01                  roads: [
//! 00 42                   66
//!                     ]
//!                 }
//!
//! 81              `IAmDispatcher`{
//! 03                  roads: [
//! 00 42                   66,
//! 01 70                   368,
//! 13 88                   5000
//!                     ]
//!                 }
//! ```
//!
//! # Example session
//!
//! In this example session, 3 clients connect to the server. Clients
//! 1 & 2 are cameras on road 123, with a 60 mph speed limit. Client 3
//! is a ticket dispatcher for road 123. The car with number plate
//! UN1X was observed passing the first camera at timestamp 0, and
//! passing the second camera 45 seconds later. It travelled 1 mile in
//! 45 seconds, which means it was travelling at 80 mph. This is in
//! excess of the speed limit, so a ticket is dispatched.
//!
//! "-->" denotes messages from the server to the client, and "<--"
//! denotes messages from the client to the server.
//!
//! ## Client 1: camera at mile 8
//!
//! ```raw
//! Hexadecimal:
//! <-- 80 00 7b 00 08 00 3c
//! <-- 20 04 55 4e 31 58 00 00 00 00
//!
//! Decoded:
//! <-- IAmCamera{road: 123, mile: 8, limit: 60}
//! <-- Plate{plate: "UN1X", timestamp: 0}
//! ```
//!
//! ## Client 2: camera at mile 9
//!
//! ```raw
//! Hexadecimal:
//! <-- 80 00 7b 00 09 00 3c
//! <-- 20 04 55 4e 31 58 00 00 00 2d
//!
//! Decoded:
//! <-- IAmCamera{road: 123, mile: 9, limit: 60}
//! <-- Plate{plate: "UN1X", timestamp: 45}
//! ```
//!
//! ## Client 3: ticket dispatcher
//!
//! ```raw
//! Hexadecimal:
//! <-- 81 01 00 7b
//! --> 21 04 55 4e 31 58 00 7b 00 08 00 00 00 00 00 09 00 00 00 2d 1f 40
//!
//! Decoded:
//! <-- IAmDispatcher{roads: [123]}
//! --> Ticket{plate: "UN1X", road: 123, mile1: 8, timestamp1: 0, mile2: 9, timestamp2: 45, speed: 8000}
//! ```
//!
//! # Details
//!
//! ## Dispatchers
//!
//! When the server generates a ticket for a road that has multiple
//! connected dispatchers, the server may choose between them
//! arbitrarily, but must not ever send the same ticket twice.
//!
//! If the server sends a ticket but the dispatcher disconnects before
//! it receives it, then the ticket simply gets lost and the driver
//! escapes punishment.
//!
//! If the server generates a ticket for a road that has no connected
//! dispatcher, it must store the ticket and deliver it once a
//! dispatcher for that road is available.  Unreliable cameras
//!
//! Sometimes number plates aren't spotted (maybe they were obscured,
//! or the image was blurry), so a car can skip one or more cameras
//! and reappear later on. You must still generate a ticket if its
//! average speed exceeded the limit between any pair of observations
//! on the same road, even if the observations were not from adjacent
//! cameras.
//!
//! ## No shortcuts
//!
//! The fastest legal route between any pair of cameras that are on
//! the same road is to use the road that those cameras are on; you
//! don't need to worry about falsely ticketing drivers who may have
//! left a road and rejoined it.
//!
//! ## Only 1 ticket per car per day
//!
//! The server may send no more than 1 ticket for any given car on any
//! given day.
//!
//! Where a ticket spans multiple days, the ticket is considered to
//! apply to every day from the start to the end day, including the
//! end day. This means that where there is a choice of observations
//! to include in a ticket, it is sometimes possible for the server to
//! choose either to send a ticket for each day, or to send a single
//! ticket that spans both days: either behaviour is acceptable. (But
//! to maximise revenues, you may prefer to send as many tickets as
//! possible).
//!
//! Since timestamps do not count leap seconds, days are defined by
//! floor(timestamp / 86400).
//!
//! ## Rounding
//!
//! It is always required to ticket a car that is exceeding the speed
//! limit by 0.5 mph or more
//!
//! In cases where the car is exceeding the speed limit by less than
//! 0.5 mph, it is acceptable to omit the ticket.
//!
//! It is never acceptable to ticket a car that had an average speed
//! below the speed limit.  Overflow
//!
//! In principle, a car travelling in excess of 655.35 mph would cause
//! the server to generate a ticket with an incorrect
//! speed. Fortunately nobody on Freedom Island has a fast enough car,
//! so you don't need to worry about it.
use std::collections::{HashMap, HashSet};
use std::future;
use std::sync::{atomic, Arc, Mutex};
use tokio::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{
    tcp::{ReadHalf, WriteHalf},
    TcpListener, TcpStream,
};
use tokio::sync::mpsc;
use tokio::time;

use tracing::{debug, info, warn};

pub mod controller;
pub mod wire;

use controller::Controller;
use wire::{ReadFrom, TaggedMessage, WriteTo};

enum ControllerMessage {
    AddDispatcher(
        usize,
        HashSet<u16>,
        mpsc::UnboundedSender<controller::Ticket>,
    ),
    RemoveDispatcher(usize),
    Plate(controller::Plate),
}

type Cameras = Arc<Mutex<HashMap<u16, (u16, usize)>>>;

#[derive(Default)]
struct Dispatchers {
    dispatchers: Vec<(
        usize,
        HashSet<u16>,
        mpsc::UnboundedSender<controller::Ticket>,
    )>,
    pending_tickets: Vec<controller::Ticket>,
}

impl Dispatchers {
    fn add_dispatcher(
        &mut self,
        id: usize,
        roads: HashSet<u16>,
        ticket_sender: mpsc::UnboundedSender<controller::Ticket>,
    ) -> Result<(), anyhow::Error> {
        self.dispatchers.push((id, roads, ticket_sender));

        self.send_pending_tickets()
    }

    fn remove_dispatcher(&mut self, removed_id: usize) {
        self.dispatchers.retain(|(id, _, _)| *id != removed_id);
    }

    fn send_tickets(&mut self, mut tickets: Vec<controller::Ticket>) -> Result<(), anyhow::Error> {
        self.pending_tickets.append(&mut tickets);

        self.send_pending_tickets()
    }

    fn send_pending_tickets(&mut self) -> Result<(), anyhow::Error> {
        let mut pending_tickets = vec![];

        for ticket in self.pending_tickets.drain(..) {
            if let Some(sender) = self.dispatchers.iter().find_map(|(_, roads, sender)| {
                if roads.contains(&ticket.road) {
                    Some(sender)
                } else {
                    None
                }
            }) {
                sender.send(ticket)?;
            } else {
                pending_tickets.push(ticket);
            }
        }

        self.pending_tickets.append(&mut pending_tickets);

        Ok(())
    }
}

/// Run the main loop.
///
/// Listen for clients.
///
/// # Errors
/// * Error when socket returns an error.
#[tracing::instrument(skip(listener))]
pub async fn run(listener: TcpListener) -> Result<(), anyhow::Error> {
    let mut controller = Controller::default();
    let mut dispatchers = Dispatchers::default();

    let cameras = Arc::new(Mutex::new(HashMap::new()));

    let (controller_sender, mut controller_receiver) = mpsc::unbounded_channel();

    loop {
        tokio::select! {
            handler = listener.accept() => {
                let (socket, _) = handler?;

                tokio::spawn(handle_client(
                    socket,
                    controller_sender.clone(),
                    cameras.clone(),
                ));
            }

            message = controller_receiver.recv() => {
                match message {
                    Some(ControllerMessage::AddDispatcher(id, roads, ticket_sender)) => {
                        debug!("adding dispatcher {id} roads {roads:?}");
                        dispatchers.add_dispatcher(id, roads, ticket_sender)?;
                    }
                    Some(ControllerMessage::RemoveDispatcher(id)) => {
                        debug!("removing dispatcher {id}");
                        dispatchers.remove_dispatcher(id);
                    }
                    Some(ControllerMessage::Plate(plate)) => {
                        info!("handling plate: {plate:?}");
                        let tickets = controller.signal(plate);
                        debug!("tickets: {tickets:?}");
                        dispatchers.send_tickets(tickets)?;
                    }
                    None => {
                        warn!("empty controller message");
                    }
                }
            }
        }
    }
}

#[tracing::instrument(skip(socket, controller_sender, cameras))]
async fn handle_client(
    mut socket: TcpStream,
    controller_sender: mpsc::UnboundedSender<ControllerMessage>,
    cameras: Cameras,
) {
    let (read, write) = socket.split();
    let mut read = BufReader::new(read);
    let mut write = BufWriter::new(write);

    let handler = async {
        let mut heartbeat = Heartbeat::new(None);
        loop {
            tokio::select! {
                msg = read.read_u8() => {
                    match msg? {
                        wire::IAmCamera::TAG => {
                            return handle_camera(
                                cameras,
                                controller_sender,
                                wire::IAmCamera::read_payload_from(&mut read).await?,
                                heartbeat,
                                &mut read,
                                &mut write,
                            )
                                .await;
                        }
                        wire::IAmDispatcher::TAG => {
                            return handle_dispatcher(
                                controller_sender,
                                wire::IAmDispatcher::read_payload_from(&mut read).await?,
                                heartbeat,
                                &mut read,
                                &mut write,
                            )
                                .await;
                        }
                        wire::WantHeartbeat::TAG if !heartbeat.is_setted() => {
                            let wire::WantHeartbeat { interval: i } =
                                wire::WantHeartbeat::read_payload_from(&mut read).await?;
                            heartbeat.set_period(Duration::from_millis(u64::from(i * 100)));
                        }
                        msg => {
                            warn!("got invalid message: 0x{msg:02x}");
                            return Err(anyhow::anyhow!("invalid message: 0x{msg:02x}"));
                        }
                    }
                }

                _r = heartbeat.tick(), if heartbeat.is_valid() => {
                    info!("sending heartbeat");
                    wire::Heartbeat.write_to(&mut write).await?;
                    write.flush().await?;
                }
            }
        }
    };

    if let Err(err) = handler.await {
        wire::Error {
            msg: err.to_string(),
        }
        .write_to(&mut write)
        .await
        .ok();
        write.flush().await.ok();
        write.shutdown().await.ok();
    }
}

#[derive(Debug)]
struct CameraGuard(Cameras, u16);

impl CameraGuard {
    fn new(cameras: Cameras, road: u16, limit: u16) -> Result<Self, &'static str> {
        {
            let mut cameras = cameras.lock().unwrap();
            let (l, c) = cameras.entry(road).or_insert((limit, 0));
            if *l != limit {
                return Err("limit conflict");
            }
            *c += 1;
        }

        debug!("added camera road {road}");

        Ok(Self(cameras, road))
    }
}

impl Drop for CameraGuard {
    fn drop(&mut self) {
        if let Ok(mut cameras) = self.0.lock() {
            if let Some((_, c)) = cameras.get_mut(&self.1) {
                *c -= 1;
                if *c == 0 {
                    cameras.remove(&self.1);
                    debug!("removed camera road {}", self.1);
                }
            }
        }
    }
}

#[derive(Debug)]
struct DispatcherGuard(mpsc::UnboundedSender<ControllerMessage>, usize);

impl DispatcherGuard {
    fn new(
        controller_sender: mpsc::UnboundedSender<ControllerMessage>,
        roads: Vec<u16>,
        ticket_sender: mpsc::UnboundedSender<controller::Ticket>,
    ) -> Result<Self, anyhow::Error> {
        static IDS: atomic::AtomicUsize = atomic::AtomicUsize::new(0);

        let id = IDS.fetch_add(1, atomic::Ordering::Relaxed);

        controller_sender.send(ControllerMessage::AddDispatcher(
            id,
            roads.into_iter().collect(),
            ticket_sender,
        ))?;

        debug!("added dispatcher {id}");

        Ok(Self(controller_sender, id))
    }
}

impl Drop for DispatcherGuard {
    fn drop(&mut self) {
        if let Err(err) = self.0.send(ControllerMessage::RemoveDispatcher(self.1)) {
            warn!("cannot remove dispatcher: {err:?}");
        }
    }
}

#[derive(Default)]
struct Heartbeat {
    interval: Option<time::Interval>,
    period: Option<Duration>,
}

impl Heartbeat {
    fn new(period: Option<Duration>) -> Self {
        let mut result = Self::default();
        if let Some(period) = period {
            result.set_period(period);
        }
        result
    }

    fn set_period(&mut self, period: Duration) {
        self.period = Some(period);
        if period == Duration::from_millis(0) {
            self.interval = None;
        } else {
            self.interval = Some(time::interval_at(Instant::now() + period, period));
        }
    }

    async fn tick(&mut self) {
        if let Some(interval) = self.interval.as_mut() {
            interval.tick().await;
        } else {
            future::pending::<()>().await;
        }
    }

    fn is_valid(&self) -> bool {
        if let Some(interval) = self.interval.as_ref() {
            interval.period() != Duration::from_millis(0)
        } else {
            false
        }
    }

    fn is_setted(&self) -> bool {
        self.period.is_some()
    }
}

#[tracing::instrument(skip(cameras, controller_sender, heartbeat, read, write))]
async fn handle_camera<'a>(
    cameras: Cameras,
    controller_sender: mpsc::UnboundedSender<ControllerMessage>,
    i_am_camera: wire::IAmCamera,
    mut heartbeat: Heartbeat,
    read: &mut BufReader<ReadHalf<'a>>,
    write: &mut BufWriter<WriteHalf<'a>>,
) -> Result<(), anyhow::Error> {
    debug!("start {i_am_camera:?}");

    let _guard = CameraGuard::new(cameras, i_am_camera.road, i_am_camera.limit)
        .map_err(|e| anyhow::anyhow!("invalid camera: {e}"))?;

    loop {
        tokio::select! {
            msg = read.read_u8() => {
                match msg? {
                    wire::Plate::TAG => {
                        let wire::Plate { plate, timestamp } = wire::Plate::read_payload_from(read).await?;
                        info!("got plate {plate:?}");

                        controller_sender.send(ControllerMessage::Plate(controller::Plate {
                            plate,
                            road: i_am_camera.road,
                            limit: i_am_camera.limit,
                            mile: i_am_camera.mile,
                            timestamp,
                        }))?;
                    }
                    wire::WantHeartbeat::TAG if !heartbeat.is_setted() => {
                        let wire::WantHeartbeat { interval: i } = wire::WantHeartbeat::read_payload_from(read).await?;

                        info!("got want heartbeat {i}");

                        heartbeat.set_period(Duration::from_millis(u64::from(i) * 100));
                    }
                    msg => {
                        return Err(anyhow::anyhow!("invalid msg: 0x{msg:02x}"));
                    }
                }
            }

            _r = heartbeat.tick(), if heartbeat.is_valid() => {
                info!("sending heartbeat");
                wire::Heartbeat.write_to(write).await?;
                write.flush().await?;
            }
        }
    }
}

#[tracing::instrument(skip(controller_sender, heartbeat, read, write))]
async fn handle_dispatcher<'a>(
    controller_sender: mpsc::UnboundedSender<ControllerMessage>,
    i_am_dispatcher: wire::IAmDispatcher,
    mut heartbeat: Heartbeat,
    read: &mut BufReader<ReadHalf<'a>>,
    write: &mut BufWriter<WriteHalf<'a>>,
) -> Result<(), anyhow::Error> {
    debug!("start {i_am_dispatcher:?}");

    let (ticket_sender, mut ticket_receiver) = mpsc::unbounded_channel();

    let _guard = DispatcherGuard::new(controller_sender, i_am_dispatcher.roads, ticket_sender);

    loop {
        tokio::select! {
            msg = read.read_u8() => {
                match msg? {
                    wire::WantHeartbeat::TAG if !heartbeat.is_setted() => {
                        let wire::WantHeartbeat { interval: i } = wire::WantHeartbeat::read_payload_from(read).await?;

                        info!("got want heartbeat {i}");

                        heartbeat.set_period(Duration::from_millis(u64::from(i) * 100));
                    }
                    msg => {
                        return Err(anyhow::anyhow!("invalid msg: 0x{msg:02x}"));
                    }
                }
            }

            _r = heartbeat.tick(), if heartbeat.is_valid() => {
                info!("sending heartbeat");
                wire::Heartbeat.write_to(write).await?;
                write.flush().await?;
            }

            ticket = ticket_receiver.recv() => {
                if let Some(ticket) = ticket {
                    info!("got {ticket:?}");
                    ticket.write_to(write).await?;
                    write.flush().await?;
                } else {
                    warn!("got null ticket");
                    break Ok(());
                }
            }
        }
    }
}
