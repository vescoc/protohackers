use std::future::Future;

use futures::{Sink, Stream};

use crate::codec::{self, packets};

pub mod authority_server;
pub mod controller;
pub mod site_visitor;

pub trait Provider {
    type Sink: Sink<packets::Packet> + Unpin;
    type Stream: Stream<Item = Result<packets::Packet, codec::Error>> + Unpin;
    type Error: std::error::Error;

    /// # Errors
    fn connect(
        &mut self,
        site: u32,
    ) -> impl Future<Output = Result<(Self::Sink, Self::Stream), Self::Error>>;
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc;

    use crate::actors::authority_server::Error;

    use super::*;

    pub(crate) type TestProviderClient = (
        u32,
        mpsc::UnboundedReceiver<packets::Packet>,
        mpsc::UnboundedSender<Result<packets::Packet, codec::Error>>,
    );

    #[derive(Clone)]
    pub(crate) struct TestProvider(mpsc::UnboundedSender<TestProviderClient>);

    impl TestProvider {
        pub(crate) fn new() -> (mpsc::UnboundedReceiver<TestProviderClient>, Self) {
            let (tx, rx) = mpsc::unbounded();

            (rx, Self(tx))
        }
    }

    impl Provider for TestProvider {
        type Sink = mpsc::UnboundedSender<packets::Packet>;
        type Stream = mpsc::UnboundedReceiver<Result<packets::Packet, codec::Error>>;
        type Error = Error;

        fn connect(
            &mut self,
            site: u32,
        ) -> impl Future<Output = Result<(Self::Sink, Self::Stream), Self::Error>> {
            let (server_upstream, upstream) = mpsc::unbounded();
            let (downstream, server_downstream) = mpsc::unbounded();

            self.0
                .unbounded_send((site, upstream, downstream))
                .expect("cannot send endpoints");

            std::future::ready(Ok((server_upstream, server_downstream)))
        }
    }
}
