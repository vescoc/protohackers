use std::collections::HashMap;

use futures::{Sink, SinkExt, Stream, StreamExt, channel::mpsc};

use thiserror::Error;

use tracing::{debug, instrument, warn};

use crate::codec::{self, packets};

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid packet: {0}")]
    InvalidPacket(&'static str),

    #[error("packet receiving error: {0}")]
    Receiving(#[from] codec::Error),

    #[error("packet sending error: {0}")]
    Sending(codec::Error),

    #[error("controller error: {0}")]
    Controller(#[from] mpsc::SendError),
}

pub struct SiteVisitor<U, D> {
    upstream: U,
    downstream: D,
    controller: mpsc::Sender<packets::site_visit::Packet>,
}

impl<U, D> SiteVisitor<U, D> {
    pub fn new(
        upstream: U,
        downstream: D,
        controller: mpsc::Sender<packets::site_visit::Packet>,
    ) -> Self {
        Self {
            upstream,
            downstream,
            controller,
        }
    }
}

impl<U, D, UE> SiteVisitor<U, D>
where
    U: Stream<Item = Result<packets::Packet, UE>> + Unpin,
    UE: Into<codec::Error>,
    D: Sink<packets::Packet> + Unpin,
    Error: From<D::Error>,
{
    #[instrument(skip_all)]
    pub async fn run(mut self) {
        debug!("run");

        if let Err(err) = self.handle_hello().await {
            warn!("got error on handle hello phase: {err}");
            self.downstream
                .send(packets::error::Packet::new(err.to_string()).into())
                .await
                .ok();
            return;
        }

        if let Err(err) = self.handle_visits().await {
            warn!("got error on handle visits phase: {err}");
            self.downstream
                .send(packets::error::Packet::new(err.to_string()).into())
                .await
                .ok();
        }
    }

    #[instrument(skip_all)]
    async fn handle_hello(&mut self) -> Result<(), Error> {
        self.downstream
            .send(packets::hello::Packet::new().into())
            .await?;

        match self.upstream.next().await {
            Some(Ok(packets::Packet::Hello(packets::hello::Packet { protocol, version })))
                if protocol == packets::hello::PESTCONTROL_PROTOCOL
                    && version == packets::hello::PESTCONTROL_VERSION =>
            {
                Ok(())
            }
            Some(Err(err)) => Err(Error::Receiving(err.into())),
            Some(Ok(packet)) => {
                warn!("invalid packet: {packet:?}");
                return Err(Error::InvalidPacket("got invalid packet"));
            }
            _ => Err(Error::InvalidPacket("waiting hello msg")),
        }
    }

    #[instrument(skip_all)]
    async fn handle_visits(&mut self) -> Result<(), Error> {
        loop {
            match self.upstream.next().await {
                Some(Ok(packets::Packet::SiteVisit(packet))) => {
                    debug!("got packet: {packet:?}");
                    let mut map = HashMap::with_capacity(packet.populations.len());
                    for packets::site_visit::Population { species, count } in &packet.populations {
                        if let Some(c) = map.get(species)
                            && *c != count
                        {
                            warn!("got conflict {species} {c} != {count}");
                            return Err(Error::InvalidPacket("conflicts"));
                        }
                        map.insert(species.clone(), count);
                    }

                    self.controller.send(packet).await?;
                }
                Some(Ok(packet)) => {
                    warn!("invalid packet: {packet:?}");
                    return Err(Error::InvalidPacket("got invalid packet"));
                }
                Some(Err(err)) => return Err(Error::Receiving(err.into())),
                None => break Ok(()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::sink;
    use futures::stream;

    use wasi_async::time::timeout;
    use wasi_async_runtime::Reactor;

    use crate::tests::{TIMEOUT, init_tracing_subscriber};

    use super::*;

    #[test]
    fn test_hello() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let (client_tx, mut client_rx) = mpsc::unbounded();

            let upstream = Box::pin(stream::once(async {
                Ok::<_, codec::Error>(packets::hello::Packet::new().into())
            }));

            let downstream = Box::pin(sink::unfold(
                client_tx,
                |mut client_tx, packet| async move {
                    client_tx.send(packet).await.unwrap();
                    Ok::<_, codec::Error>(client_tx)
                },
            ));
            let (controller, _) = mpsc::channel(1);

            let site_visitor = SiteVisitor::new(upstream, downstream, controller);

            site_visitor.run().await;

            assert_eq!(
                timeout(TIMEOUT, client_rx.recv()).await.unwrap().unwrap(),
                packets::hello::Packet::new().into()
            );
        });
    }

    #[test]
    fn test_session() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let (client_tx, mut client_rx) = mpsc::unbounded();

            let upstream = Box::pin(stream::iter(vec![
                Ok::<_, codec::Error>(packets::hello::Packet::new().into()),
                Ok(packets::site_visit::Packet {
                    site: 12354,
                    populations: vec![packets::site_visit::Population {
                        species: "long-tailed-rat".to_string(),
                        count: 20,
                    }],
                }
                .into()),
            ]));

            let downstream = Box::pin(sink::unfold(
                client_tx,
                |mut client_tx, packet| async move {
                    client_tx.send(packet).await.unwrap();
                    Ok::<_, codec::Error>(client_tx)
                },
            ));
            let (controller, mut controller_tx) = mpsc::channel(1);

            let site_visitor = SiteVisitor::new(upstream, downstream, controller);

            site_visitor.run().await;

            assert_eq!(
                timeout(TIMEOUT, client_rx.recv()).await.unwrap().unwrap(),
                packets::hello::Packet::new().into()
            );

            assert_eq!(
                packets::site_visit::Packet {
                    site: 12354,
                    populations: vec![packets::site_visit::Population {
                        species: "long-tailed-rat".to_string(),
                        count: 20
                    }]
                },
                timeout(TIMEOUT, controller_tx.recv())
                    .await
                    .unwrap()
                    .unwrap()
            );
        });
    }

    #[test]
    fn test_invalid_packet() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let (client_tx, mut client_rx) = mpsc::unbounded();

            let upstream = Box::pin(stream::iter(vec![
                Ok::<_, codec::Error>(packets::hello::Packet::new().into()),
                Ok(packets::ok::Packet.into()),
            ]));

            let downstream = Box::pin(sink::unfold(
                client_tx,
                |mut client_tx, packet| async move {
                    client_tx.send(packet).await.unwrap();
                    Ok::<_, codec::Error>(client_tx)
                },
            ));
            let (controller, _controller_tx) = mpsc::channel(1);

            let site_visitor = SiteVisitor::new(upstream, downstream, controller);

            site_visitor.run().await;

            assert_eq!(
                timeout(TIMEOUT, client_rx.recv()).await.unwrap().unwrap(),
                packets::hello::Packet::new().into(),
            );
            assert_eq!(
                timeout(TIMEOUT, client_rx.recv()).await.unwrap().unwrap(),
                packets::error::Packet::new("invalid packet: got invalid packet").into()
            );

            assert!(timeout(TIMEOUT, client_rx.recv()).await.unwrap().is_err());
        });
    }
}
