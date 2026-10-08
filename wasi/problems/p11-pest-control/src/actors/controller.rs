use std::collections::HashMap;
use std::fmt;

use futures::channel::mpsc;

use tracing::{debug, info, instrument, warn};

use wasi_async_runtime::Reactor;

use crate::actors::Provider;
use crate::actors::authority_server::AuthorityServer;
use crate::codec::packets;

pub struct Controller<P> {
    authority_server_provider: P,
    site_visits: mpsc::Receiver<packets::site_visit::Packet>,
    authority_servers: HashMap<u32, mpsc::UnboundedSender<Vec<packets::site_visit::Population>>>,
}

impl<P> fmt::Debug for Controller<P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Controller")
    }
}

impl<P> Controller<P> {
    pub fn new(
        authority_server_provider: P,
        site_visits: mpsc::Receiver<packets::site_visit::Packet>,
    ) -> Self {
        Self {
            authority_server_provider,
            site_visits,
            authority_servers: HashMap::new(),
        }
    }
}

impl<P: Provider + Clone + 'static> Controller<P> {
    #[instrument]
    pub async fn run(mut self) {
        info!("run start");

        let reactor = Reactor::current().await;

        while let Ok(packets::site_visit::Packet { site, populations }) =
            self.site_visits.recv().await
        {
            debug!("got populations: {populations:?}");

            let authority_server = self
                .authority_servers
                .entry(site)
                .or_insert_with_key(|site| {
                    let authority_server_provider = self.authority_server_provider.clone();
                    let (authority_server_tx, authority_server_rx) = mpsc::unbounded();

                    let authority_server =
                        AuthorityServer::new(*site, authority_server_provider, authority_server_rx);
                    reactor.spawn(async move { authority_server.run().await });

                    authority_server_tx
                });

            if let Err(err) = authority_server.unbounded_send(populations) {
                warn!("cannot send to site {site}: {err}");
                self.authority_servers.remove(&site);
            }
        }

        info!("run done");
    }
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt, StreamExt};

    use wasi_async::time::timeout;

    use crate::actors::tests::TestProvider;
    use crate::tests::{TIMEOUT, init_tracing_subscriber};

    use super::*;

    #[test]
    #[instrument]
    fn test_session() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async {
            let (mut endpoints, provider) = TestProvider::new();

            let (mut site_visits, site_visits_rx) = mpsc::channel(1);

            let controller = Controller::new(provider, site_visits_rx);

            Reactor::spawn_in_current(controller.run()).await;

            site_visits
                .send(packets::site_visit::Packet::new(
                    12345,
                    vec![packets::site_visit::Population::new("dog", 10)],
                ))
                .await
                .unwrap();

            let Ok((12345, mut upstream, mut downstream)) = endpoints.recv().await else {
                panic!("invalid endpoints")
            };

            assert_eq!(
                timeout(TIMEOUT, upstream.next()).await.unwrap().unwrap(),
                packets::hello::Packet::new().into(),
            );

            downstream
                .send(Ok(packets::hello::Packet::new().into()))
                .await
                .unwrap();

            assert_eq!(
                packets::Packet::DialAuthority(packets::dial_authority::Packet::new(12345)),
                timeout(TIMEOUT, upstream.next()).await.unwrap().unwrap()
            );
        });
    }
}
