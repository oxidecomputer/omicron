// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for keeping track of DNS servers

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::DnsGroup;
use nexus_db_model::ServiceKind;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::DataPageParams;
use std::net::{SocketAddr, SocketAddrV6};
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::watch;

// This constraint could be relaxed by paginating through the list of servers,
// but we don't expect to have this many servers any time soon.
const MAX_DNS_SERVERS: u32 = 10;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DnsServersList {
    pub addresses: Vec<SocketAddr>,
}

/// Background task that keeps track of the latest list of DNS servers for a DNS
/// group
pub struct DnsServersWatcher {
    datastore: Arc<DataStore>,
    dns_group: DnsGroup,
    last: Option<DnsServersList>,
    tx: watch::Sender<Option<DnsServersList>>,
    rx: watch::Receiver<Option<DnsServersList>>,
}

impl DnsServersWatcher {
    pub fn new(
        datastore: Arc<DataStore>,
        dns_group: DnsGroup,
    ) -> DnsServersWatcher {
        let (tx, rx) = watch::channel(None);
        DnsServersWatcher { datastore, dns_group, last: None, tx, rx }
    }

    /// Exposes the latest list of DNS servers for this DNS group
    ///
    /// You can use the returned [`watch::Receiver`] to look at the latest
    /// list of servers or to be notified when it changes.
    pub fn watcher(&self) -> watch::Receiver<Option<DnsServersList>> {
        self.rx.clone()
    }
}

impl BackgroundTask for DnsServersWatcher {
    fn activate<'a, 'b, 'c>(
        &'a mut self,
        opctx: &'b OpContext,
    ) -> BoxFuture<'c, ()>
    where
        'a: 'c,
        'b: 'c,
    {
        async {
            // Set up a logger for this activation that includes metadata about
            // the current generation.
            let log = match &self.last {
                None => opctx.log.clone(),
                Some(old) => {
                    let old_addrs_dbg = format!("{:?}", old);
                    opctx.log.new(o!(
                        "current_servers" => old_addrs_dbg,
                    ))
                }
            };

            // Read the latest configuration for this DNS group.
            let service_kind = match self.dns_group {
                DnsGroup::Internal => ServiceKind::InternalDNSConfig,
                DnsGroup::External => ServiceKind::ExternalDNSConfig,
            };

            let pagparams = DataPageParams {
                marker: None,
                limit: NonZeroU32::try_from(MAX_DNS_SERVERS).unwrap(),
                direction: dropshot::PaginationOrder::Ascending,
            };

            let result = self
                .datastore
                .services_list_kind(opctx, service_kind, &pagparams)
                .await;

            if let Err(error) = result {
                // XXX-dap is this error reporting going to suck if it's an
                // internal error?
                warn!(
                    &log,
                    "failed to read list of DNS servers";
                    "error" => format!("{:#}", error)
                );
                return;
            }

            let services = result.unwrap();
            if services.len() >= usize::try_from(MAX_DNS_SERVERS).unwrap() {
                warn!(
                    &log,
                    "found {} servers, which is more than MAX_DNS_SERVERS \
                    ({}).  There may be more that will not be used.",
                    services.len(),
                    MAX_DNS_SERVERS
                );
            }

            let new_config = DnsServersList {
                addresses: services
                    .into_iter()
                    .map(|s| SocketAddrV6::new(*s.ip, *s.port, 0, 0).into())
                    .collect(),
            };
            let new_addrs_dbg = format!("{:?}", new_config);

            match &self.last {
                None => {
                    info!(
                        &log,
                        "found DNS servers (initial)";
                        "addresses" => new_addrs_dbg,
                    );
                    self.last = Some(new_config.clone());
                    self.tx.send_replace(Some(new_config));
                }

                Some(old) => {
                    // The datastore should be sorting the DNS servers by id in
                    // order to paginate through them.  Thus, it should be valid
                    // to compare what we got directly to what we had before
                    // without worrying about the order being different.
                    if *old == new_config {
                        debug!(
                            &log,
                            "found DNS servers (no change)";
                            "addresses" => new_addrs_dbg,
                        );
                    } else {
                        info!(
                            &log,
                            "found DNS servers (changed)";
                            "addresses" => new_addrs_dbg,
                        );
                        self.last = Some(new_config.clone());
                        self.tx.send_replace(Some(new_config));
                    }
                }
            };
        }
        .boxed()
    }
}
