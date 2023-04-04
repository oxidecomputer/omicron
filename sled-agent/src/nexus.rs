// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[cfg(test)]
pub use crate::mocks::MockNexusClient as NexusClient;
#[cfg(not(test))]
pub use nexus_client::Client as NexusClient;

use internal_dns::resolver::{ResolveError, Resolver};
use internal_dns::ServiceName;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use slog::Logger;
use std::future::Future;
use std::net::Ipv6Addr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

struct Inner {
    log: Logger,
    resolver: Resolver,
}

/// Wrapper around a [`NexusClient`] object, which allows deferring
/// the DNS lookup until accessed.
///
/// Without the assistance of OS-level DNS lookups, the [`NexusClient`]
/// interface requires knowledge of the target service IP address.
/// For some services, like Nexus, this can be painful, as the IP address
/// may not have even been allocated when the Sled Agent starts.
///
/// This structure allows clients to access the client on-demand, performing
/// the DNS lookup only once it is actually needed.
#[derive(Clone)]
pub struct LazyNexusClient {
    inner: Arc<Inner>,
}

impl LazyNexusClient {
    pub fn new(log: Logger, addr: Ipv6Addr) -> Result<Self, ResolveError> {
        Ok(Self {
            inner: Arc::new(Inner {
                log: log.clone(),
                resolver: Resolver::new_from_ip(
                    log.new(o!("component" => "DnsResolver")),
                    addr,
                )?,
            }),
        })
    }

    pub async fn get_ip(&self) -> Result<Ipv6Addr, ResolveError> {
        self.inner.resolver.lookup_ipv6(ServiceName::Nexus).await
    }

    pub async fn get(&self) -> Result<NexusClient, ResolveError> {
        let address = self.get_ip().await?;

        Ok(NexusClient::new(
            &format!("http://[{}]:{}", address, NEXUS_INTERNAL_PORT),
            self.inner.log.clone(),
        ))
    }
}

type NexusRequestFut = dyn Future<Output = ()> + Send;
type NexusRequest = Pin<Box<NexusRequestFut>>;

/// A queue of futures which represent requests to Nexus.
pub struct NexusRequestQueue {
    tx: mpsc::UnboundedSender<NexusRequest>,
    _worker: JoinHandle<()>,
}

impl NexusRequestQueue {
    /// Creates a new request queue, along with a worker which executes
    /// any incoming tasks.
    pub fn new() -> Self {
        // TODO(https://github.com/oxidecomputer/omicron/issues/1917):
        // In the future, this should basically just be a wrapper around a
        // generation number, and we shouldn't be serializing requests to Nexus.
        //
        // In the meanwhile, we're using an unbounded_channel for simplicity, so
        // that we don't need to cope with dropped notifications /
        // retransmissions.
        let (tx, mut rx) = mpsc::unbounded_channel();

        let _worker = tokio::spawn(async move {
            while let Some(fut) = rx.recv().await {
                fut.await;
            }
        });

        Self { tx, _worker }
    }

    /// Gets access to the sending portion of the request queue.
    ///
    /// Callers can use this to add their own requests.
    pub fn sender(&self) -> &mpsc::UnboundedSender<NexusRequest> {
        &self.tx
    }
}

pub fn d2n_params(
    params: &dns_service_client::types::DnsConfigParams,
) -> nexus_client::types::DnsConfigParams {
    nexus_client::types::DnsConfigParams {
        generation: params.generation,
        time_created: params.time_created,
        zones: params.zones.iter().map(d2n_zone).collect(),
    }
}

fn d2n_zone(
    zone: &dns_service_client::types::DnsConfigZone,
) -> nexus_client::types::DnsConfigZone {
    nexus_client::types::DnsConfigZone {
        zone_name: zone.zone_name.clone(),
        records: zone
            .records
            .iter()
            .map(|(n, r)| (n.clone(), r.iter().map(d2n_record).collect()))
            .collect(),
    }
}

fn d2n_record(
    record: &dns_service_client::types::DnsRecord,
) -> nexus_client::types::DnsRecord {
    match record {
        dns_service_client::types::DnsRecord::Aaaa(addr) => {
            nexus_client::types::DnsRecord::Aaaa(*addr)
        }
        dns_service_client::types::DnsRecord::Srv(srv) => {
            nexus_client::types::DnsRecord::Srv(nexus_client::types::Srv {
                port: srv.port,
                prio: srv.prio,
                target: srv.target.clone(),
                weight: srv.weight,
            })
        }
    }
}
