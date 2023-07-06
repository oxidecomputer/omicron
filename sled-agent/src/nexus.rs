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
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A thin wrapper over a progenitor-generated NexusClient.
///
/// Also attaches the "DNS resolver" for historical reasons.
#[derive(Clone)]
pub struct NexusClientWithResolver {
    client: NexusClient,
    resolver: Arc<Resolver>,
}

impl NexusClientWithResolver {
    pub fn new(
        log: &Logger,
        resolver: Arc<Resolver>,
    ) -> Result<Self, ResolveError> {
        let client = reqwest::ClientBuilder::new()
            .dns_resolver(resolver.clone())
            .build()
            .expect("Failed to build client");

        let dns_name = ServiceName::Nexus.srv_name();
        Ok(Self {
            client: NexusClient::new_with_client(
                &format!("http://{dns_name}:{NEXUS_INTERNAL_PORT}"),
                client,
                log.new(o!("component" => "NexusClient")),
            ),
            resolver,
        })
    }

    /// Access the progenitor-based Nexus Client.
    pub fn client(&self) -> &NexusClient {
        &self.client
    }

    /// Access the DNS resolver used by the Nexus Client.
    ///
    /// WARNING: If you're using this resolver to access an IP address of
    /// another service, be aware that it might change if that service moves
    /// around! Be cautious when accessing and persisting IP addresses of other
    /// services.
    pub fn resolver(&self) -> &Arc<Resolver> {
        &self.resolver
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
        dns_service_client::types::DnsRecord::A(addr) => {
            nexus_client::types::DnsRecord::A(*addr)
        }
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
