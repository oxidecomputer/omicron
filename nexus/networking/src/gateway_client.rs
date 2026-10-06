// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::backoff::{self, BackoffError};
use omicron_uuid_kinds::RackUuid;
use parallel_task_set::ParallelTaskSet;
use slog::{Logger, o};
use slog_error_chain::InlineErrorChain;
use std::net::SocketAddrV6;

#[derive(Clone, Debug)]
pub struct GatewayClient {
    pub addr: SocketAddrV6,
    pub client: gateway_client::Client,
}

pub type ClientError = gateway_client::Error<gateway_client::types::Error>;

impl GatewayClient {
    pub fn from_addr(log: &Logger, addr: SocketAddrV6) -> Self {
        let url = format!("http://{addr}");
        let log = log
            .new(o!("gateway_url" => url.clone(), "addr" => addr.to_string()));
        let client = gateway_client::Client::new(&url, log);
        GatewayClient { addr, client }
    }

    pub async fn resolve_all_gateways<'l>(
        log: &'l Logger,
        resolver: &internal_dns_resolver::Resolver,
    ) -> Result<impl Iterator<Item = Self> + 'l, anyhow::Error> {
        let addrs = resolver
            .lookup_all_socket_v6(
                internal_dns_types::names::ServiceName::ManagementGatewayService,
            )
            .await?;

        anyhow::ensure!(!addrs.is_empty(), "no MGS addresses resolved");

        Ok(addrs.into_iter().map(move |addr| Self::from_addr(log, addr)))
    }
}

/// A map of [`RackUuid`]s to [`GatewayClient`]s.
#[derive(Debug)]
pub struct GatewaysByRack {
    by_rack: iddqd::IdHashMap<RackGateways>,
    unknown: Vec<(GatewayClient, ClientError)>,
}

impl GatewaysByRack {
    pub async fn resolve_all_gateways(
        log: &Logger,
        resolver: &internal_dns_resolver::Resolver,
    ) -> Result<Self, anyhow::Error> {
        let gateways =
            GatewayClient::resolve_all_gateways(log, resolver).await?;

        // Now that we've resolved all the gateways, ask them what racks they
        // are part of. We'll do this in parallel, with a concurrency limit. The
        // default limit from `ParallelTaskSet` ought to be plenty.
        let mut tasks = ParallelTaskSet::new();
        let mut this =
            Self { by_rack: iddqd::IdHashMap::default(), unknown: Vec::new() };
        for gateway in gateways {
            let log = log.clone();
            let joined = tasks
                .spawn(async move {
                    const MAX_TRIES: usize = 3;
                    let mut tries = 0;
                    let client = &gateway.client;
                    let rack_id = backoff::retry(
                        backoff::retry_policy_internal_service(),
                        || async move {
                            client
                                .rack_id_get()
                                .await
                                .map(|rsp| rsp.into_inner().rack_id)
                                .map_err(|e| {
                                    if tries >= MAX_TRIES {
                                        BackoffError::permanent(e)
                                    } else {
                                        tries += 1;
                                        BackoffError::transient(e)
                                    }
                                })
                        },
                    )
                    .await;
                    match rack_id {
                        Ok(rack_id) => (Ok(rack_id), gateway),
                        Err(e) => {
                            slog::warn!(
                                log,
                                "failed to determine rack ID for resolved MGS \
                                 after {MAX_TRIES} attempts";
                                "error" => InlineErrorChain::new(&e),
                                "gateway_addr" => %gateway.addr,
                            );
                            (Err(e), gateway)
                        }
                    }
                })
                .await;
            if let Some((maybe_id, gateway)) = joined {
                this.insert_discovery_result(maybe_id, gateway);
            }
        }

        // wait for the last set of tasks to come back...
        while let Some((maybe_id, gateway)) = tasks.join_next().await {
            this.insert_discovery_result(maybe_id, gateway);
        }

        anyhow::ensure!(
            !this.by_rack.is_empty(),
            "no gateways had their rack IDs discovered ({} were resolved but \
             have unknown rack IDs)",
            this.unknown.len()
        );

        Ok(this)
    }

    fn insert_discovery_result(
        &mut self,
        rack_id: Result<RackUuid, ClientError>,
        gateway: GatewayClient,
    ) {
        match rack_id {
            Ok(rack_id) => {
                self.by_rack
                    .entry(&rack_id)
                    .or_insert_with(|| RackGateways {
                        rack_id,
                        gateways: Vec::new(),
                    })
                    .gateways
                    .push(gateway);
            }
            Err(e) => {
                self.unknown.push((gateway, e));
            }
        }
    }

    /// Borrows the discovered gateway clients for the given rack ID, if any
    /// were discovered.
    pub fn for_rack(&self, rack_id: &RackUuid) -> Option<&[GatewayClient]> {
        self.by_rack.get(rack_id).map(|r| &r.gateways[..])
    }

    /// Returns an iterator over all racks for which one or more gateway client
    /// was discovered.
    ///
    /// Note that the iteration order here is intentionally non-deterministic,
    /// since the clients are stored in an `iddqd::IdHashMap` keyed by the rack
    /// ID. In general, we try to use deterministic ordering for maps. Here, we
    /// *intentionally* do the opposite, since this is used by background tasks
    /// that perform operations on all racks in the cluster, and we would like
    /// these background tasks to start at different positions to evenly
    /// distribute load.
    pub fn all_discovered(&self) -> impl Iterator<Item = &RackGateways> + '_ {
        self.by_rack.iter()
    }

    /// Borrows the set of resolved clients for which the rack ID is unknown,
    /// along with the last error encountered while trying to discover the rack
    /// ID.
    pub fn unknown(&self) -> &[(GatewayClient, ClientError)] {
        &self.unknown
    }
}

#[derive(Debug)]
pub struct RackGateways {
    pub rack_id: RackUuid,
    pub gateways: Vec<GatewayClient>,
}

impl iddqd::IdHashItem for RackGateways {
    type Key<'k> = &'k RackUuid;

    fn key(&self) -> Self::Key<'_> {
        &self.rack_id
    }

    iddqd::id_upcast! {}
}
