// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Asset;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use slog::{Logger, o};
use std::net::SocketAddrV6;

#[derive(Clone, Debug)]
pub struct GatewayClient {
    pub addr: SocketAddrV6,
    pub client: gateway_client::Client,
    pub rack_id: RackUuid,
}

impl GatewayClient {
    pub fn from_addr(
        log: &Logger,
        addr: SocketAddrV6,
        rack_id: RackUuid,
    ) -> Self {
        let url = format!("http://{addr}");
        let log = log.new(o!(
            "gateway_url" => url.clone(),
            "addr" => addr.to_string(),
            "rack_id" => rack_id.to_string()
        ));
        let client = gateway_client::Client::new(&url, log);
        GatewayClient { addr, client, rack_id }
    }

    pub async fn resolve_all_gateways(
        datastore: &DataStore,
        resolver: &internal_dns_resolver::Resolver,
        opctx: &OpContext,
    ) -> Result<Vec<Self>, anyhow::Error> {
        let addrs = resolver
            .lookup_all_socket_v6(
                internal_dns_types::names::ServiceName::ManagementGatewayService,
            )
            .await?;

        anyhow::ensure!(!addrs.is_empty(), "no MGS addresses resolved");

        let mut clients = Vec::with_capacity(addrs.len());
        for addr in addrs {
            let Some(rack) = datastore
                .rack_lookup_by_ip(opctx, *addr.ip())
                .await
                .with_context(|| {
                    format!("failed to query rack ID for MGS address {addr}")
                })?
            else {
                slog::error!(
                    opctx.log,
                    "resolved a MGS address that does not correspond to a \
                     known rack subnet; ignoring it";
                    "mgs_addr" => %addr,
                );
                continue;
            };
            let rack_id = RackUuid::from_untyped_uuid(rack.identity().id);
            clients.push(Self::from_addr(&opctx.log, addr, rack_id))
        }

        Ok(clients)
    }
}
