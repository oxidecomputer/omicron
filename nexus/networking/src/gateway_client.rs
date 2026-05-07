// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use slog::{Logger, o};
use std::net::SocketAddrV6;

#[derive(Clone, Debug)]
pub struct GatewayClient {
    pub addr: SocketAddrV6,
    pub client: gateway_client::Client,
}

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
