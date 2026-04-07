// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use internal_dns_types::names::ServiceName;
use omicron_common::address::MGD_PORT;
use sled_agent_types::early_networking::SwitchSlot;
use std::{
    collections::HashMap,
    net::{Ipv6Addr, SocketAddrV6},
};

pub(crate) async fn build_mgd_clients(
    mappings: HashMap<SwitchSlot, std::net::Ipv6Addr>,
    log: &slog::Logger,
    resolver: &internal_dns_resolver::Resolver,
) -> HashMap<SwitchSlot, mg_admin_client::Client> {
    let mut clients: Vec<(SwitchSlot, mg_admin_client::Client)> = vec![];
    for (switch_slot, addr) in &mappings {
        let port = match resolver.lookup_all_socket_v6(ServiceName::Mgd).await {
            Ok(addrs) => {
                let port_map: HashMap<Ipv6Addr, u16> = addrs
                    .into_iter()
                    .map(|sockaddr| (*sockaddr.ip(), sockaddr.port()))
                    .collect();

                *port_map.get(&addr).unwrap_or(&MGD_PORT)
            }
            Err(e) => {
                error!(log, "failed to  addresses"; "error" => %e);
                MGD_PORT
            }
        };

        let socketaddr =
            std::net::SocketAddr::V6(SocketAddrV6::new(*addr, port, 0, 0));
        let client = mg_admin_client::Client::new(
            format!("http://{}", socketaddr).as_str(),
            log.clone(),
        );
        clients.push((*switch_slot, client));
    }
    clients.into_iter().collect::<HashMap<_, _>>()
}
