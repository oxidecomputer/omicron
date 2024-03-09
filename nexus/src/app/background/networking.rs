use omicron_common::address::DENDRITE_PORT;
use omicron_common::{address::MGD_PORT, api::external::SwitchLocation};
use std::{collections::HashMap, net::SocketAddrV6};

pub(crate) fn build_mgd_clients(
    mappings: HashMap<SwitchLocation, std::net::Ipv6Addr>,
    log: &slog::Logger,
) -> HashMap<SwitchLocation, mg_admin_client::Client> {
    let mut clients: Vec<(SwitchLocation, mg_admin_client::Client)> = vec![];
    for (location, addr) in &mappings {
        let port = MGD_PORT;
        let socketaddr =
            std::net::SocketAddr::V6(SocketAddrV6::new(*addr, port, 0, 0));
        let client =
            match mg_admin_client::Client::new(&log.clone(), socketaddr) {
                Ok(client) => client,
                Err(e) => {
                    error!(
                        log,
                        "error building mgd client";
                        "location" => %location,
                        "addr" => %addr,
                        "error" => %e,
                    );
                    continue;
                }
            };
        clients.push((*location, client));
    }
    clients.into_iter().collect::<HashMap<_, _>>()
}

pub(crate) fn build_dpd_clients(
    mappings: &HashMap<SwitchLocation, std::net::Ipv6Addr>,
    log: &slog::Logger,
) -> HashMap<SwitchLocation, dpd_client::Client> {
    let dpd_clients: HashMap<SwitchLocation, dpd_client::Client> = mappings
        .iter()
        .map(|(location, addr)| {
            let port = DENDRITE_PORT;

            let client_state = dpd_client::ClientState {
                tag: String::from("nexus"),
                log: log.new(o!(
                    "component" => "DpdClient"
                )),
            };

            let dpd_client = dpd_client::Client::new(
                &format!("http://[{addr}]:{port}"),
                client_state,
            );
            (*location, dpd_client)
        })
        .collect();
    dpd_clients
}
