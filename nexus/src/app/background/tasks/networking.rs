// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use db::datastore::SwitchPortSettingsCombinedResult;
use dpd_client::types::{
    LinkCreate, LinkId, LinkSettings, PortFec, PortSettings, PortSpeed, TxEq,
};
use nexus_db_model::{SwitchLinkFec, SwitchLinkSpeed};
use nexus_db_queries::db;
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
        let client = mg_admin_client::Client::new(
            format!("http://{}", socketaddr).as_str(),
            log.clone(),
        );
        clients.push((*location, client));
    }
    clients.into_iter().collect::<HashMap<_, _>>()
}

pub(crate) fn api_to_dpd_port_settings(
    settings: &SwitchPortSettingsCombinedResult,
) -> Result<PortSettings, String> {
    let mut dpd_port_settings = PortSettings { links: HashMap::new() };

    //TODO breakouts
    let link_id = LinkId(0);
    let tx_eq = if let Some(t) = settings.tx_eq.get(0) {
        Some(TxEq {
            pre1: t.pre1,
            pre2: t.pre2,
            main: t.main,
            post2: t.post2,
            post1: t.post2,
        })
    } else {
        None
    };
    for l in settings.links.iter() {
        dpd_port_settings.links.insert(
            link_id.to_string(),
            LinkSettings {
                params: LinkCreate {
                    autoneg: l.autoneg,
                    lane: Some(LinkId(0)),
                    kr: false,
                    tx_eq: tx_eq.clone(),
                    fec: l.fec.map(|fec| match fec {
                        SwitchLinkFec::Firecode => PortFec::Firecode,
                        SwitchLinkFec::Rs => PortFec::Rs,
                        SwitchLinkFec::None => PortFec::None,
                    }),
                    speed: match l.speed {
                        SwitchLinkSpeed::Speed0G => PortSpeed::Speed0G,
                        SwitchLinkSpeed::Speed1G => PortSpeed::Speed1G,
                        SwitchLinkSpeed::Speed10G => PortSpeed::Speed10G,
                        SwitchLinkSpeed::Speed25G => PortSpeed::Speed25G,
                        SwitchLinkSpeed::Speed40G => PortSpeed::Speed40G,
                        SwitchLinkSpeed::Speed50G => PortSpeed::Speed50G,
                        SwitchLinkSpeed::Speed100G => PortSpeed::Speed100G,
                        SwitchLinkSpeed::Speed200G => PortSpeed::Speed200G,
                        SwitchLinkSpeed::Speed400G => PortSpeed::Speed400G,
                    },
                },
                //TODO won't work for breakouts
                addrs: settings
                    .addresses
                    .iter()
                    .map(|a| a.address.addr())
                    .collect(),
            },
        );
    }

    Ok(dpd_port_settings)
}
