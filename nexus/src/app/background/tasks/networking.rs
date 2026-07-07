// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use db::datastore::SwitchPortSettingsCombinedResult;
use dpd_client::types::{
    LinkCreate, LinkId, LinkSettings, PortFec, PortSettings, PortSpeed, TxEq,
};
use internal_dns_resolver::ResolveError;
use internal_dns_types::names::ServiceName;
use nexus_db_model::{SwitchLinkFec, SwitchLinkSpeed};
use nexus_db_queries::db;
use sled_agent_types::early_networking::SwitchSlot;
use slog_error_chain::InlineErrorChain;
use std::collections::HashMap;

/// Get all MGD known `SwitchSlot -> MGD client` pairs.
///
/// # Errors
///
/// Fails if we cannot resolve MGD in DNS.
///
/// For any MGD instance we resolve via DNS, if the MGD instance does not know
/// its own switch slot, the switch slot -> client mapping for that instance
/// will be omitted from the returned map. Callers must not assume an `Ok(_)`
/// return value contains any client.
pub(crate) async fn resolve_mgd_clients(
    resolver: &internal_dns_resolver::Resolver,
    log: &slog::Logger,
) -> Result<HashMap<SwitchSlot, mg_admin_client::Client>, ResolveError> {
    let mgd_addrs = resolver.lookup_all_socket_v6(ServiceName::Mgd).await?;
    let mut clients = HashMap::new();
    for addr in mgd_addrs {
        let client = mg_admin_client::Client::new(
            &format!("http://{addr}"),
            log.clone(),
        );
        let switch_slot = match client.switch_identifiers().await {
            Ok(response) => match response.slot {
                Some(0) => SwitchSlot::Switch0,
                Some(1) => SwitchSlot::Switch1,
                Some(n) => {
                    warn!(
                        log, "failed to determine switch slot for mgd";
                        "addr" => %addr,
                        "error" => format!("mgd returned unknown slot {n}"),
                    );
                    continue;
                }
                None => {
                    warn!(
                        log, "failed to determine switch slot for mgd";
                        "addr" => %addr,
                        "error" => "mgd does not yet know its switch slot",
                    );
                    continue;
                }
            },
            Err(err) => {
                warn!(
                    log, "failed to determine switch slot for mgd";
                    "addr" => %addr,
                    InlineErrorChain::new(&err),
                );
                continue;
            }
        };
        clients.insert(switch_slot, client);
    }
    Ok(clients)
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
                    .filter(|a| !a.address.addr().is_unspecified())
                    .map(|a| a.address.addr())
                    .collect(),
            },
        );
    }

    Ok(dpd_port_settings)
}
