// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use db::datastore::SwitchPortSettingsCombinedResult;
use dpd_client::types::{
    LinkCreate, LinkId, LinkSettings, PortFec, PortSettings, PortSpeed,
};
use nexus_db_model::{SwitchLinkFec, SwitchLinkSpeed};
use nexus_db_queries::db;
use std::collections::HashMap;

pub(crate) fn api_to_dpd_port_settings(
    settings: &SwitchPortSettingsCombinedResult,
) -> Result<PortSettings, String> {
    let mut dpd_port_settings = PortSettings {
        links: HashMap::new(),
        v4_routes: HashMap::new(),
        v6_routes: HashMap::new(),
    };

    //TODO breakouts
    let link_id = LinkId(0);

    for l in settings.links.iter() {
        dpd_port_settings.links.insert(
            link_id.to_string(),
            LinkSettings {
                params: LinkCreate {
                    autoneg: l.autoneg,
                    lane: Some(LinkId(0)),
                    kr: false,
                    fec: match l.fec {
                        SwitchLinkFec::Firecode => PortFec::Firecode,
                        SwitchLinkFec::Rs => PortFec::Rs,
                        SwitchLinkFec::None => PortFec::None,
                    },
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
                    .map(|a| a.address.ip())
                    .collect(),
            },
        );
    }

    Ok(dpd_port_settings)
}
