// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest;
use omicron_common::api::external::IdentityMetadataCreateParams;
use oxnet::IpNet;
use sled_agent_types_versions::latest::early_networking::PortFec;
use sled_agent_types_versions::latest::early_networking::PortSpeed;

impl From<IpNet> for latest::networking::AddressLotBlockCreate {
    fn from(ipnet: IpNet) -> Self {
        match ipnet {
            IpNet::V4(v4) => Self {
                first_address: v4.first_addr().into(),
                last_address: v4.last_addr().into(),
            },
            IpNet::V6(v6) => Self {
                first_address: v6.first_addr().into(),
                last_address: v6.last_addr().into(),
            },
        }
    }
}

impl From<mg_admin_client::types::FsmStateKind>
    for latest::networking::BgpPeerState
{
    fn from(s: mg_admin_client::types::FsmStateKind) -> Self {
        use mg_admin_client::types::FsmStateKind;
        match s {
            FsmStateKind::Idle => Self::Idle,
            FsmStateKind::Connect => Self::Connect,
            FsmStateKind::Active => Self::Active,
            FsmStateKind::OpenSent => Self::OpenSent,
            FsmStateKind::OpenConfirm => Self::OpenConfirm,
            FsmStateKind::ConnectionCollision => Self::ConnectionCollision,
            FsmStateKind::SessionSetup => Self::SessionSetup,
            FsmStateKind::Established => Self::Established,
        }
    }
}

impl latest::networking::BgpMessageHistory {
    pub fn new(arg: mg_admin_client::types::MessageHistory) -> Self {
        Self(arg)
    }
}

impl latest::networking::AggregateBgpMessageHistory {
    pub fn new(
        switch_histories: Vec<latest::networking::SwitchBgpHistory>,
    ) -> Self {
        Self { switch_histories }
    }
}

impl latest::networking::SwitchPortSettingsCreate {
    pub fn new(identity: IdentityMetadataCreateParams) -> Self {
        Self {
            identity,
            port_config: latest::networking::SwitchPortConfigCreate {
                geometry: latest::networking::SwitchPortGeometry::Qsfp28x1,
            },
            groups: Vec::new(),
            links: Vec::new(),
            interfaces: Vec::new(),
            routes: Vec::new(),
            bgp_peers: Vec::new(),
            addresses: Vec::new(),
        }
    }
}

// TODO-cleanup We could push `LinkFec` and `LinkSpeed` down into
// sled-agent-types-versions and reeport them instead of having these
// conversions. That requires some drift fixes.
impl From<PortFec> for latest::networking::LinkFec {
    fn from(x: PortFec) -> Self {
        match x {
            PortFec::Firecode => Self::Firecode,
            PortFec::None => Self::None,
            PortFec::Rs => Self::Rs,
        }
    }
}

impl From<PortSpeed> for latest::networking::LinkSpeed {
    fn from(x: PortSpeed) -> Self {
        match x {
            PortSpeed::Speed0G => Self::Speed0G,
            PortSpeed::Speed1G => Self::Speed1G,
            PortSpeed::Speed10G => Self::Speed10G,
            PortSpeed::Speed25G => Self::Speed25G,
            PortSpeed::Speed40G => Self::Speed40G,
            PortSpeed::Speed50G => Self::Speed50G,
            PortSpeed::Speed100G => Self::Speed100G,
            PortSpeed::Speed200G => Self::Speed200G,
            PortSpeed::Speed400G => Self::Speed400G,
        }
    }
}
