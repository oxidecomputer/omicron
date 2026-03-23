// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest;
use omicron_common::api::external::IdentityMetadataCreateParams;
use oxnet::IpNet;
use sled_agent_types_versions::latest::early_networking::UplinkAddress;
use sled_agent_types_versions::latest::early_networking::UplinkIpNet;
use sled_agent_types_versions::latest::early_networking::UplinkIpNetError;
use std::fmt;

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

impl fmt::Display for latest::networking::LoopbackAddressIpNet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<UplinkIpNet> for latest::networking::LoopbackAddressIpNet {
    fn from(value: UplinkIpNet) -> Self {
        Self(value)
    }
}

impl TryFrom<IpNet> for latest::networking::LoopbackAddressIpNet {
    type Error = UplinkIpNetError;

    fn try_from(value: IpNet) -> Result<Self, Self::Error> {
        Ok(Self(value.try_into()?))
    }
}

impl From<latest::networking::LoopbackAddressIpNet> for UplinkIpNet {
    fn from(value: latest::networking::LoopbackAddressIpNet) -> Self {
        value.0
    }
}

impl From<latest::networking::LoopbackAddressIpNet> for UplinkAddress {
    fn from(value: latest::networking::LoopbackAddressIpNet) -> Self {
        Self::Static { ip_net: value.into() }
    }
}

impl From<latest::networking::LoopbackAddressIpNet> for IpNet {
    fn from(value: latest::networking::LoopbackAddressIpNet) -> Self {
        value.0.into()
    }
}
