// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest;
use oxnet::IpNet;

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
