// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::SimpleIdentity;
use oxnet::IpNet;
use sled_agent_types_versions::v1::early_networking::SwitchSlot;
use uuid::Uuid;

impl<T> latest::networking::SwitchResult<T> {
    pub fn into_result(
        self,
    ) -> Result<T, latest::networking::SwitchUnavailableReason> {
        match self {
            Self::Available { value } => Ok(value),
            Self::Unavailable { reason } => Err(reason),
        }
    }
}

impl<T> latest::networking::SwitchResults<T> {
    pub fn iter(
        &self,
    ) -> std::array::IntoIter<
        (SwitchSlot, &latest::networking::SwitchResult<T>),
        2,
    > {
        [
            (SwitchSlot::Switch0, &self.switch0),
            (SwitchSlot::Switch1, &self.switch1),
        ]
        .into_iter()
    }
}

impl<'a, T> IntoIterator for &'a latest::networking::SwitchResults<T> {
    type Item = (SwitchSlot, &'a latest::networking::SwitchResult<T>);
    type IntoIter = std::array::IntoIter<Self::Item, 2>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T> IntoIterator for latest::networking::SwitchResults<T> {
    type Item = (SwitchSlot, latest::networking::SwitchResult<T>);
    type IntoIter = std::array::IntoIter<Self::Item, 2>;

    fn into_iter(self) -> Self::IntoIter {
        [
            (SwitchSlot::Switch0, self.switch0),
            (SwitchSlot::Switch1, self.switch1),
        ]
        .into_iter()
    }
}

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

impl From<mg_api_types::bgp::session::FsmStateKind>
    for latest::networking::BgpPeerState
{
    fn from(s: mg_api_types::bgp::session::FsmStateKind) -> Self {
        use mg_api_types::bgp::session::FsmStateKind;
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

impl SimpleIdentity for latest::networking::LldpNeighbor {
    fn id(&self) -> Uuid {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn switch_results_iterators_include_slots_in_order() {
        use latest::networking::{SwitchResult, SwitchResults};

        let results = SwitchResults {
            switch0: SwitchResult::Available { value: 0 },
            switch1: SwitchResult::Available { value: 1 },
        };

        let borrowed: Vec<_> = results
            .iter()
            .map(|(slot, result)| {
                let SwitchResult::Available { value } = result else {
                    panic!("expected an available result");
                };
                (slot, *value)
            })
            .collect();
        assert_eq!(
            borrowed,
            vec![(SwitchSlot::Switch0, 0), (SwitchSlot::Switch1, 1)]
        );
        assert_eq!((&results).into_iter().count(), 2);

        let owned: Vec<_> = results
            .into_iter()
            .map(|(slot, result)| (slot, result.into_result().unwrap()))
            .collect();
        assert_eq!(owned, borrowed);
    }
}
