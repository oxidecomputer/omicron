// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2025_11_20_00::bfd::BfdState;
use crate::v2026_03_06_01;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types_versions::v1::early_networking::BfdMode;
use std::net::IpAddr;

use super::networking::{SwitchError, SwitchResults};

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
pub struct BfdPeerStatus {
    pub peer: IpAddr,
    pub state: BfdState,
    pub local: Option<IpAddr>,
    pub detection_threshold: u8,
    pub required_rx: u64,
    pub mode: BfdMode,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BfdPeerStatuses(pub Vec<BfdPeerStatus>);

impl TryFrom<SwitchResults<BfdPeerStatuses>>
    for Vec<v2026_03_06_01::bfd::BfdStatus>
{
    type Error = SwitchError;

    fn try_from(
        results: SwitchResults<BfdPeerStatuses>,
    ) -> Result<Self, Self::Error> {
        let mut statuses = Vec::new();
        for (switch_slot, result) in results {
            let value = match result.into_result() {
                Ok(value) => value,
                Err(SwitchError::MgdUnresolved) => {
                    continue;
                }
                Err(error) => return Err(error),
            };
            statuses.extend(value.0.into_iter().map(|status| {
                v2026_03_06_01::bfd::BfdStatus {
                    peer: status.peer,
                    state: status.state,
                    switch_slot,
                    local: status.local,
                    detection_threshold: status.detection_threshold,
                    required_rx: status.required_rx,
                    mode: status.mode,
                }
            }));
        }
        Ok(statuses)
    }
}

#[cfg(test)]
mod tests {
    use super::super::networking::{SwitchError, SwitchResult};
    use super::*;
    use sled_agent_types_versions::v1::early_networking::SwitchSlot;
    use std::net::{IpAddr, Ipv6Addr};

    #[test]
    fn conversion_restores_switch_slots_and_omits_unavailable_switches() {
        let peer = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let results = SwitchResults {
            switch0: SwitchResult::Ok {
                value: BfdPeerStatuses(vec![BfdPeerStatus {
                    peer,
                    state: BfdState::Up,
                    local: None,
                    detection_threshold: 3,
                    required_rx: 1000,
                    mode: BfdMode::SingleHop,
                }]),
            },
            switch1: SwitchResult::Err { error: SwitchError::MgdUnresolved },
        };

        let statuses =
            Vec::<v2026_03_06_01::bfd::BfdStatus>::try_from(results).unwrap();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].peer, peer);
        assert_eq!(statuses[0].switch_slot, SwitchSlot::Switch0);
        assert_eq!(statuses[0].state, BfdState::Up);
    }

    #[test]
    fn conversion_preserves_query_failure() {
        let results = SwitchResults {
            switch0: SwitchResult::Err { error: SwitchError::QueryFailed },
            switch1: SwitchResult::Ok { value: BfdPeerStatuses(Vec::new()) },
        };

        assert_eq!(
            Vec::<v2026_03_06_01::bfd::BfdStatus>::try_from(results),
            Err(SwitchError::QueryFailed),
        );
    }
}
