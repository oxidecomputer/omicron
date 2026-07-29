// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v1::early_networking as v1;
use crate::v1::early_networking::BfdMode;
use crate::v1::early_networking::SwitchSlot;
use crate::v20::early_networking as v20;
use crate::v42::early_networking as v42;
use crate::v44::early_networking as v44;
use oxnet::Ipv6Net;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::num::NonZeroU8;

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
pub struct BfdPeerConfig {
    pub local: Option<IpAddr>,
    pub remote: IpAddr,
    pub detection_threshold: NonZeroU8,
    pub required_rx: u64,
    pub mode: BfdMode,
    pub switch: SwitchSlot,
}

// This conversion is infallible and will clamp `detection_threshold` to the
// valid range (1-255) rather than rejecting invalid configs from the bootstore.
impl From<v1::BfdPeerConfig> for BfdPeerConfig {
    fn from(old: v1::BfdPeerConfig) -> Self {
        Self {
            local: old.local,
            remote: old.remote,
            detection_threshold: NonZeroU8::new(old.detection_threshold)
                .unwrap_or(NonZeroU8::MIN),
            required_rx: old.required_rx,
            mode: old.mode,
            switch: old.switch,
        }
    }
}

impl From<BfdPeerConfig> for v1::BfdPeerConfig {
    fn from(new: BfdPeerConfig) -> Self {
        Self {
            local: new.local,
            remote: new.remote,
            detection_threshold: new.detection_threshold.get(),
            required_rx: new.required_rx,
            mode: new.mode,
            switch: new.switch,
        }
    }
}

/// Initial network configuration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct RackNetworkConfig {
    pub rack_subnet: Ipv6Net,
    // TODO: #3591 Consider making infra-ip ranges implicit for uplinks
    /// First ip address to be used for configuring network infrastructure
    pub infra_ip_first: IpAddr,
    /// Last ip address to be used for configuring network infrastructure
    pub infra_ip_last: IpAddr,
    /// Uplinks for connecting the rack to external networks
    pub ports: v42::UplinkPorts,
    /// BGP configurations for connecting the rack to external networks
    pub bgp: Vec<v20::BgpConfig>,
    /// BFD configuration for connecting the rack to external networks
    #[serde(default)]
    pub bfd: Vec<v44::BfdPeerConfig>,
}

impl From<v42::RackNetworkConfig> for RackNetworkConfig {
    fn from(old: v42::RackNetworkConfig) -> Self {
        Self {
            rack_subnet: old.rack_subnet,
            infra_ip_first: old.infra_ip_first,
            infra_ip_last: old.infra_ip_last,
            ports: old.ports,
            bgp: old.bgp,
            bfd: old.bfd.into_iter().map(From::from).collect(),
        }
    }
}

impl From<RackNetworkConfig> for v42::RackNetworkConfig {
    fn from(new: RackNetworkConfig) -> Self {
        Self {
            rack_subnet: new.rack_subnet,
            infra_ip_first: new.infra_ip_first,
            infra_ip_last: new.infra_ip_last,
            ports: new.ports,
            bgp: new.bgp,
            bfd: new.bfd.into_iter().map(From::from).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bfd_peer_config_conversion_clamps_zero_threshold() {
        let old = v1::BfdPeerConfig {
            local: None,
            remote: "192.0.2.1".parse().unwrap(),
            detection_threshold: 0,
            required_rx: 1000,
            mode: BfdMode::SingleHop,
            switch: SwitchSlot::Switch0,
        };
        let new = BfdPeerConfig::from(old);
        assert_eq!(new.detection_threshold, NonZeroU8::MIN);
    }

    #[test]
    fn bfd_peer_config_conversion_preserves_nonzero_threshold() {
        let old = v1::BfdPeerConfig {
            local: None,
            remote: "192.0.2.1".parse().unwrap(),
            detection_threshold: 3,
            required_rx: 1000,
            mode: BfdMode::SingleHop,
            switch: SwitchSlot::Switch0,
        };
        let new = BfdPeerConfig::from(old);
        assert_eq!(new.detection_threshold.get(), 3);
    }
}
