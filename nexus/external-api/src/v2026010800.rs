// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types from 2026010800 (`MULTICAST_IMPLICIT_LIFECYCLE_UPDATES`)
//! that changed in 2026012100 (`MULTICAST_DROP_MVLAN_ADD_HAS_ANY`).
//!
//! ## MulticastGroup
//!
//! [`MulticastGroup`] includes the deprecated `mvlan` field (always None) and
//! omits `has_any_source_member` which was added in 2026012100.
//!
//! Affected endpoints:
//! - `GET /v1/multicast-groups` (multicast_group_list)
//! - `GET /v1/multicast-groups/{multicast_group}` (multicast_group_view)

use std::net::IpAddr;

use omicron_common::api::external::IdentityMetadata;
use omicron_common::vlan::VlanID;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use nexus_types::external_api::views;

/// View of a Multicast Group
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroup {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The multicast IP address held by this resource.
    pub multicast_ip: IpAddr,
    /// Union of all member source IP addresses (computed, read-only).
    ///
    /// This field shows the combined source IPs across all group members.
    /// Individual members may subscribe to different sources; this union
    /// reflects all sources that any member is subscribed to.
    /// Empty array means no members have source filtering enabled.
    pub source_ips: Vec<IpAddr>,
    /// Multicast VLAN (MVLAN) for egress multicast traffic to upstream networks.
    /// None means no VLAN tagging on egress.
    pub mvlan: Option<VlanID>,
    /// The ID of the IP pool this resource belongs to.
    pub ip_pool_id: Uuid,
    /// Current state of the multicast group.
    pub state: String,
}

impl From<views::MulticastGroup> for MulticastGroup {
    fn from(v: views::MulticastGroup) -> Self {
        Self {
            identity: v.identity,
            multicast_ip: v.multicast_ip,
            source_ips: v.source_ips,
            // mvlan has been removed from multicast groups. Return None for
            // backwards compatibility with old API clients.
            mvlan: None,
            ip_pool_id: v.ip_pool_id,
            state: v.state,
        }
    }
}
