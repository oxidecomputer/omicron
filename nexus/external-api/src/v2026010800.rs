// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2026010800 to 2026011000.
//!
//! This version (MULTICAST_IMPLICIT_LIFECYCLE_UPDATES) uses the same request
//! types as current (MulticastGroupIdentifier, source_ips in member add), but
//! still includes `mvlan` in responses for backwards compatibility.
//!
//! ## MulticastGroup Changes
//!
//! [`MulticastGroup`] includes `mvlan` field which was removed in 2026011000
//! (MULTICAST_DROP_MVLAN). The mvlan field was for egress multicast VLAN
//! tagging which is not in MVP scope.
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

/// View of a Multicast Group.
///
/// This version includes `mvlan` for backwards compatibility with clients
/// using API version 2026010800..2026011000.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroup {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The multicast IP address held by this resource.
    pub multicast_ip: IpAddr,
    /// Source IP addresses for multicast source filtering (SSM requires these;
    /// ASM can optionally use them via IGMPv3/MLDv2). Empty array means any source.
    pub source_ips: Vec<IpAddr>,
    // Deprecated: Always None. Field kept for backwards compatibility with
    // clients using API version 2026010800..2026011000. Removed in 2026011000
    // as egress multicast is not in MVP scope.
    /// Multicast VLAN (MVLAN) for egress multicast traffic to upstream networks.
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
