// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal multicast types used by Nexus.

use omicron_common::api::external::IdentityMetadataCreateParams;
use std::net::IpAddr;

/// Internal parameters for creating a multicast group.
///
/// Groups are created implicitly when the first member joins. This struct
/// is used internally by Nexus to pass creation parameters to the datastore.
///
/// Note: Source IPs are now per-member, not per-group. Groups are created
/// without sources; sources are specified when members join.
#[derive(Clone, Debug)]
pub struct MulticastGroupCreate {
    pub identity: IdentityMetadataCreateParams,
    /// The multicast IP address to allocate.
    ///
    /// If `None`, one will be allocated from the default pool.
    pub multicast_ip: Option<IpAddr>,
    /// Whether the joining member has source IPs.
    ///
    /// Used for default pool selection when `multicast_ip` is `None`:
    /// - If true: prefer SSM pool (232/8), fall back to ASM (224/8)
    /// - If false: use ASM pool directly
    pub has_sources: bool,
}
