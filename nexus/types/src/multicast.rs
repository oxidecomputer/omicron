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
#[derive(Clone, Debug)]
pub struct MulticastGroupCreate {
    pub identity: IdentityMetadataCreateParams,
    /// The multicast IP address to allocate.
    ///
    /// If `None`, one will be allocated from the default pool.
    pub multicast_ip: Option<IpAddr>,
    /// Source IP addresses for Source-Specific Multicast (SSM).
    ///
    /// None uses default behavior (Any-Source Multicast).
    /// Empty list explicitly allows any source (Any-Source Multicast).
    /// Non-empty list restricts to specific sources (SSM).
    pub source_ips: Option<Vec<IpAddr>>,
}
