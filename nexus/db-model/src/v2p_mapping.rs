// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Ipv4Addr;
use crate::Ipv6Addr;
use crate::MacAddr;
use crate::Vni;
use omicron_uuid_kinds::SledUuid;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// This is not backed by an actual database view,
/// but it is still essentially a "view" in the sense
/// that it is a read-only data model derived from
/// multiple db tables
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct V2PMappingView {
    pub nic_id: Uuid,
    pub sled_id: SledUuid,
    pub sled_ip: Ipv6Addr,
    pub vni: Vni,
    pub mac: MacAddr,
    // NOTE: At least one of the below will be non-None.
    pub ipv4: Option<Ipv4Addr>,
    pub ipv6: Option<Ipv6Addr>,
}
