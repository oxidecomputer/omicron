// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Probe types for version SILO_PROJECT_IP_VERSION_AND_POOL_TYPE.
//!
//! This version uses `NetworkInterfaceV1` which has a single IP address.

use omicron_common::api::external;
use omicron_common::api::external::Name;
use omicron_common::api::internal::shared::v1::NetworkInterface as NetworkInterfaceV1;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v2025112000::probe::ProbeExternalIp;

/// Information about a probe.
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct ProbeInfo {
    pub id: Uuid,
    pub name: Name,
    #[schemars(with = "Uuid")]
    pub sled: SledUuid,
    pub external_ips: Vec<ProbeExternalIp>,
    pub interface: NetworkInterfaceV1,
}

impl TryFrom<crate::v2026010300::probe::ProbeInfo> for ProbeInfo {
    type Error = external::Error;

    fn try_from(
        new: crate::v2026010300::probe::ProbeInfo,
    ) -> Result<ProbeInfo, Self::Error> {
        Ok(ProbeInfo {
            id: new.id,
            name: new.name,
            sled: new.sled,
            external_ips: new.external_ips,
            interface: new.interface.try_into()?,
        })
    }
}
