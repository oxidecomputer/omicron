// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Probe types for version DUAL_STACK_NICS.

use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use omicron_common::api::internal::shared::network_interface::NetworkInterface;
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
    pub interface: NetworkInterface,
}

impl TryFrom<ProbeInfo> for crate::v2025112000::probe::ProbeInfo {
    type Error = external::Error;

    fn try_from(
        new: ProbeInfo,
    ) -> Result<crate::v2025112000::probe::ProbeInfo, Self::Error> {
        Ok(crate::v2025112000::probe::ProbeInfo {
            id: new.id,
            name: new.name,
            sled: new.sled,
            external_ips: new.external_ips,
            interface: new.interface.try_into()?,
        })
    }
}

/// Create time parameters for probes.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProbeCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    #[schemars(with = "Uuid")]
    pub sled: SledUuid,
    pub ip_pool: Option<NameOrId>,
}

impl From<crate::v2025112000::probe::ProbeCreate> for ProbeCreate {
    fn from(old: crate::v2025112000::probe::ProbeCreate) -> ProbeCreate {
        ProbeCreate {
            identity: old.identity,
            sled: old.sled,
            ip_pool: old.ip_pool,
        }
    }
}
