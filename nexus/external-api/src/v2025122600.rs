// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2025121700 to 2025122600

use nexus_types::external_api::shared::ProbeExternalIp;
use omicron_common::api::external::Name;
use omicron_common::api::internal::shared::network_interface::v1::NetworkInterface;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct ProbeInfo {
    pub id: Uuid,
    pub name: Name,
    #[schemars(with = "Uuid")]
    pub sled: SledUuid,
    pub external_ips: Vec<ProbeExternalIp>,
    // NOTE: This type currently appears in both the external and internal APIs.
    // It's not used in the internal API anymore, and we've not yet expanded the
    // external API to support dual-stack NICs. When we do, this whole type
    // needs a new version in the external API, and the internal API needs to
    // continue to refer to this original version.
    pub interface: NetworkInterface,
}

impl TryFrom<nexus_types::external_api::shared::ProbeInfo> for ProbeInfo {
    type Error = omicron_common::api::external::Error;
    fn try_from(
        value: nexus_types::external_api::shared::ProbeInfo,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            name: value.name.clone(),
            sled: value.sled,
            external_ips: value.external_ips.clone(),
            interface: value.interface.try_into()?,
        })
    }
}
