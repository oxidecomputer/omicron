// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Probe types for version INITIAL.

use std::net::IpAddr;

use omicron_common::api::external::{IdentityMetadataCreateParams, Name, NameOrId};
use omicron_common::api::internal::shared::network_interface::v1::NetworkInterface as NetworkInterfaceV1;
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
    pub interface: NetworkInterfaceV1,
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct ProbeExternalIp {
    pub ip: IpAddr,
    pub first_port: u16,
    pub last_port: u16,
    pub kind: ProbeExternalIpKind,
}

#[derive(Debug, Clone, Copy, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeExternalIpKind {
    Snat,
    Floating,
    Ephemeral,
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

/// List probes with an optional name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ProbeListSelector {
    /// A name or id to use when selecting a probe.
    pub name_or_id: Option<NameOrId>,
}
