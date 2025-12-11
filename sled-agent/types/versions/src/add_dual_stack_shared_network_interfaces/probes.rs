// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Probe types for Sled Agent API version 10+.
//!
//! This version uses NetworkInterface v2 (dual-stack, multiple IP addresses).

use iddqd::IdHashItem;
use iddqd::IdHashMap;
use iddqd::id_upcast;
use omicron_common::api::external::Error as ExternalError;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_uuid_kinds::ProbeUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;

/// Parameters used to create a probe.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProbeCreate {
    /// The ID for the probe.
    pub id: ProbeUuid,
    /// The external IP addresses assigned to the probe.
    pub external_ips: Vec<ExternalIp>,
    /// The probe's networking interface.
    pub interface: NetworkInterface,
}

impl IdHashItem for ProbeCreate {
    type Key<'a> = ProbeUuid;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}

/// An external IP address used by a probe.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ExternalIp {
    /// The external IP address.
    pub ip: IpAddr,
    /// The kind of address this is.
    pub kind: IpKind,
    /// The first port used by the address.
    pub first_port: u16,
    /// The last port used by the address.
    pub last_port: u16,
}

/// The kind of external IP address of a probe.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IpKind {
    Snat,
    Ephemeral,
    Floating,
}

/// A set of probes that the target sled should run.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProbeSet {
    /// The exact set of probes to run.
    pub probes: IdHashMap<ProbeCreate>,
}

impl TryFrom<crate::v6::probes::ProbeCreate> for ProbeCreate {
    type Error = ExternalError;

    fn try_from(
        v6: crate::v6::probes::ProbeCreate,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            id: v6.id,
            external_ips: v6.external_ips.into_iter().map(Into::into).collect(),
            interface: v6.interface.try_into()?,
        })
    }
}

impl From<crate::v6::probes::ExternalIp> for ExternalIp {
    fn from(v6: crate::v6::probes::ExternalIp) -> Self {
        Self {
            ip: v6.ip,
            kind: v6.kind.into(),
            first_port: v6.first_port,
            last_port: v6.last_port,
        }
    }
}

impl From<crate::v6::probes::IpKind> for IpKind {
    fn from(v6: crate::v6::probes::IpKind) -> Self {
        match v6 {
            crate::v6::probes::IpKind::Snat => Self::Snat,
            crate::v6::probes::IpKind::Ephemeral => Self::Ephemeral,
            crate::v6::probes::IpKind::Floating => Self::Floating,
        }
    }
}

impl TryFrom<crate::v6::probes::ProbeSet> for ProbeSet {
    type Error = ExternalError;

    fn try_from(v6: crate::v6::probes::ProbeSet) -> Result<Self, Self::Error> {
        v6.probes
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()
            .map(|probes| Self { probes })
    }
}
