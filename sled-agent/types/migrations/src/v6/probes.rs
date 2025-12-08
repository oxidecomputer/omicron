// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Probe types for Sled Agent API versions 6-9.
//!
//! Probes were introduced in v6 (ADD_PROBE_PUT_ENDPOINT).
//! Uses NetworkInterface v1 (single IP, not dual-stack).

use iddqd::IdHashItem;
use iddqd::IdHashMap;
use iddqd::id_upcast;
use omicron_common::api::internal::shared::network_interface::v1::NetworkInterface;
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
