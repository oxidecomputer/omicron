// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for manipulating networking probe zones.

use omicron_common::api::internal::shared::NetworkInterface;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProbePath {
    /// The ID of the probe.
    pub probe_id: Uuid,
}

/// Parameters used to create a probe.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProbeCreate {
    /// The ID for the probe.
    pub id: Uuid,
    /// The external IP addresses assigned to the probe.
    pub external_ips: Vec<ExternalIp>,
    /// The probe's networking interface.
    pub interface: NetworkInterface,
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
    pub probes: Vec<ProbeCreate>,
}
