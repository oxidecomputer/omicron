// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External IP types for version INITIAL.

use omicron_common::api::external::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

use super::floating_ip::FloatingIp;

/// The kind of an external IP address for an instance
#[derive(
    Debug, Clone, Copy, Deserialize, Eq, Serialize, JsonSchema, PartialEq,
)]
#[serde(rename_all = "snake_case")]
pub enum IpKind {
    SNat,
    Ephemeral,
    Floating,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExternalIp {
    #[serde(rename = "snat")]
    SNat(SNatIp),
    Ephemeral {
        ip: IpAddr,
        ip_pool_id: Uuid,
    },
    Floating(FloatingIp),
}

impl ExternalIp {
    pub fn ip(&self) -> IpAddr {
        match self {
            Self::SNat(snat) => snat.ip,
            Self::Ephemeral { ip, .. } => *ip,
            Self::Floating(float) => float.ip,
        }
    }

    pub fn kind(&self) -> IpKind {
        match self {
            Self::SNat(_) => IpKind::SNat,
            Self::Ephemeral { .. } => IpKind::Ephemeral,
            Self::Floating(_) => IpKind::Floating,
        }
    }
}

/// A source NAT IP address.
///
/// SNAT addresses are ephemeral addresses used only for outbound connectivity.
#[derive(Debug, Clone, Deserialize, PartialEq, Serialize, JsonSchema)]
pub struct SNatIp {
    /// The IP address.
    pub ip: IpAddr,
    /// The first usable port within the IP address.
    pub first_port: u16,
    /// The last usable port within the IP address.
    pub last_port: u16,
    /// ID of the IP Pool from which the address is taken.
    pub ip_pool_id: Uuid,
}

impl From<FloatingIp> for ExternalIp {
    fn from(value: FloatingIp) -> Self {
        ExternalIp::Floating(value)
    }
}

impl TryFrom<ExternalIp> for FloatingIp {
    type Error = Error;

    fn try_from(value: ExternalIp) -> Result<Self, Self::Error> {
        match value {
            ExternalIp::SNat(_) | ExternalIp::Ephemeral { .. } => {
                Err(Error::internal_error(
                    "tried to convert an SNAT or ephemeral IP into a floating IP",
                ))
            }
            ExternalIp::Floating(v) => Ok(v),
        }
    }
}
