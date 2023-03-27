// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Well-known DNS names and related types for internal DNS (see RFD 248)

use std::fmt;

/// Name for the control plane DNS zone
pub const DNS_ZONE: &str = "control-plane.oxide.internal";

/// Names for services where backends are interchangeable.
#[derive(Clone, Debug, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub enum ServiceName {
    Clickhouse,
    Cockroach,
    InternalDNS,
    Nexus,
    Oximeter,
    ManagementGatewayService,
    Wicketd,
    Dendrite,
    Tfport,
    CruciblePantry,
}

impl fmt::Display for ServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ServiceName::Clickhouse => write!(f, "clickhouse"),
            ServiceName::Cockroach => write!(f, "cockroach"),
            ServiceName::InternalDNS => write!(f, "internalDNS"),
            ServiceName::Nexus => write!(f, "nexus"),
            ServiceName::Oximeter => write!(f, "oximeter"),
            ServiceName::ManagementGatewayService => write!(f, "mgs"),
            ServiceName::Wicketd => write!(f, "wicketd"),
            ServiceName::Dendrite => write!(f, "dendrite"),
            ServiceName::Tfport => write!(f, "tfport"),
            ServiceName::CruciblePantry => write!(f, "crucible-pantry"),
        }
    }
}

/// Names for services where backends are not interchangeable.
#[derive(Clone, Debug, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub enum BackendName {
    Crucible,
    SledAgent,
}

impl fmt::Display for BackendName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            BackendName::Crucible => write!(f, "crucible"),
            BackendName::SledAgent => write!(f, "sledagent"),
        }
    }
}
