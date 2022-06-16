// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::fmt;
use uuid::Uuid;

const DNS_ZONE: &str = "control-plane.oxide.internal";

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum ServiceName {
    Clickhouse,
    Cockroach,
    InternalDNS,
    Nexus,
    Oximeter,
}

impl fmt::Display for ServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ServiceName::Clickhouse => write!(f, "clickhouse"),
            ServiceName::Cockroach => write!(f, "cockroach"),
            ServiceName::InternalDNS => write!(f, "internalDNS"),
            ServiceName::Nexus => write!(f, "nexus"),
            ServiceName::Oximeter => write!(f, "oximeter"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
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

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum SRV {
    /// A service identified and accessed by name, such as "nexus", "CRDB", etc.
    ///
    /// This is used in cases where services are interchangeable.
    Service(ServiceName),

    /// A service identified by name and a unique identifier.
    ///
    /// This is used in cases where services are not interchangeable, such as
    /// for the Sled agent.
    Backend(BackendName, Uuid),
}

impl fmt::Display for SRV {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            SRV::Service(name) => {
                write!(f, "_{}._tcp.{}", name, DNS_ZONE)
            }
            SRV::Backend(name, id) => {
                write!(f, "_{}._tcp.{}.{}", name, id, DNS_ZONE)
            }
        }
    }
}

pub enum AAAA {
    /// Identifies an AAAA record for a sled.
    Sled(Uuid),

    /// Identifies an AAAA record for a zone within a sled.
    Zone(Uuid),
}

impl fmt::Display for AAAA {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            AAAA::Sled(id) => {
                write!(f, "{}.sled.{}", id, DNS_ZONE)
            }
            AAAA::Zone(id) => {
                write!(f, "{}.host.{}", id, DNS_ZONE)
            }
        }
    }
}
