// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Naming scheme for Internal DNS names (RFD 248).

use std::fmt;
use uuid::Uuid;

pub(crate) const DNS_ZONE: &str = "control-plane.oxide.internal";

/// Names for services where backends are interchangeable.
#[derive(Clone, Debug, Hash, Eq, PartialEq, PartialOrd)]
pub enum ServiceName {
    Clickhouse,
    Cockroach,
    InternalDNS,
    Nexus,
    Oximeter,
    ManagementGatewayService,
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
            ServiceName::Dendrite => write!(f, "dendrite"),
            ServiceName::Tfport => write!(f, "tfport"),
            ServiceName::CruciblePantry => write!(f, "crucible-pantry"),
        }
    }
}

/// Names for services where backends are not interchangeable.
#[derive(Clone, Debug, Hash, Eq, PartialEq, PartialOrd)]
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

#[derive(Clone, Debug, Hash, Eq, PartialEq, PartialOrd)]
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

#[derive(Clone, Debug, PartialEq, PartialOrd)]
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn display_srv_service() {
        assert_eq!(
            SRV::Service(ServiceName::Clickhouse).to_string(),
            "_clickhouse._tcp.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Service(ServiceName::Cockroach).to_string(),
            "_cockroach._tcp.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Service(ServiceName::InternalDNS).to_string(),
            "_internalDNS._tcp.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Service(ServiceName::Nexus).to_string(),
            "_nexus._tcp.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Service(ServiceName::Oximeter).to_string(),
            "_oximeter._tcp.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Service(ServiceName::Dendrite).to_string(),
            "_dendrite._tcp.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Service(ServiceName::CruciblePantry).to_string(),
            "_crucible-pantry._tcp.control-plane.oxide.internal",
        );
    }

    #[test]
    fn display_srv_backend() {
        let uuid = Uuid::nil();
        assert_eq!(
            SRV::Backend(BackendName::Crucible, uuid).to_string(),
            "_crucible._tcp.00000000-0000-0000-0000-000000000000.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Backend(BackendName::SledAgent, uuid).to_string(),
            "_sledagent._tcp.00000000-0000-0000-0000-000000000000.control-plane.oxide.internal",
        );
    }

    #[test]
    fn display_aaaa() {
        let uuid = Uuid::nil();
        assert_eq!(
            AAAA::Sled(uuid).to_string(),
            "00000000-0000-0000-0000-000000000000.sled.control-plane.oxide.internal",
        );
        assert_eq!(
            AAAA::Zone(uuid).to_string(),
            "00000000-0000-0000-0000-000000000000.host.control-plane.oxide.internal",
        );
    }
}
