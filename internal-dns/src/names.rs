// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Well-known DNS names and related types for internal DNS (see RFD 248)

use uuid::Uuid;

/// Name for the control plane DNS zone
pub const DNS_ZONE: &str = "control-plane.oxide.internal";

/// Names of services within the control plane
#[derive(Clone, Debug, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub enum ServiceName {
    Clickhouse,
    Cockroach,
    InternalDNS,
    ExternalDNS,
    Nexus,
    Oximeter,
    ManagementGatewayService,
    Wicketd,
    Dendrite,
    Tfport,
    CruciblePantry,
    SledAgent(Uuid),
    Crucible(Uuid),
    BoundaryNTP,
    InternalNTP,
    Maghemite,
}

impl ServiceName {
    fn service_kind(&self) -> &'static str {
        match self {
            ServiceName::Clickhouse => "clickhouse",
            ServiceName::Cockroach => "cockroach",
            ServiceName::ExternalDNS => "external-dns",
            ServiceName::InternalDNS => "nameservice",
            ServiceName::Nexus => "nexus",
            ServiceName::Oximeter => "oximeter",
            ServiceName::ManagementGatewayService => "mgs",
            ServiceName::Wicketd => "wicketd",
            ServiceName::Dendrite => "dendrite",
            ServiceName::Tfport => "tfport",
            ServiceName::CruciblePantry => "crucible-pantry",
            ServiceName::SledAgent(_) => "sledagent",
            ServiceName::Crucible(_) => "crucible",
            ServiceName::BoundaryNTP => "boundary-ntp",
            ServiceName::InternalNTP => "internal-ntp",
            ServiceName::Maghemite => "maghemite",
        }
    }

    /// Returns the DNS name for this service, ignoring the zone part of the DNS
    /// name
    pub(crate) fn dns_name(&self) -> String {
        match self {
            ServiceName::Clickhouse
            | ServiceName::Cockroach
            | ServiceName::InternalDNS
            | ServiceName::ExternalDNS
            | ServiceName::Nexus
            | ServiceName::Oximeter
            | ServiceName::ManagementGatewayService
            | ServiceName::Wicketd
            | ServiceName::Dendrite
            | ServiceName::Tfport
            | ServiceName::CruciblePantry
            | ServiceName::BoundaryNTP
            | ServiceName::InternalNTP
            | ServiceName::Maghemite => {
                format!("_{}._tcp", self.service_kind())
            }
            ServiceName::SledAgent(id) => {
                format!("_{}._tcp.{}", self.service_kind(), id)
            }
            ServiceName::Crucible(id) => {
                format!("_{}._tcp.{}", self.service_kind(), id)
            }
        }
    }
}
