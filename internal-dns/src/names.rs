// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Well-known DNS names and related types for internal DNS (see RFD 248)

use omicron_common::api::internal::shared::SwitchLocation;
use uuid::Uuid;

/// Name for the control plane DNS zone
pub const DNS_ZONE: &str = "control-plane.oxide.internal";

/// Name for the delegated external DNS zone that's used in testing and
/// development
pub const DNS_ZONE_EXTERNAL_TESTING: &str = "oxide-dev.test";

/// Names of services within the control plane
#[derive(Clone, Copy, Debug, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub enum ServiceName {
    Clickhouse,
    ClickhouseKeeper,
    Cockroach,
    InternalDns,
    ExternalDns,
    Nexus,
    Oximeter,
    ManagementGatewayService,
    Wicketd,
    Dendrite,
    Tfport,
    CruciblePantry,
    SledAgent(Uuid),
    Crucible(Uuid),
    BoundaryNtp,
    InternalNtp,
    Maghemite, //TODO change to Dpd - maghemite has several services.
    Mgd,
    Scrimlet(SwitchLocation),
}

impl ServiceName {
    fn service_kind(&self) -> &'static str {
        match self {
            ServiceName::Clickhouse => "clickhouse",
            ServiceName::ClickhouseKeeper => "clickhouse-keeper",
            ServiceName::Cockroach => "cockroach",
            ServiceName::ExternalDns => "external-dns",
            ServiceName::InternalDns => "nameservice",
            ServiceName::Nexus => "nexus",
            ServiceName::Oximeter => "oximeter",
            ServiceName::ManagementGatewayService => "mgs",
            ServiceName::Wicketd => "wicketd",
            ServiceName::Dendrite => "dendrite",
            ServiceName::Tfport => "tfport",
            ServiceName::CruciblePantry => "crucible-pantry",
            ServiceName::SledAgent(_) => "sledagent",
            ServiceName::Crucible(_) => "crucible",
            ServiceName::BoundaryNtp => "boundary-ntp",
            ServiceName::InternalNtp => "internal-ntp",
            ServiceName::Maghemite => "maghemite",
            ServiceName::Mgd => "mgd",
            ServiceName::Scrimlet(_) => "scrimlet",
        }
    }

    /// Returns the DNS name for this service, ignoring the zone part of the DNS
    /// name
    pub fn dns_name(&self) -> String {
        match self {
            ServiceName::Clickhouse
            | ServiceName::ClickhouseKeeper
            | ServiceName::Cockroach
            | ServiceName::InternalDns
            | ServiceName::ExternalDns
            | ServiceName::Nexus
            | ServiceName::Oximeter
            | ServiceName::ManagementGatewayService
            | ServiceName::Wicketd
            | ServiceName::Dendrite
            | ServiceName::Tfport
            | ServiceName::CruciblePantry
            | ServiceName::BoundaryNtp
            | ServiceName::InternalNtp
            | ServiceName::Maghemite
            | ServiceName::Mgd => {
                format!("_{}._tcp", self.service_kind())
            }
            ServiceName::SledAgent(id) => {
                format!("_{}._tcp.{}", self.service_kind(), id)
            }
            ServiceName::Crucible(id) => {
                format!("_{}._tcp.{}", self.service_kind(), id)
            }
            ServiceName::Scrimlet(location) => {
                format!("_{location}._scrimlet._tcp")
            }
        }
    }

    /// Returns the full DNS name of this service
    pub fn srv_name(&self) -> String {
        format!("{}.{DNS_ZONE}", self.dns_name())
    }
}
