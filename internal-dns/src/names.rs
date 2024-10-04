// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Well-known DNS names and related types for internal DNS (see RFD 248)

use omicron_uuid_kinds::{OmicronZoneUuid, SledUuid};

/// Name for the special boundary NTP DNS name
///
/// chrony does not support SRV records. This name resolves to AAAA records for
/// each boundary NTP zone, and then we can point internal NTP chrony instances
/// at this name for it to find the boundary NTP zones.
pub const BOUNDARY_NTP_DNS_NAME: &str = "boundary-ntp";

/// Name for the control plane DNS zone
pub const DNS_ZONE: &str = "control-plane.oxide.internal";

/// Name for the delegated external DNS zone that's used in testing and
/// development
pub const DNS_ZONE_EXTERNAL_TESTING: &str = "oxide-dev.test";

/// Names of services within the control plane
#[derive(Clone, Copy, Debug, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub enum ServiceName {
    /// The HTTP interface to a single-node ClickHouse server.
    Clickhouse,
    /// The HTTP interface for managing replicated clickhouse servers and keepers
    ClickhouseAdmin,
    /// The native TCP interface to a ClickHouse server.
    ///
    /// NOTE: This is used for either single-node or a replicated cluster.
    ClickhouseNative,
    /// The TCP interface to a ClickHouse Keeper server.
    ClickhouseKeeper,
    /// The HTTP interface to a replicated ClickHouse server.
    ClickhouseServer,
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
    SledAgent(SledUuid),
    Crucible(OmicronZoneUuid),
    BoundaryNtp,
    InternalNtp,
    Maghemite, //TODO change to Dpd - maghemite has several services.
    Mgd,
}

impl ServiceName {
    fn service_kind(&self) -> &'static str {
        match self {
            ServiceName::Clickhouse => "clickhouse",
            ServiceName::ClickhouseAdmin => "clickhouse-admin",
            ServiceName::ClickhouseNative => "clickhouse-native",
            ServiceName::ClickhouseKeeper => "clickhouse-keeper",
            ServiceName::ClickhouseServer => "clickhouse-server",
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
        }
    }

    /// Returns the DNS name for this service, ignoring the zone part of the DNS
    /// name
    pub fn dns_name(&self) -> String {
        match self {
            ServiceName::Clickhouse
            | ServiceName::ClickhouseAdmin
            | ServiceName::ClickhouseNative
            | ServiceName::ClickhouseKeeper
            | ServiceName::ClickhouseServer
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
        }
    }

    /// Returns the full DNS name of this service
    pub fn srv_name(&self) -> String {
        format!("{}.{DNS_ZONE}", self.dns_name())
    }
}
