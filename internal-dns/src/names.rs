// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Well-known DNS names and related types for internal DNS (see RFD 248)

use omicron_common::api::internal::shared::SwitchLocation;
use std::str::{FromStr, Split};
use thiserror::Error;
use uuid::Uuid;

/// Name for the control plane DNS zone
pub const DNS_ZONE: &str = "control-plane.oxide.internal";

/// Name for the delegated external DNS zone that's used in testing and
/// development
pub const DNS_ZONE_EXTERNAL_TESTING: &str = "oxide-dev.test";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseServiceNameError {
    #[error("Failed to parse UUID")]
    InvalidUuid(#[from] uuid::Error),
    #[error("DNS name missing expected components")]
    MissingComponent,
    #[error("Missing expected '_' in prefix")]
    MissingUnderscorePrefix,
    #[error("Unknown service type: {0}")]
    UnknownServiceType(String),
    #[error("Unexpected component (expected {expected}, found {found})")]
    UnexpectedComponent {
        expected: String,
        found: String,
    }
}

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
    pub fn service_kind(&self) -> &'static str {
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
    pub(crate) fn dns_name(&self) -> String {
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

    /// Parses the [ServiceName] from the DNS name (ignoring the zone).
    ///
    /// This should be identical to the output of [Self::dns_name].
    pub fn from_dns_name(s: &str) -> Result<Self, ParseServiceNameError> {
        let mut parts = s.split('.');
        let first = parts.next()
            .ok_or_else(|| ParseServiceNameError::MissingComponent)?
            .strip_prefix('_')
            .ok_or_else(|| ParseServiceNameError::MissingUnderscorePrefix)?;

        // A helper to consume a whole component of the path
        let parse_exact = |parts: &mut Split<char>, expected: &str| {
            let part = parts.next().ok_or_else(|| ParseServiceNameError::MissingComponent)?;
            if part != expected {
                return Err(ParseServiceNameError::UnexpectedComponent {
                    expected: expected.to_string(),
                    found: part.to_string(),
                });
            }
            Ok(())
        };

        // A helper to consume a whole component of the path as a UUID
        let parse_uuid = |parts: &mut Split<char>| -> Result<Uuid, ParseServiceNameError> {
            let part = parts.next().ok_or_else(|| ParseServiceNameError::MissingComponent)?;
            Ok(Uuid::from_str(part)?)
        };

        let name = match first {
            "clickhouse" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::Clickhouse
            },
            "clickhouse-keeper" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::ClickhouseKeeper
            },
            "cockroach" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::Cockroach
            },
            "external-dns" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::ExternalDns
            },
            "nameservice" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::InternalDns
            },
            "nexus" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::Nexus
            },
            "oximeter" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::Oximeter
            },
            "mgs" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::ManagementGatewayService
            },
            "wicketd" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::Wicketd
            },
            "dendrite" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::Dendrite
            },
            "tfport" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::Tfport
            },
            "crucible-pantry" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::CruciblePantry
            },
            "boundary-ntp" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::BoundaryNtp
            },
            "internal-ntp" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::InternalNtp
            },
            "maghemite" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::Maghemite
            },
            "mgd" => {
                parse_exact(&mut parts, "_tcp")?;
                Self::Mgd
            },
            "sledagent" => {
                parse_exact(&mut parts, "_tcp")?;
                let id = parse_uuid(&mut parts)?;
                Self::SledAgent(id)
            },
            "crucible" => {
                parse_exact(&mut parts, "_tcp")?;
                let id = parse_uuid(&mut parts)?;
                Self::Crucible(id)
            },
            other => {
                let switch_location = SwitchLocation::from_str(other)
                    .map_err(|_| ParseServiceNameError::UnknownServiceType(other.to_string()))?;
                parse_exact(&mut parts, "_scrimlet")?;
                parse_exact(&mut parts, "_tcp")?;
                Self::Scrimlet(switch_location)
            }
        };
        Ok(name)
    }

    /// Returns the full DNS name of this service
    pub fn srv_name(&self) -> String {
        format!("{}.{DNS_ZONE}", self.dns_name())
    }
}

// TODO: Tests!
