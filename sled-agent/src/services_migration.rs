// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! XXX-dap TODO-doc this is probably where we could use a big block comment
//! explaining the big picture here

use crate::params::ZoneType;
use camino::Utf8PathBuf;
use omicron_common::api::external::Generation;
use omicron_common::api::internal::shared::{
    NetworkInterface, SourceNatConfig,
};
use omicron_common::ledger::Ledgerable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter, Result as FormatResult};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use uuid::Uuid;

/// Describes a request to create a zone running one or more services.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct ServiceZoneRequest {
    // The UUID of the zone to be initialized.
    // TODO: Should this be removed? If we have UUIDs on the services, what's
    // the point of this?
    pub id: Uuid,
    // The type of the zone to be created.
    pub zone_type: ZoneType,
    // The addresses on which the service should listen for requests.
    pub addresses: Vec<Ipv6Addr>,
    // Datasets which should be managed by this service.
    #[serde(default)]
    pub dataset: Option<DatasetRequest>,
    // Services that should be run in the zone
    pub services: Vec<ServiceZoneService>,
}

impl ServiceZoneRequest {
    // The full name of the zone, if it was to be created as a zone.
    pub fn zone_name(&self) -> String {
        illumos_utils::running_zone::InstalledZone::get_zone_name(
            &self.zone_type.to_string(),
            self.zone_name_unique_identifier(),
        )
    }

    // The name of a unique identifier for the zone, if one is necessary.
    pub fn zone_name_unique_identifier(&self) -> Option<Uuid> {
        match &self.zone_type {
            // The switch zone is necessarily a singleton.
            ZoneType::Switch => None,
            // All other zones should be identified by their zone UUID.
            ZoneType::Clickhouse
            | ZoneType::ClickhouseKeeper
            | ZoneType::CockroachDb
            | ZoneType::Crucible
            | ZoneType::ExternalDns
            | ZoneType::InternalDns
            | ZoneType::Nexus
            | ZoneType::CruciblePantry
            | ZoneType::Ntp
            | ZoneType::Oximeter => Some(self.id),
        }
    }
}

/// Used to request that the Sled initialize a single service.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct ServiceZoneService {
    pub id: Uuid,
    pub details: ServiceType,
}

/// Describes service-specific parameters.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServiceType {
    Nexus {
        /// The address at which the internal nexus server is reachable.
        internal_address: SocketAddrV6,
        /// The address at which the external nexus server is reachable.
        external_ip: IpAddr,
        /// The service vNIC providing external connectivity using OPTE.
        nic: NetworkInterface,
        /// Whether Nexus's external endpoint should use TLS
        external_tls: bool,
        /// External DNS servers Nexus can use to resolve external hosts.
        external_dns_servers: Vec<IpAddr>,
    },
    ExternalDns {
        /// The address at which the external DNS server API is reachable.
        http_address: SocketAddrV6,
        /// The address at which the external DNS server is reachable.
        dns_address: SocketAddr,
        /// The service vNIC providing external connectivity using OPTE.
        nic: NetworkInterface,
    },
    InternalDns {
        http_address: SocketAddrV6,
        dns_address: SocketAddrV6,
        /// The addresses in the global zone which should be created
        ///
        /// For the DNS service, which exists outside the sleds's typical subnet - adding an
        /// address in the GZ is necessary to allow inter-zone traffic routing.
        gz_address: Ipv6Addr,

        /// The address is also identified with an auxiliary bit of information
        /// to ensure that the created global zone address can have a unique name.
        gz_address_index: u32,
    },
    Oximeter {
        address: SocketAddrV6,
    },
    CruciblePantry {
        address: SocketAddrV6,
    },
    BoundaryNtp {
        address: SocketAddrV6,
        ntp_servers: Vec<String>,
        dns_servers: Vec<IpAddr>,
        domain: Option<String>,
        /// The service vNIC providing outbound connectivity using OPTE.
        nic: NetworkInterface,
        /// The SNAT configuration for outbound connections.
        snat_cfg: SourceNatConfig,
    },
    InternalNtp {
        address: SocketAddrV6,
        ntp_servers: Vec<String>,
        dns_servers: Vec<IpAddr>,
        domain: Option<String>,
    },
    Clickhouse {
        address: SocketAddrV6,
    },
    ClickhouseKeeper {
        address: SocketAddrV6,
    },
    CockroachDb {
        address: SocketAddrV6,
    },
    Crucible {
        address: SocketAddrV6,
    },
}

impl std::fmt::Display for ServiceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        match self {
            ServiceType::Nexus { .. } => write!(f, "nexus"),
            ServiceType::ExternalDns { .. } => write!(f, "external_dns"),
            ServiceType::InternalDns { .. } => write!(f, "internal_dns"),
            ServiceType::Oximeter { .. } => write!(f, "oximeter"),
            ServiceType::CruciblePantry { .. } => write!(f, "crucible/pantry"),
            ServiceType::BoundaryNtp { .. }
            | ServiceType::InternalNtp { .. } => write!(f, "ntp"),

            ServiceType::Clickhouse { .. } => write!(f, "clickhouse"),
            ServiceType::ClickhouseKeeper { .. } => {
                write!(f, "clickhouse_keeper")
            }
            ServiceType::CockroachDb { .. } => write!(f, "cockroachdb"),
            ServiceType::Crucible { .. } => write!(f, "crucible"),
        }
    }
}

/// Used to request that the Sled initialize multiple services.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ServiceEnsureBody {
    pub services: Vec<ServiceZoneRequest>,
}

/// Describes a request to provision a specific dataset
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct DatasetRequest {
    pub id: Uuid,
    pub name: crate::storage::dataset::DatasetName,
    pub service_address: SocketAddrV6,
}

// This struct represents the combo of "what zone did you ask for" + "where did
// we put it".
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct ZoneRequestV1 {
    zone: ServiceZoneRequest,
    // TODO: Consider collapsing "root" into ServiceZoneRequest
    #[schemars(with = "String")]
    root: Utf8PathBuf,
}

// The filename of the ledger, within the provided directory.
const SERVICES_LEDGER_FILENAME: &str = "services.json";

// A wrapper around `ZoneRequest`, which allows it to be serialized
// to a JSON file.
// XXX-dap TODO-doc
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct AllZoneRequestsV1 {
    generation: Generation,
    requests: Vec<ZoneRequestV1>,
}

impl Default for AllZoneRequestsV1 {
    fn default() -> Self {
        Self { generation: Generation::new(), requests: vec![] }
    }
}

impl Ledgerable for AllZoneRequestsV1 {
    fn is_newer_than(&self, other: &AllZoneRequestsV1) -> bool {
        self.generation >= other.generation
    }

    fn generation_bump(&mut self) {
        self.generation = self.generation.next();
    }
}

#[cfg(test)]
mod test {
    use super::AllZoneRequestsV1;

    #[test]
    fn test_all_zone_requests_schema() {
        let schema = schemars::schema_for!(AllZoneRequestsV1);
        expectorate::assert_contents(
            "../schema/all-zone-requests.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }
}
