// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! XXX-dap TODO-doc this is probably where we could use a big block comment
//! explaining the big picture here

use crate::params::{
    DatasetKind, OmicronZoneConfig, OmicronZoneDataset, OmicronZoneType,
    ZoneType,
};
use crate::services::{OmicronZoneConfigComplete, ZonesConfig};
use anyhow::{anyhow, ensure, Context};
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

impl DatasetRequest {
    fn to_omicron_zone_dataset(
        self,
        kind: DatasetKind,
        service_address: SocketAddrV6,
    ) -> Result<OmicronZoneDataset, anyhow::Error> {
        ensure!(
            kind == *self.name.dataset(),
            "expected dataset kind {:?}, found {:?}",
            kind,
            self.name.dataset(),
        );

        ensure!(
            self.service_address == service_address,
            "expected dataset kind {:?} service address to be {}, found {}",
            kind,
            service_address,
            self.service_address,
        );

        Ok(OmicronZoneDataset { pool_name: self.name.pool().clone() })
    }
}

// This struct represents the combo of "what zone did you ask for" + "where did
// we put it".
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct ServiceRequest {
    zone: ServiceZoneRequest,
    // TODO: Consider collapsing "root" into ServiceZoneRequest
    #[schemars(with = "String")]
    root: Utf8PathBuf,
}

// The filename of the ledger, within the provided directory.
pub const SERVICES_LEDGER_FILENAME: &str = "services.json";

// A wrapper around `ZoneRequest`, which allows it to be serialized
// to a JSON file.
// XXX-dap TODO-doc
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct AllServiceRequests {
    generation: Generation,
    requests: Vec<ServiceRequest>,
}

impl Default for AllServiceRequests {
    fn default() -> Self {
        Self { generation: Generation::new(), requests: vec![] }
    }
}

impl Ledgerable for AllServiceRequests {
    fn is_newer_than(&self, other: &AllServiceRequests) -> bool {
        self.generation >= other.generation
    }

    fn generation_bump(&mut self) {
        self.generation = self.generation.next();
    }
}

impl TryFrom<AllServiceRequests> for ZonesConfig {
    type Error = anyhow::Error;

    fn try_from(input: AllServiceRequests) -> Result<Self, Self::Error> {
        // XXX-dap starting to think that the generation in OmicronZonesConfig
        // (at least the one stored on disk) ought to be an Option or something?
        // There's no correct value here.  Any Nexus one needs to be treated as
        // newer.  The generation from the ledger does not matter.
        // XXX-dap TODO-doc explain why the generation from the ledger does not
        // matter here (they _all_ predate the Nexus-provided one).
        let zones = input
            .requests
            .into_iter()
            .map(OmicronZoneConfigComplete::try_from)
            .collect::<Result<Vec<_>, _>>()
            .context("mapping `AllServiceRequests` to `ZonesConfig`")?;
        Ok(ZonesConfig { generation: Generation::new(), zones })
    }
}

impl TryFrom<ServiceRequest> for OmicronZoneConfigComplete {
    type Error = anyhow::Error;

    fn try_from(input: ServiceRequest) -> Result<Self, Self::Error> {
        Ok(OmicronZoneConfigComplete {
            zone: OmicronZoneConfig::try_from(input.zone)?,
            root: input.root,
        })
    }
}

impl TryFrom<ServiceZoneRequest> for OmicronZoneConfig {
    type Error = anyhow::Error;

    fn try_from(input: ServiceZoneRequest) -> Result<Self, Self::Error> {
        let error_context = || {
            format!(
                "zone {} (type {:?})",
                input.id,
                input.zone_type.to_string()
            )
        };

        // Historically, this type was used to describe two distinct kinds of
        // thing:
        //
        // 1. an "Omicron" zone: Clickhouse, CockroachDb, Nexus, etc.  We call
        //    these Omicron zones because they're managed by the control plane
        //    (Omicron).  Nexus knows about these, stores information in
        //    CockroachDB about them, and is responsible for using Sled Agent
        //    APIs to configure these zones.
        //
        // 2. a "sled-local" zone.  The only such zone is the "switch" zone.
        //    This is not really known to Nexus nor exposed outside Sled Agent.
        //    It's configured either based on Sled Agent's config file or else
        //    autodetection of whether this system _is_ a Scrimlet.
        //
        // All of the types in this file describe the ledgered configuration of
        // the Omicron zones.  We don't care about the switch zone here.  Even
        // for Omicron zones, the `ServiceZoneRequest` type is much more general
        // than was strictly necessary to represent the kinds of zones we
        // defined in practice.  The more constrained schema is described by
        // `OmicronZoneConfig`.  This function verifies that the structures we
        // find conform to that more constrained schema.
        //
        // Many of these properties were determined by code inspection.  They
        // could be wrong!  But we've tried hard to make sure we're not wrong.

        match input.zone_type {
            ZoneType::Clickhouse
            | ZoneType::ClickhouseKeeper
            | ZoneType::CockroachDb
            | ZoneType::CruciblePantry
            | ZoneType::Crucible
            | ZoneType::ExternalDns
            | ZoneType::InternalDns
            | ZoneType::Nexus
            | ZoneType::Ntp
            | ZoneType::Oximeter => (),
            ZoneType::Switch => {
                return Err(anyhow!("unsupported zone type"))
                    .with_context(error_context)
            }
        }

        let id = input.id;

        // In production systems, Omicron zones only ever had exactly one
        // address here.  Multiple addresses were used for the "switch" zone,
        // which cannot appear here.
        if input.addresses.len() != 1 {
            return Err(anyhow!(
                "expected exactly one address, found {}",
                input.addresses.len()
            ))
            .with_context(error_context);
        }

        let underlay_address = input.addresses[0];

        // In production systems, Omicron zones only ever had exactly one
        // "service" inside them.  (Multiple services were only supported for
        // the "switch" zone and for Omicron zones in pre-release versions of
        // Omicron, neither of which we expect to see here.)
        if input.services.len() != 1 {
            return Err(anyhow!(
                "expected exactly one service, found {}",
                input.services.len(),
            ))
            .with_context(error_context);
        }

        let service = input.services.into_iter().next().unwrap();

        // The id for the one service we found must match the overall request
        // id.
        if service.id != input.id {
            return Err(anyhow!(
                "expected service id ({}) to match id ({})",
                service.id,
                input.id,
            ))
            .with_context(error_context);
        }

        // If there's a dataset, its id must match the overall request id.
        let has_dataset = input.dataset.is_some();
        if let Some(dataset) = &input.dataset {
            if dataset.id != input.id {
                return Err(anyhow!(
                    "expected dataset id ({}) to match id ({})",
                    dataset.id,
                    input.id,
                ))
                .with_context(error_context);
            }
        }

        let dataset_request = input
            .dataset
            .ok_or_else(|| anyhow!("missing dataset"))
            .with_context(error_context);

        let zone_type = match service.details {
            ServiceType::Nexus {
                internal_address,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => OmicronZoneType::Nexus {
                internal_address,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            },
            ServiceType::ExternalDns { http_address, dns_address, nic } => {
                OmicronZoneType::ExternalDns {
                    dataset: dataset_request?.to_omicron_zone_dataset(
                        DatasetKind::ExternalDns,
                        http_address,
                    )?,
                    http_address,
                    dns_address,
                    nic,
                }
            }
            ServiceType::InternalDns {
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            } => OmicronZoneType::InternalDns {
                dataset: dataset_request?.to_omicron_zone_dataset(
                    DatasetKind::InternalDns,
                    http_address,
                )?,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            },
            ServiceType::Oximeter { address } => {
                OmicronZoneType::Oximeter { address }
            }
            ServiceType::CruciblePantry { address } => {
                OmicronZoneType::CruciblePantry { address }
            }
            ServiceType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            },
            ServiceType::InternalNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
            } => OmicronZoneType::InternalNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
            },
            ServiceType::Clickhouse { address } => {
                OmicronZoneType::Clickhouse {
                    address,
                    dataset: dataset_request?.to_omicron_zone_dataset(
                        DatasetKind::Clickhouse,
                        address,
                    )?,
                }
            }
            ServiceType::ClickhouseKeeper { address } => {
                OmicronZoneType::ClickhouseKeeper {
                    address,
                    dataset: dataset_request?.to_omicron_zone_dataset(
                        DatasetKind::Clickhouse,
                        address,
                    )?,
                }
            }
            ServiceType::CockroachDb { address } => {
                OmicronZoneType::CockroachDb {
                    address,
                    dataset: dataset_request?.to_omicron_zone_dataset(
                        DatasetKind::CockroachDb,
                        address,
                    )?,
                }
            }
            ServiceType::Crucible { address } => OmicronZoneType::Crucible {
                address,
                dataset: dataset_request?
                    .to_omicron_zone_dataset(DatasetKind::Crucible, address)?,
            },
        };

        if zone_type.dataset_name().is_none() && has_dataset {
            // This indicates that the legacy form specified a dataset for a
            // zone type that we do not (today) believe should have one.  This
            // should be impossible.  If it happens, we need to re-evaluate our
            // assumptions in designing `OmicronZoneType`.
            return Err(anyhow!("found dataset that went unused"))
                .with_context(error_context);
        }

        Ok(OmicronZoneConfig { id, underlay_address, zone_type })
    }
}

#[cfg(test)]
mod test {
    use super::AllServiceRequests;

    // XXX-dap add similar test for new ledger
    #[test]
    fn test_all_services_requests_schema() {
        let schema = schemars::schema_for!(AllServiceRequests);
        expectorate::assert_contents(
            "../schema/all-zone-requests.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }
}
