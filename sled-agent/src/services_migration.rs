// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled Agents are responsible for running zones that make up much of the
//! control plane (Omicron).  Configuration for these zones is owned by the
//! control plane, but that configuration must be persisted locally in order to
//! support cold boot of the control plane.  (The control plane can't very well
//! tell sled agents what to run if it's not online yet!)
//!
//! Historically, these configurations were represented as an
//! `AllZonesRequests`, which contains a bunch of `ZoneRequest`s, each
//! containing a `ServiceZoneRequest`.  This last structure was quite general
//! and made it possible to express a world of configurations that are not
//! actually valid.  To avoid spreading extra complexity, these structures were
//! replaced with `OmicronZonesConfigLocal` and `OmicronZonesConfig`,
//! respectively.  Upgrading production systems across this change requires
//! migrating any locally-stored configuration in the old format into the new
//! one.
//!
//! This file defines these old-format types and functions to convert them to
//! the new types, solely to perform that migration.  We can remove all this
//! when we're satified that all deployed systems that we care about have moved
//! past this change.

use crate::params::{
    OmicronZoneConfig, OmicronZoneDataset, OmicronZoneType, ZoneType,
    OMICRON_ZONES_CONFIG_INITIAL_VERSION,
};
use crate::services::{OmicronZoneConfigLocal, OmicronZonesConfigLocal};
use anyhow::{anyhow, ensure, Context};
use camino::Utf8PathBuf;
use omicron_common::api::external::Generation;
use omicron_common::api::internal::shared::{
    NetworkInterface, SourceNatConfig,
};
use omicron_common::ledger::Ledgerable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_storage::dataset::{DatasetKind, DatasetName};
use std::fmt::Debug;
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use uuid::Uuid;

/// The filename of the ledger containing this old-format configuration.
pub const SERVICES_LEDGER_FILENAME: &str = "services.json";

/// A wrapper around `ZoneRequest` that allows it to be serialized to a JSON
/// file.
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct AllZoneRequests {
    /// ledger generation (not an Omicron-provided generation)
    generation: Generation,
    requests: Vec<ZoneRequest>,
}

impl Default for AllZoneRequests {
    fn default() -> Self {
        Self { generation: Generation::new(), requests: vec![] }
    }
}

impl Ledgerable for AllZoneRequests {
    fn is_newer_than(&self, other: &AllZoneRequests) -> bool {
        self.generation >= other.generation
    }

    fn generation_bump(&mut self) {
        self.generation = self.generation.next();
    }
}

impl TryFrom<AllZoneRequests> for OmicronZonesConfigLocal {
    type Error = anyhow::Error;

    fn try_from(input: AllZoneRequests) -> Result<Self, Self::Error> {
        // The Omicron version number that we choose here (2) deserves some
        // explanation.
        //
        // This is supposed to be the control-plane-issued version number for
        // this configuration.  But any configuration that we're converting here
        // predates the point where the control plane issued version numbers at
        // all.  So what should we assign it?  Well, what are the constraints?
        //
        // - It must be newer than version 1 because version 1 canonically
        //   represents the initial state of having no zones deployed.  If we
        //   used version 1 here, any code could ignore this configuration on
        //   the grounds that it's no newer than what it already has.  (The
        //   contents of a given version are supposed to be immutable.)
        //
        // - It should be older than anything else that the control plane might
        //   try to send us so that if the control plane wants to change
        //   anything, we won't ignore its request because we think this
        //   configuration is newer.  But really this has to be the control
        //   plane's responsibility, not ours.  That is: Nexus needs to ask us
        //   what our version number is and subsequent configurations should use
        //   newer version numbers.  It's not a great plan for it to assume
        //   anything about the version numbers deployed on sleds whose
        //   configurations it's never seen.  (In practice, newly deployed
        //   systems currently wind up with generation 5, so it _could_ choose
        //   something like 6 to start with -- or some larger number to leave
        //   some buffer.)
        //
        // In summary, 2 seems fine.
        let omicron_version =
            Generation::from(OMICRON_ZONES_CONFIG_INITIAL_VERSION).next();

        // The ledger version doesn't really matter.  In case it's useful, we
        // pick the version from the ledger that we loaded.
        let ledger_version = input.generation;

        let zones = input
            .requests
            .into_iter()
            .map(OmicronZoneConfigLocal::try_from)
            .collect::<Result<Vec<_>, _>>()
            .context(
                "mapping `AllZoneRequests` to `OmicronZonesConfigLocal`",
            )?;

        Ok(OmicronZonesConfigLocal { omicron_version, ledger_version, zones })
    }
}

/// This struct represents the combo of "what zone did you ask for" + "where did
/// we put it".
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct ZoneRequest {
    zone: ServiceZoneRequest,
    #[schemars(with = "String")]
    root: Utf8PathBuf,
}

impl TryFrom<ZoneRequest> for OmicronZoneConfigLocal {
    type Error = anyhow::Error;

    fn try_from(input: ZoneRequest) -> Result<Self, Self::Error> {
        Ok(OmicronZoneConfigLocal {
            zone: OmicronZoneConfig::try_from(input.zone)?,
            root: input.root,
        })
    }
}

/// Describes a request to create a zone running one or more services.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
struct ServiceZoneRequest {
    // The UUID of the zone to be initialized.
    id: Uuid,
    // The type of the zone to be created.
    zone_type: ZoneType,
    // The addresses on which the service should listen for requests.
    addresses: Vec<Ipv6Addr>,
    // Datasets which should be managed by this service.
    #[serde(default)]
    dataset: Option<DatasetRequest>,
    // Services that should be run in the zone
    services: Vec<ServiceZoneService>,
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

/// Used to request that the Sled initialize a single service.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
struct ServiceZoneService {
    id: Uuid,
    details: ServiceType,
}

/// Describes service-specific parameters.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServiceType {
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
        /// For the DNS service, which exists outside the sleds's typical subnet
        /// - adding an address in the GZ is necessary to allow inter-zone
        /// traffic routing.
        gz_address: Ipv6Addr,

        /// The address is also identified with an auxiliary bit of information
        /// to ensure that the created global zone address can have a unique
        /// name.
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

/// Describes a request to provision a specific dataset
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
struct DatasetRequest {
    id: Uuid,
    name: DatasetName,
    service_address: SocketAddrV6,
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

#[cfg(test)]
mod test {
    use super::AllZoneRequests;

    #[test]
    fn test_all_services_requests_schema() {
        let schema = schemars::schema_for!(AllZoneRequests);
        expectorate::assert_contents(
            "../schema/all-zone-requests.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }
}
