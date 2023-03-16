// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Plan generation for "where should services be initialized".

use crate::params::{
    DatasetEnsureBody, ServiceType, ServiceZoneRequest, ZoneType,
};
use crate::rack_setup::config::SetupServiceConfig as Config;
use crate::rack_setup::sled_interface::SledInterface;
use omicron_common::address::{
    get_switch_zone_address, Ipv6Subnet, ReservedRackSubnet, DNS_PORT,
    DNS_SERVER_PORT, RSS_RESERVED_ADDRESSES, SLED_PREFIX,
};
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::HashMap;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

// The number of Nexus instances to create from RSS.
const NEXUS_COUNT: usize = 1;

// The number of CRDB instances to create from RSS.
const CRDB_COUNT: usize = 1;

// TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
// when Nexus provisions Oximeter.
const OXIMETER_COUNT: usize = 1;
// TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
// when Nexus provisions Clickhouse.
const CLICKHOUSE_COUNT: usize = 1;
// TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove.
// when Nexus provisions the Pantry.
const PANTRY_COUNT: usize = 1;

fn rss_service_plan_path(path: &Path) -> PathBuf {
    path.join("rss-service-plan.toml")
}

/// Describes errors which may occur while generating a plan for services.
#[derive(Error, Debug)]
pub enum PlanError {
    #[error("I/O error while {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Cannot deserialize TOML file at {path}: {err}")]
    Toml { path: PathBuf, err: toml::de::Error },

    #[error(transparent)]
    Sled(#[from] crate::rack_setup::sled_interface::Error),
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SledRequest {
    /// Datasets to be created.
    #[serde(default, rename = "dataset")]
    pub datasets: Vec<DatasetEnsureBody>,

    /// Services to be instantiated.
    #[serde(default, rename = "service")]
    pub services: Vec<ServiceZoneRequest>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Plan {
    pub services: HashMap<SocketAddrV6, SledRequest>,
}

impl Plan {
    pub async fn load(
        log: &Logger,
        directory: &Path,
    ) -> Result<Option<Plan>, PlanError> {
        // If we already created a plan for this RSS to allocate
        // services to sleds, re-use that existing plan.
        let rss_service_plan_path = rss_service_plan_path(directory);
        if rss_service_plan_path.exists() {
            info!(log, "RSS plan already created, loading from file");

            let plan: Self = toml::from_str(
                &tokio::fs::read_to_string(&rss_service_plan_path)
                    .await
                    .map_err(|err| PlanError::Io {
                        message: format!(
                            "Loading RSS plan {rss_service_plan_path:?}"
                        ),
                        err,
                    })?,
            )
            .map_err(|err| PlanError::Toml {
                path: rss_service_plan_path,
                err,
            })?;
            Ok(Some(plan))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn create(
        log: &Logger,
        directory: &Path,
        sleds: &impl SledInterface,
        config: &Config,
        sled_addrs: &Vec<SocketAddrV6>,
    ) -> Result<Self, PlanError> {
        let reserved_rack_subnet = ReservedRackSubnet::new(config.az_subnet());
        let dns_subnets = reserved_rack_subnet.get_dns_subnets();

        let mut allocations = vec![];

        for idx in 0..sled_addrs.len() {
            let sled_address = sled_addrs[idx];
            let subnet: Ipv6Subnet<SLED_PREFIX> =
                Ipv6Subnet::<SLED_PREFIX>::new(*sled_address.ip());
            let u2_zpools = sleds.get_u2_zpools(log, sled_address).await?;
            let mut addr_alloc = AddressBumpAllocator::new(subnet);

            let mut request = SledRequest::default();

            // The first enumerated sleds get assigned the responsibility
            // of hosting Nexus.
            if idx < NEXUS_COUNT {
                let address = addr_alloc.next().expect("Not enough addrs");
                request.services.push(ServiceZoneRequest {
                    id: Uuid::new_v4(),
                    zone_type: ZoneType::Nexus,
                    addresses: vec![address],
                    gz_addresses: vec![],
                    services: vec![ServiceType::Nexus {
                        internal_ip: address,
                        external_ip: config.nexus_external_address,
                    }],
                })
            }

            // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
            if idx < OXIMETER_COUNT {
                let address = addr_alloc.next().expect("Not enough addrs");
                request.services.push(ServiceZoneRequest {
                    id: Uuid::new_v4(),
                    zone_type: ZoneType::Oximeter,
                    addresses: vec![address],
                    gz_addresses: vec![],
                    services: vec![ServiceType::Oximeter],
                })
            }

            // The first enumerated sleds host the CRDB datasets, using
            // zpools described from the underlying config file.
            if idx < CRDB_COUNT {
                let address = SocketAddrV6::new(
                    addr_alloc.next().expect("Not enough addrs"),
                    omicron_common::address::COCKROACH_PORT,
                    0,
                    0,
                );
                request.datasets.push(DatasetEnsureBody {
                    id: Uuid::new_v4(),
                    zpool_id: u2_zpools[0],
                    dataset_kind: crate::params::DatasetKind::CockroachDb {
                        all_addresses: vec![address],
                    },
                    address,
                });
            }

            // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
            if idx < CLICKHOUSE_COUNT {
                let address = SocketAddrV6::new(
                    addr_alloc.next().expect("Not enough addrs"),
                    omicron_common::address::CLICKHOUSE_PORT,
                    0,
                    0,
                );
                request.datasets.push(DatasetEnsureBody {
                    id: Uuid::new_v4(),
                    zpool_id: u2_zpools[0],
                    dataset_kind: crate::params::DatasetKind::Clickhouse,
                    address,
                });
            }

            // Each zpool gets a crucible zone.
            //
            // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
            for zpool_id in u2_zpools {
                let address = SocketAddrV6::new(
                    addr_alloc.next().expect("Not enough addrs"),
                    omicron_common::address::CRUCIBLE_PORT,
                    0,
                    0,
                );
                request.datasets.push(DatasetEnsureBody {
                    id: Uuid::new_v4(),
                    zpool_id,
                    dataset_kind: crate::params::DatasetKind::Crucible,
                    address,
                });
            }

            // The first enumerated sleds get assigned the additional
            // responsibility of being internal DNS servers.
            if idx < dns_subnets.len() {
                let dns_subnet = &dns_subnets[idx];
                let dns_addr = dns_subnet.dns_address().ip();
                request.services.push(ServiceZoneRequest {
                    id: Uuid::new_v4(),
                    zone_type: ZoneType::InternalDNS,
                    addresses: vec![dns_addr],
                    gz_addresses: vec![dns_subnet.gz_address().ip()],
                    services: vec![ServiceType::InternalDns {
                        server_address: SocketAddrV6::new(
                            dns_addr,
                            DNS_SERVER_PORT,
                            0,
                            0,
                        ),
                        dns_address: SocketAddrV6::new(
                            dns_addr, DNS_PORT, 0, 0,
                        ),
                    }],
                });
            }

            // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
            if idx < PANTRY_COUNT {
                let address = addr_alloc.next().expect("Not enough addrs");
                request.services.push(ServiceZoneRequest {
                    id: Uuid::new_v4(),
                    zone_type: ZoneType::CruciblePantry,
                    addresses: vec![address],
                    gz_addresses: vec![],
                    services: vec![ServiceType::CruciblePantry],
                })
            }

            allocations.push((sled_address, request));
        }

        let mut services = std::collections::HashMap::new();
        for (addr, allocation) in allocations {
            services.insert(addr, allocation);
        }

        let plan = Self { services };

        // Once we've constructed a plan, write it down to durable storage.
        let serialized_plan =
            toml::Value::try_from(&plan).unwrap_or_else(|e| {
                panic!("Cannot serialize configuration: {:#?}: {}", plan, e)
            });
        let plan_str = toml::to_string(&serialized_plan)
            .expect("Cannot turn config to string");

        info!(log, "Plan serialized as: {}", plan_str);
        let path = rss_service_plan_path(directory);
        tokio::fs::write(&path, plan_str).await.map_err(|err| {
            PlanError::Io {
                message: format!("Storing RSS service plan to {path:?}"),
                err,
            }
        })?;
        info!(log, "Service plan written to storage");

        Ok(plan)
    }
}

struct AddressBumpAllocator {
    last_addr: Ipv6Addr,
}

impl AddressBumpAllocator {
    fn new(subnet: Ipv6Subnet<SLED_PREFIX>) -> Self {
        Self { last_addr: get_switch_zone_address(subnet) }
    }

    fn next(&mut self) -> Option<Ipv6Addr> {
        let mut segments: [u16; 8] = self.last_addr.segments();
        segments[7] = segments[7].checked_add(1)?;
        if segments[7] > RSS_RESERVED_ADDRESSES {
            return None;
        }
        self.last_addr = Ipv6Addr::from(segments);
        Some(self.last_addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPECTED_RESERVED_ADDRESSES: u16 = 2;
    const EXPECTED_USABLE_ADDRESSES: u16 =
        RSS_RESERVED_ADDRESSES - EXPECTED_RESERVED_ADDRESSES;

    #[test]
    fn bump_allocator_basics() {
        let address = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0);
        let subnet = Ipv6Subnet::<SLED_PREFIX>::new(address);

        let mut allocator = AddressBumpAllocator::new(subnet);
        assert_eq!(
            allocator.next().unwrap(),
            Ipv6Addr::new(
                0xfd00,
                0,
                0,
                0,
                0,
                0,
                0,
                EXPECTED_RESERVED_ADDRESSES + 1
            ),
        );
        assert_eq!(
            allocator.next().unwrap(),
            Ipv6Addr::new(
                0xfd00,
                0,
                0,
                0,
                0,
                0,
                0,
                EXPECTED_RESERVED_ADDRESSES + 2
            ),
        );
    }

    #[test]
    fn bump_allocator_exhaustion() {
        let address = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0);
        let subnet = Ipv6Subnet::<SLED_PREFIX>::new(address);

        let mut allocator = AddressBumpAllocator::new(subnet);
        for i in 0..EXPECTED_USABLE_ADDRESSES {
            assert!(
                allocator.next().is_some(),
                "Could not allocate {i}-th address"
            );
        }
        assert!(allocator.next().is_none(), "Expected allocation to fail");
    }
}
