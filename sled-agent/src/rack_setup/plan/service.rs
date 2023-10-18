// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Plan generation for "where should services be initialized".

use crate::bootstrap::params::StartSledAgentRequest;
use crate::params::{
    DatasetKind, DatasetRequest, ServiceType, ServiceZoneRequest,
    ServiceZoneService, ZoneType,
};
use crate::rack_setup::config::SetupServiceConfig as Config;
use crate::storage::dataset::{DatasetArcPrimaryCache, DatasetName};
use crate::storage_manager::StorageResources;
use camino::Utf8PathBuf;
use dns_service_client::types::DnsConfigParams;
use illumos_utils::zpool::ZpoolName;
use internal_dns::{ServiceName, DNS_ZONE};
use omicron_common::address::{
    get_sled_address, get_switch_zone_address, Ipv6Subnet, ReservedRackSubnet,
    DENDRITE_PORT, DNS_HTTP_PORT, DNS_PORT, DNS_REDUNDANCY, MAX_DNS_REDUNDANCY,
    MGS_PORT, NTP_PORT, NUM_SOURCE_NAT_PORTS, RSS_RESERVED_ADDRESSES,
    SLED_PREFIX,
};
use omicron_common::api::external::{MacAddr, Vni};
use omicron_common::api::internal::shared::{
    NetworkInterface, NetworkInterfaceKind, SourceNatConfig,
};
use omicron_common::backoff::{
    retry_notify_ext, retry_policy_internal_service_aggressive, BackoffError,
};
use omicron_common::ledger::{self, Ledger, Ledgerable};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_client::{
    types as SledAgentTypes, Client as SledAgentClient, Error as SledAgentError,
};
use slog::Logger;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::num::Wrapping;
use thiserror::Error;
use uuid::Uuid;

// The number of boundary NTP servers to create from RSS.
const BOUNDARY_NTP_COUNT: usize = 2;

// The number of Nexus instances to create from RSS.
const NEXUS_COUNT: usize = 3;

// The number of CRDB instances to create from RSS.
const CRDB_COUNT: usize = 5;

// TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
// when Nexus provisions Oximeter.
const OXIMETER_COUNT: usize = 1;
// TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
// when Nexus provisions Clickhouse.
// TODO(https://github.com/oxidecomputer/omicron/issues/4000): Set to 2 once we enable replicated ClickHouse
const CLICKHOUSE_COUNT: usize = 1;
// TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
// when Nexus provisions Clickhouse keeper.
// TODO(https://github.com/oxidecomputer/omicron/issues/4000): Set to 3 once we enable replicated ClickHouse
const CLICKHOUSE_KEEPER_COUNT: usize = 0;
// TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove.
// when Nexus provisions Crucible.
const MINIMUM_U2_ZPOOL_COUNT: usize = 3;
// TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove.
// when Nexus provisions the Pantry.
const PANTRY_COUNT: usize = 3;

/// Describes errors which may occur while generating a plan for services.
#[derive(Error, Debug)]
pub enum PlanError {
    #[error("I/O error while {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Failed to access ledger: {0}")]
    Ledger(#[from] ledger::Error),

    #[error("Error making HTTP request to Sled Agent: {0}")]
    SledApi(#[from] SledAgentError<SledAgentTypes::Error>),

    #[error("Error initializing sled via sled-agent: {0}")]
    SledInitialization(String),

    #[error("Failed to allocate service IP for service: {0}")]
    ServiceIp(&'static str),

    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(reqwest::Error),

    #[error("Ran out of sleds / U2 storage pools")]
    NotEnoughSleds,
}

#[derive(
    Clone, Debug, Default, Deserialize, Serialize, PartialEq, JsonSchema,
)]
pub struct SledRequest {
    /// Services to be instantiated.
    #[serde(default, rename = "service")]
    pub services: Vec<ServiceZoneRequest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Plan {
    pub services: HashMap<SocketAddrV6, SledRequest>,
    pub dns_config: DnsConfigParams,
}

impl Ledgerable for Plan {
    fn is_newer_than(&self, _other: &Self) -> bool {
        true
    }
    fn generation_bump(&mut self) {}
}
const RSS_SERVICE_PLAN_FILENAME: &str = "rss-service-plan.json";

impl Plan {
    pub async fn load(
        log: &Logger,
        storage: &StorageResources,
    ) -> Result<Option<Plan>, PlanError> {
        let paths: Vec<Utf8PathBuf> = storage
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
            .into_iter()
            .map(|p| p.join(RSS_SERVICE_PLAN_FILENAME))
            .collect();

        // If we already created a plan for this RSS to allocate
        // services to sleds, re-use that existing plan.
        let ledger = Ledger::<Self>::new(log, paths.clone()).await;

        if let Some(ledger) = ledger {
            info!(log, "RSS plan already created, loading from file");
            Ok(Some(ledger.data().clone()))
        } else {
            Ok(None)
        }
    }

    async fn is_sled_scrimlet(
        log: &Logger,
        address: SocketAddrV6,
    ) -> Result<bool, PlanError> {
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .map_err(PlanError::HttpClient)?;
        let client = SledAgentClient::new_with_client(
            &format!("http://{}", address),
            client,
            log.new(o!("SledAgentClient" => address.to_string())),
        );

        let role = client.sled_role_get().await?.into_inner();
        match role {
            SledAgentTypes::SledRole::Gimlet => Ok(false),
            SledAgentTypes::SledRole::Scrimlet => Ok(true),
        }
    }

    // Gets zpool UUIDs from U.2 devices on the sled.
    async fn get_u2_zpools_from_sled(
        log: &Logger,
        address: SocketAddrV6,
    ) -> Result<Vec<ZpoolName>, PlanError> {
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .map_err(PlanError::HttpClient)?;
        let client = SledAgentClient::new_with_client(
            &format!("http://{}", address),
            client,
            log.new(o!("SledAgentClient" => address.to_string())),
        );

        let get_u2_zpools = || async {
            let zpools: Vec<ZpoolName> = client
                .zpools_get()
                .await
                .map(|response| {
                    response
                        .into_inner()
                        .into_iter()
                        .filter_map(|zpool| match zpool.disk_type {
                            SledAgentTypes::DiskType::U2 => {
                                Some(ZpoolName::new_external(zpool.id))
                            }
                            SledAgentTypes::DiskType::M2 => None,
                        })
                        .collect()
                })
                .map_err(|err| {
                    BackoffError::transient(PlanError::SledApi(err))
                })?;

            if zpools.len() < MINIMUM_U2_ZPOOL_COUNT {
                return Err(BackoffError::transient(
                    PlanError::SledInitialization(
                        "Awaiting zpools".to_string(),
                    ),
                ));
            }

            Ok(zpools)
        };

        let log_failure = |error, call_count, total_duration| {
            if call_count == 0 {
                info!(log, "failed to get zpools from {address}"; "error" => ?error);
            } else if total_duration > std::time::Duration::from_secs(20) {
                warn!(log, "failed to get zpools from {address}"; "error" => ?error, "total duration" => ?total_duration);
            }
        };
        let u2_zpools = retry_notify_ext(
            retry_policy_internal_service_aggressive(),
            get_u2_zpools,
            log_failure,
        )
        .await?;

        Ok(u2_zpools)
    }

    pub async fn create(
        log: &Logger,
        config: &Config,
        storage: &StorageResources,
        sleds: &HashMap<SocketAddrV6, StartSledAgentRequest>,
    ) -> Result<Self, PlanError> {
        let mut dns_builder = internal_dns::DnsConfigBuilder::new();
        let mut svc_port_builder = ServicePortBuilder::new(config);

        // Load the information we need about each Sled to be able to allocate
        // components on it.
        let mut sled_info = {
            let result: Result<Vec<SledInfo>, PlanError> =
                futures::future::try_join_all(sleds.values().map(
                    |sled_request| async {
                        let subnet = sled_request.subnet;
                        let sled_address = get_sled_address(subnet);
                        let u2_zpools =
                            Self::get_u2_zpools_from_sled(log, sled_address)
                                .await?;
                        let is_scrimlet =
                            Self::is_sled_scrimlet(log, sled_address).await?;
                        Ok(SledInfo::new(
                            sled_request.id,
                            subnet,
                            sled_address,
                            u2_zpools,
                            is_scrimlet,
                        ))
                    },
                ))
                .await;
            result?
        };

        // Scrimlets get DNS records for running Dendrite.
        let scrimlets: Vec<_> =
            sled_info.iter().filter(|s| s.is_scrimlet).collect();
        if scrimlets.is_empty() {
            return Err(PlanError::SledInitialization(
                "No scrimlets observed".to_string(),
            ));
        }
        for sled in scrimlets {
            let address = get_switch_zone_address(sled.subnet);
            let zone =
                dns_builder.host_dendrite(sled.sled_id, address).unwrap();
            dns_builder
                .service_backend_zone(
                    ServiceName::Dendrite,
                    &zone,
                    DENDRITE_PORT,
                )
                .unwrap();
            dns_builder
                .service_backend_zone(
                    ServiceName::ManagementGatewayService,
                    &zone,
                    MGS_PORT,
                )
                .unwrap();
        }

        // We'll stripe most services across all available Sleds, round-robin
        // style.  In development and CI, this might only be one Sled.  We'll
        // only report `NotEnoughSleds` below if there are zero Sleds or if we
        // ran out of zpools on the available Sleds.
        let mut sled_allocator = (0..sled_info.len()).cycle();

        // Provision internal DNS zones, striping across Sleds.
        let reserved_rack_subnet = ReservedRackSubnet::new(config.az_subnet());
        static_assertions::const_assert!(DNS_REDUNDANCY <= MAX_DNS_REDUNDANCY,);
        let dns_subnets =
            &reserved_rack_subnet.get_dns_subnets()[0..DNS_REDUNDANCY];
        let rack_dns_servers = dns_subnets
            .into_iter()
            .map(|dns_subnet| dns_subnet.dns_address().ip().into())
            .collect::<Vec<IpAddr>>();
        for i in 0..dns_subnets.len() {
            let dns_subnet = &dns_subnets[i];
            let ip = dns_subnet.dns_address().ip();
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let http_address = SocketAddrV6::new(ip, DNS_HTTP_PORT, 0, 0);
            let dns_address = SocketAddrV6::new(ip, DNS_PORT, 0, 0);

            let id = Uuid::new_v4();
            let zone = dns_builder.host_zone(id, ip).unwrap();
            dns_builder
                .service_backend_zone(
                    ServiceName::InternalDns,
                    &zone,
                    DNS_HTTP_PORT,
                )
                .unwrap();
            let dataset_name =
                sled.alloc_from_u2_zpool(DatasetKind::InternalDns)?;

            sled.request.services.push(ServiceZoneRequest {
                id,
                zone_type: ZoneType::InternalDns,
                addresses: vec![ip],
                dataset: Some(DatasetRequest {
                    id,
                    name: dataset_name,
                    service_address: http_address,
                    arc_primary_cache: DatasetArcPrimaryCache::Inherit,
                }),
                services: vec![ServiceZoneService {
                    id,
                    details: ServiceType::InternalDns {
                        http_address,
                        dns_address,
                        gz_address: dns_subnet.gz_address().ip(),
                        gz_address_index: i.try_into().expect("Giant indices?"),
                    },
                }],
            });
        }

        // Provision CockroachDB zones, continuing to stripe across Sleds.
        for _ in 0..CRDB_COUNT {
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let id = Uuid::new_v4();
            let ip = sled.addr_alloc.next().expect("Not enough addrs");
            let port = omicron_common::address::COCKROACH_PORT;
            let address = SocketAddrV6::new(ip, port, 0, 0);
            let zone = dns_builder.host_zone(id, ip).unwrap();
            dns_builder
                .service_backend_zone(ServiceName::Cockroach, &zone, port)
                .unwrap();
            let dataset_name =
                sled.alloc_from_u2_zpool(DatasetKind::CockroachDb)?;
            sled.request.services.push(ServiceZoneRequest {
                id,
                zone_type: ZoneType::CockroachDb,
                addresses: vec![ip],
                dataset: Some(DatasetRequest {
                    id,
                    name: dataset_name,
                    service_address: address,
                    arc_primary_cache: DatasetArcPrimaryCache::Inherit,
                }),
                services: vec![ServiceZoneService {
                    id,
                    details: ServiceType::CockroachDb { address },
                }],
            });
        }

        // Provision external DNS zones, continuing to stripe across sleds.
        // The number of DNS services depends on the number of external DNS
        // server IP addresses given to us at RSS-time.
        // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
        loop {
            let id = Uuid::new_v4();
            let Some((nic, external_ip)) = svc_port_builder.next_dns(id) else {
                break;
            };

            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let internal_ip = sled.addr_alloc.next().expect("Not enough addrs");
            let http_port = omicron_common::address::DNS_HTTP_PORT;
            let http_address = SocketAddrV6::new(internal_ip, http_port, 0, 0);
            let zone = dns_builder.host_zone(id, internal_ip).unwrap();
            dns_builder
                .service_backend_zone(
                    ServiceName::ExternalDns,
                    &zone,
                    http_port,
                )
                .unwrap();
            let dns_port = omicron_common::address::DNS_PORT;
            let dns_address = SocketAddr::new(external_ip, dns_port);
            let dataset_kind = DatasetKind::ExternalDns;
            let dataset_name = sled.alloc_from_u2_zpool(dataset_kind)?;

            sled.request.services.push(ServiceZoneRequest {
                id,
                zone_type: ZoneType::ExternalDns,
                addresses: vec![*http_address.ip()],
                dataset: Some(DatasetRequest {
                    id,
                    name: dataset_name,
                    service_address: http_address,
                    arc_primary_cache: DatasetArcPrimaryCache::Inherit,
                }),
                services: vec![ServiceZoneService {
                    id,
                    details: ServiceType::ExternalDns {
                        http_address,
                        dns_address,
                        nic,
                    },
                }],
            });
        }

        // Provision Nexus zones, continuing to stripe across sleds.
        for _ in 0..NEXUS_COUNT {
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let id = Uuid::new_v4();
            let address = sled.addr_alloc.next().expect("Not enough addrs");
            let zone = dns_builder.host_zone(id, address).unwrap();
            dns_builder
                .service_backend_zone(
                    ServiceName::Nexus,
                    &zone,
                    omicron_common::address::NEXUS_INTERNAL_PORT,
                )
                .unwrap();
            let (nic, external_ip) = svc_port_builder.next_nexus(id)?;
            sled.request.services.push(ServiceZoneRequest {
                id,
                zone_type: ZoneType::Nexus,
                addresses: vec![address],
                dataset: None,
                services: vec![ServiceZoneService {
                    id,
                    details: ServiceType::Nexus {
                        internal_address: SocketAddrV6::new(
                            address,
                            omicron_common::address::NEXUS_INTERNAL_PORT,
                            0,
                            0,
                        ),
                        external_ip,
                        nic,
                        // Tell Nexus to use TLS if and only if the caller
                        // provided TLS certificates.  This effectively
                        // determines the status of TLS for the lifetime of
                        // the rack.  In production-like deployments, we'd
                        // always expect TLS to be enabled.  It's only in
                        // development that it might not be.
                        external_tls: !config.external_certificates.is_empty(),
                        external_dns_servers: config.dns_servers.clone(),
                    },
                }],
            })
        }

        // Provision Oximeter zones, continuing to stripe across sleds.
        // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
        for _ in 0..OXIMETER_COUNT {
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let id = Uuid::new_v4();
            let address = sled.addr_alloc.next().expect("Not enough addrs");
            let zone = dns_builder.host_zone(id, address).unwrap();
            dns_builder
                .service_backend_zone(
                    ServiceName::Oximeter,
                    &zone,
                    omicron_common::address::OXIMETER_PORT,
                )
                .unwrap();
            sled.request.services.push(ServiceZoneRequest {
                id,
                zone_type: ZoneType::Oximeter,
                addresses: vec![address],
                dataset: None,
                services: vec![ServiceZoneService {
                    id,
                    details: ServiceType::Oximeter {
                        address: SocketAddrV6::new(
                            address,
                            omicron_common::address::OXIMETER_PORT,
                            0,
                            0,
                        ),
                    },
                }],
            })
        }

        // Provision Clickhouse zones, continuing to stripe across sleds.
        // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
        for _ in 0..CLICKHOUSE_COUNT {
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let id = Uuid::new_v4();
            let ip = sled.addr_alloc.next().expect("Not enough addrs");
            let port = omicron_common::address::CLICKHOUSE_PORT;
            let address = SocketAddrV6::new(ip, port, 0, 0);
            let zone = dns_builder.host_zone(id, ip).unwrap();
            dns_builder
                .service_backend_zone(ServiceName::Clickhouse, &zone, port)
                .unwrap();
            let dataset_name =
                sled.alloc_from_u2_zpool(DatasetKind::Clickhouse)?;
            sled.request.services.push(ServiceZoneRequest {
                id,
                zone_type: ZoneType::Clickhouse,
                addresses: vec![ip],
                dataset: Some(DatasetRequest {
                    id,
                    name: dataset_name,
                    service_address: address,
                    arc_primary_cache: DatasetArcPrimaryCache::Inherit,
                }),
                services: vec![ServiceZoneService {
                    id,
                    details: ServiceType::Clickhouse { address },
                }],
            });
        }

        // Provision Clickhouse Keeper zones, continuing to stripe across sleds.
        // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
        // Temporary linter rule until replicated Clickhouse is enabled
        #[allow(clippy::reversed_empty_ranges)]
        for _ in 0..CLICKHOUSE_KEEPER_COUNT {
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let id = Uuid::new_v4();
            let ip = sled.addr_alloc.next().expect("Not enough addrs");
            let port = omicron_common::address::CLICKHOUSE_KEEPER_PORT;
            let address = SocketAddrV6::new(ip, port, 0, 0);
            let zone = dns_builder.host_zone(id, ip).unwrap();
            dns_builder
                .service_backend_zone(
                    ServiceName::ClickhouseKeeper,
                    &zone,
                    port,
                )
                .unwrap();
            let dataset_name =
                sled.alloc_from_u2_zpool(DatasetKind::ClickhouseKeeper)?;
            sled.request.services.push(ServiceZoneRequest {
                id,
                zone_type: ZoneType::ClickhouseKeeper,
                addresses: vec![ip],
                dataset: Some(DatasetRequest {
                    id,
                    name: dataset_name,
                    service_address: address,
                    arc_primary_cache: DatasetArcPrimaryCache::Inherit,
                }),
                services: vec![ServiceZoneService {
                    id,
                    details: ServiceType::ClickhouseKeeper { address },
                }],
            });
        }

        // Provision Crucible Pantry zones, continuing to stripe across sleds.
        // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
        for _ in 0..PANTRY_COUNT {
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let address = sled.addr_alloc.next().expect("Not enough addrs");
            let port = omicron_common::address::CRUCIBLE_PANTRY_PORT;
            let id = Uuid::new_v4();
            let zone = dns_builder.host_zone(id, address).unwrap();
            dns_builder
                .service_backend_zone(ServiceName::CruciblePantry, &zone, port)
                .unwrap();
            sled.request.services.push(ServiceZoneRequest {
                id,
                zone_type: ZoneType::CruciblePantry,
                addresses: vec![address],
                dataset: None,
                services: vec![ServiceZoneService {
                    id,
                    details: ServiceType::CruciblePantry {
                        address: SocketAddrV6::new(address, port, 0, 0),
                    },
                }],
            })
        }

        // Provision a Crucible zone on every zpool on every Sled.
        // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
        for sled in sled_info.iter_mut() {
            for pool in &sled.u2_zpools {
                let ip = sled.addr_alloc.next().expect("Not enough addrs");
                let port = omicron_common::address::CRUCIBLE_PORT;
                let address = SocketAddrV6::new(ip, port, 0, 0);
                let id = Uuid::new_v4();
                let zone = dns_builder.host_zone(id, ip).unwrap();
                dns_builder
                    .service_backend_zone(
                        ServiceName::Crucible(id),
                        &zone,
                        port,
                    )
                    .unwrap();

                sled.request.services.push(ServiceZoneRequest {
                    id,
                    zone_type: ZoneType::Crucible,
                    addresses: vec![ip],
                    dataset: Some(DatasetRequest {
                        id,
                        name: DatasetName::new(
                            pool.clone(),
                            DatasetKind::Crucible,
                        ),
                        service_address: address,
                        arc_primary_cache: DatasetArcPrimaryCache::Metadata,
                    }),
                    services: vec![ServiceZoneService {
                        id,
                        details: ServiceType::Crucible { address },
                    }],
                });
            }
        }

        // All sleds get an NTP server, but the first few are nominated as
        // boundary servers, responsible for communicating with the external
        // network.
        let mut boundary_ntp_servers = vec![];
        for (idx, sled) in sled_info.iter_mut().enumerate() {
            let id = Uuid::new_v4();
            let address = sled.addr_alloc.next().expect("Not enough addrs");
            let zone = dns_builder.host_zone(id, address).unwrap();

            let (services, svcname) = if idx < BOUNDARY_NTP_COUNT {
                boundary_ntp_servers.push(format!("{}.host.{}", id, DNS_ZONE));
                let (nic, snat_cfg) = svc_port_builder.next_snat(id)?;
                (
                    vec![ServiceZoneService {
                        id,
                        details: ServiceType::BoundaryNtp {
                            address: SocketAddrV6::new(address, NTP_PORT, 0, 0),
                            ntp_servers: config.ntp_servers.clone(),
                            dns_servers: config.dns_servers.clone(),
                            domain: None,
                            nic,
                            snat_cfg,
                        },
                    }],
                    ServiceName::BoundaryNtp,
                )
            } else {
                (
                    vec![ServiceZoneService {
                        id,
                        details: ServiceType::InternalNtp {
                            address: SocketAddrV6::new(address, NTP_PORT, 0, 0),
                            ntp_servers: boundary_ntp_servers.clone(),
                            dns_servers: rack_dns_servers.clone(),
                            domain: None,
                        },
                    }],
                    ServiceName::InternalNtp,
                )
            };

            dns_builder.service_backend_zone(svcname, &zone, NTP_PORT).unwrap();

            sled.request.services.push(ServiceZoneRequest {
                id,
                zone_type: ZoneType::Ntp,
                addresses: vec![address],
                dataset: None,
                services,
            });
        }

        let services: HashMap<_, _> = sled_info
            .into_iter()
            .map(|sled_info| (sled_info.sled_address, sled_info.request))
            .collect();

        let dns_config = dns_builder.build();
        let plan = Self { services, dns_config };

        // Once we've constructed a plan, write it down to durable storage.
        let paths: Vec<Utf8PathBuf> = storage
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
            .into_iter()
            .map(|p| p.join(RSS_SERVICE_PLAN_FILENAME))
            .collect();
        let mut ledger = Ledger::<Self>::new_with(log, paths, plan.clone());
        ledger.commit().await?;
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

/// Wraps up the information used to allocate components to a Sled
struct SledInfo {
    /// unique id for the sled agent
    sled_id: Uuid,
    /// the sled's unique IPv6 subnet
    subnet: Ipv6Subnet<SLED_PREFIX>,
    /// the address of the Sled Agent on the sled's subnet
    sled_address: SocketAddrV6,
    /// the list of zpools on the Sled
    u2_zpools: Vec<ZpoolName>,
    /// spreads components across a Sled's zpools
    u2_zpool_allocators:
        HashMap<DatasetKind, Box<dyn Iterator<Item = usize> + Send + Sync>>,
    /// whether this Sled is a scrimlet
    is_scrimlet: bool,
    /// allocator for addresses in this Sled's subnet
    addr_alloc: AddressBumpAllocator,
    /// under-construction list of services being deployed to a Sled
    request: SledRequest,
}

impl SledInfo {
    fn new(
        sled_id: Uuid,
        subnet: Ipv6Subnet<SLED_PREFIX>,
        sled_address: SocketAddrV6,
        u2_zpools: Vec<ZpoolName>,
        is_scrimlet: bool,
    ) -> SledInfo {
        SledInfo {
            sled_id,
            subnet,
            sled_address,
            u2_zpools,
            u2_zpool_allocators: HashMap::new(),
            is_scrimlet,
            addr_alloc: AddressBumpAllocator::new(subnet),
            request: Default::default(),
        }
    }

    /// Allocates a dataset of the specified type from one of the U.2 pools on
    /// this Sled
    fn alloc_from_u2_zpool(
        &mut self,
        kind: DatasetKind,
    ) -> Result<DatasetName, PlanError> {
        // We have two goals here:
        //
        // - For datasets of different types, they should be able to use the
        //   same pool.
        //
        // - For datasets of the same type, they must be on separate pools.  We
        //   want to fail explicitly if we can't do that (which might happen if
        //   we've tried to allocate more datasets than we have pools).  Sled
        //   Agent does not support having multiple datasets of some types
        //   (e.g., cockroachdb) on the same pool.
        //
        // To achieve this, we maintain one iterator per dataset kind that
        // enumerates the valid zpool indexes.
        let allocator = self
            .u2_zpool_allocators
            .entry(kind.clone())
            .or_insert_with(|| Box::new(0..self.u2_zpools.len()));
        match allocator.next() {
            None => Err(PlanError::NotEnoughSleds),
            Some(which_zpool) => {
                Ok(DatasetName::new(self.u2_zpools[which_zpool].clone(), kind))
            }
        }
    }
}

struct ServicePortBuilder {
    internal_services_ip_pool: Box<dyn Iterator<Item = IpAddr> + Send>,
    external_dns_ips: std::vec::IntoIter<IpAddr>,

    next_snat_ip: Option<IpAddr>,
    next_snat_port: Wrapping<u16>,

    dns_v4_ips: Box<dyn Iterator<Item = Ipv4Addr> + Send>,
    dns_v6_ips: Box<dyn Iterator<Item = Ipv6Addr> + Send>,

    nexus_v4_ips: Box<dyn Iterator<Item = Ipv4Addr> + Send>,
    nexus_v6_ips: Box<dyn Iterator<Item = Ipv6Addr> + Send>,

    ntp_v4_ips: Box<dyn Iterator<Item = Ipv4Addr> + Send>,
    ntp_v6_ips: Box<dyn Iterator<Item = Ipv6Addr> + Send>,

    used_macs: HashSet<MacAddr>,
}

impl ServicePortBuilder {
    fn new(config: &Config) -> Self {
        use omicron_common::address::{
            DNS_OPTE_IPV4_SUBNET, DNS_OPTE_IPV6_SUBNET, NEXUS_OPTE_IPV4_SUBNET,
            NEXUS_OPTE_IPV6_SUBNET, NTP_OPTE_IPV4_SUBNET, NTP_OPTE_IPV6_SUBNET,
        };
        use omicron_common::nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;

        let external_dns_ips_set = config
            .external_dns_ips
            .iter()
            .copied()
            .collect::<BTreeSet<IpAddr>>();
        let internal_services_ip_pool = Box::new(
            config
                .internal_services_ip_pool_ranges
                .clone()
                .into_iter()
                .flat_map(|range| range.iter())
                // External DNS IPs are required to be present in
                // `internal_services_ip_pool_ranges`, but we want to skip them
                // when choosing IPs for non-DNS services, so filter them out
                // here.
                .filter(move |ip| !external_dns_ips_set.contains(ip)),
        );
        let external_dns_ips = config.external_dns_ips.clone().into_iter();

        let dns_v4_ips = Box::new(
            DNS_OPTE_IPV4_SUBNET
                .0
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
        );
        let dns_v6_ips = Box::new(
            DNS_OPTE_IPV6_SUBNET
                .0
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
        );
        let nexus_v4_ips = Box::new(
            NEXUS_OPTE_IPV4_SUBNET
                .0
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
        );
        let nexus_v6_ips = Box::new(
            NEXUS_OPTE_IPV6_SUBNET
                .0
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
        );
        let ntp_v4_ips = Box::new(
            NTP_OPTE_IPV4_SUBNET
                .0
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
        );
        let ntp_v6_ips = Box::new(
            NTP_OPTE_IPV6_SUBNET
                .0
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
        );
        Self {
            internal_services_ip_pool,
            external_dns_ips,
            next_snat_ip: None,
            next_snat_port: Wrapping(0),
            dns_v4_ips,
            dns_v6_ips,
            nexus_v4_ips,
            nexus_v6_ips,
            ntp_v4_ips,
            ntp_v6_ips,
            used_macs: HashSet::new(),
        }
    }

    fn next_internal_service_ip(&mut self) -> Option<IpAddr> {
        self.internal_services_ip_pool.next()
    }

    fn random_mac(&mut self) -> MacAddr {
        let mut mac = MacAddr::random_system();
        while !self.used_macs.insert(mac) {
            mac = MacAddr::random_system();
        }
        mac
    }

    fn next_dns(&mut self, svc_id: Uuid) -> Option<(NetworkInterface, IpAddr)> {
        use omicron_common::address::{
            DNS_OPTE_IPV4_SUBNET, DNS_OPTE_IPV6_SUBNET,
        };
        let external_ip = self.external_dns_ips.next()?;

        let (ip, subnet) = match external_ip {
            IpAddr::V4(_) => (
                self.dns_v4_ips.next().unwrap().into(),
                (*DNS_OPTE_IPV4_SUBNET).into(),
            ),
            IpAddr::V6(_) => (
                self.dns_v6_ips.next().unwrap().into(),
                (*DNS_OPTE_IPV6_SUBNET).into(),
            ),
        };

        let nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Service { id: svc_id },
            name: format!("external-dns-{svc_id}").parse().unwrap(),
            ip,
            mac: self.random_mac(),
            subnet,
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
        };

        Some((nic, external_ip))
    }

    fn next_nexus(
        &mut self,
        svc_id: Uuid,
    ) -> Result<(NetworkInterface, IpAddr), PlanError> {
        use omicron_common::address::{
            NEXUS_OPTE_IPV4_SUBNET, NEXUS_OPTE_IPV6_SUBNET,
        };
        let external_ip = self
            .next_internal_service_ip()
            .ok_or_else(|| PlanError::ServiceIp("Nexus"))?;

        let (ip, subnet) = match external_ip {
            IpAddr::V4(_) => (
                self.nexus_v4_ips.next().unwrap().into(),
                (*NEXUS_OPTE_IPV4_SUBNET).into(),
            ),
            IpAddr::V6(_) => (
                self.nexus_v6_ips.next().unwrap().into(),
                (*NEXUS_OPTE_IPV6_SUBNET).into(),
            ),
        };

        let nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Service { id: svc_id },
            name: format!("nexus-{svc_id}").parse().unwrap(),
            ip,
            mac: self.random_mac(),
            subnet,
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
        };

        Ok((nic, external_ip))
    }

    fn next_snat(
        &mut self,
        svc_id: Uuid,
    ) -> Result<(NetworkInterface, SourceNatConfig), PlanError> {
        use omicron_common::address::{
            NTP_OPTE_IPV4_SUBNET, NTP_OPTE_IPV6_SUBNET,
        };
        let snat_ip = self
            .next_snat_ip
            .or_else(|| self.next_internal_service_ip())
            .ok_or_else(|| PlanError::ServiceIp("Boundary NTP"))?;
        let first_port = self.next_snat_port.0;
        let last_port = first_port + (NUM_SOURCE_NAT_PORTS - 1);

        self.next_snat_port += NUM_SOURCE_NAT_PORTS;
        if self.next_snat_port.0 == 0 {
            self.next_snat_ip = None;
        }

        let snat_cfg = SourceNatConfig { ip: snat_ip, first_port, last_port };

        let (ip, subnet) = match snat_ip {
            IpAddr::V4(_) => (
                self.ntp_v4_ips.next().unwrap().into(),
                (*NTP_OPTE_IPV4_SUBNET).into(),
            ),
            IpAddr::V6(_) => (
                self.ntp_v6_ips.next().unwrap().into(),
                (*NTP_OPTE_IPV6_SUBNET).into(),
            ),
        };

        let nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Service { id: svc_id },
            name: format!("ntp-{svc_id}").parse().unwrap(),
            ip,
            mac: self.random_mac(),
            subnet,
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
        };

        Ok((nic, snat_cfg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::params::BootstrapAddressDiscovery;
    use crate::bootstrap::params::RecoverySiloConfig;
    use omicron_common::address::IpRange;

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

    #[test]
    fn service_port_builder_skips_dns_ips() {
        // Conjure up a config; only the internal services pools and
        // external DNS IPs matter when constructing a ServicePortBuilder.
        let ip_pools = [
            ("192.168.1.10", "192.168.1.14"),
            ("fd00::20", "fd00::23"),
            ("fd01::100", "fd01::103"),
        ];
        let dns_ips = [
            "192.168.1.10",
            "192.168.1.13",
            "fd00::22",
            "fd01::100",
            "fd01::103",
        ];
        let config = Config {
            rack_subnet: Ipv6Addr::LOCALHOST,
            trust_quorum_peers: None,
            bootstrap_discovery: BootstrapAddressDiscovery::OnlyOurs,
            ntp_servers: Vec::new(),
            dns_servers: Vec::new(),
            internal_services_ip_pool_ranges: ip_pools
                .iter()
                .map(|(a, b)| {
                    let a: IpAddr = a.parse().unwrap();
                    let b: IpAddr = b.parse().unwrap();
                    IpRange::try_from((a, b)).unwrap()
                })
                .collect(),
            external_dns_ips: dns_ips
                .iter()
                .map(|ip| ip.parse().unwrap())
                .collect(),
            external_dns_zone_name: "".to_string(),
            external_certificates: Vec::new(),
            recovery_silo: RecoverySiloConfig {
                silo_name: "recovery".parse().unwrap(),
                user_name: "recovery".parse().unwrap(),
                user_password_hash: "$argon2id$v=19$m=98304,t=13,p=1$RUlWc0ZxaHo0WFdrN0N6ZQ$S8p52j85GPvMhR/ek3GL0el/oProgTwWpHJZ8lsQQoY".parse().unwrap(),
            },
            rack_network_config: None,
        };

        let mut svp = ServicePortBuilder::new(&config);

        // We should only get back the 5 DNS IPs we specified.
        let mut svp_dns_ips = Vec::new();
        while let Some((_interface, ip)) = svp.next_dns(Uuid::new_v4()) {
            svp_dns_ips.push(ip.to_string());
        }
        assert_eq!(svp_dns_ips, dns_ips);

        // next_internal_service_ip() should return all the IPs in our
        // `ip_pools` ranges _except_ the 5 DNS IPs.
        let expected_internal_service_ips = [
            // "192.168.1.10", DNS IP
            "192.168.1.11",
            "192.168.1.12",
            // "192.168.1.13", DNS IP
            "192.168.1.14",
            "fd00::20",
            "fd00::21",
            // "fd00::22", DNS IP
            "fd00::23",
            // "fd01::100", DNS IP
            "fd01::101",
            "fd01::102",
            // "fd01::103", DNS IP
        ];
        let mut internal_service_ips = Vec::new();
        while let Some(ip) = svp.next_internal_service_ip() {
            internal_service_ips.push(ip.to_string());
        }
        assert_eq!(internal_service_ips, expected_internal_service_ips);
    }

    #[test]
    fn test_rss_service_plan_schema() {
        let schema = schemars::schema_for!(Plan);
        expectorate::assert_contents(
            "../schema/rss-service-plan.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }
}
