// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Plan generation for "where should services be initialized".

use anyhow::anyhow;
use anyhow::bail;
use chrono::Utc;
use iddqd::IdOrdMap;
use iddqd::errors::DuplicateItem;
use iddqd::id_upcast;
use illumos_utils::zpool::ZpoolName;
use internal_dns_types::config::{
    DnsConfigBuilder, DnsConfigParams, Host, Zone,
};
use internal_dns_types::names::ServiceName;
use nexus_sled_agent_shared::inventory::{
    Inventory, OmicronZoneDataset, SledRole,
};
use nexus_types::deployment::{
    Blueprint, BlueprintDatasetConfig, BlueprintDatasetDisposition,
    BlueprintHostPhase2DesiredSlots, BlueprintPhysicalDiskConfig,
    BlueprintPhysicalDiskDisposition, BlueprintSledConfig, BlueprintSource,
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZoneImageSource,
    BlueprintZoneType, CockroachDbPreserveDowngrade,
    OmicronZoneExternalFloatingAddr, OmicronZoneExternalFloatingIp,
    OmicronZoneExternalSnatIp, OximeterReadMode, PendingMgsUpdates,
    blueprint_zone_type,
};
use nexus_types::external_api::views::SledState;
use omicron_common::address::{
    DENDRITE_PORT, DNS_HTTP_PORT, DNS_PORT, Ipv6Subnet, MGD_PORT, MGS_PORT,
    NEXUS_INTERNAL_PORT, NEXUS_LOCKSTEP_PORT, NTP_PORT, NUM_SOURCE_NAT_PORTS,
    REPO_DEPOT_PORT, RSS_RESERVED_ADDRESSES, ReservedRackSubnet, SLED_PREFIX,
    get_sled_address, get_switch_zone_address,
};
use omicron_common::api::external::{Generation, MacAddr, Vni};
use omicron_common::api::internal::shared::{
    NetworkInterface, NetworkInterfaceKind, PrivateIpConfig,
    PrivateIpConfigError, SourceNatConfigError, SourceNatConfigGeneric,
};
use omicron_common::backoff::{
    BackoffError, retry_notify_ext, retry_policy_internal_service_aggressive,
};
use omicron_common::disk::{
    CompressionAlgorithm, DatasetConfig, DatasetKind, DatasetName, DiskVariant,
    SharedDatasetConfig,
};
use omicron_common::policy::{
    BOUNDARY_NTP_REDUNDANCY, COCKROACHDB_REDUNDANCY,
    CRUCIBLE_PANTRY_REDUNDANCY, INTERNAL_DNS_REDUNDANCY, NEXUS_REDUNDANCY,
    OXIMETER_REDUNDANCY, RESERVED_INTERNAL_DNS_REDUNDANCY,
    SINGLE_NODE_CLICKHOUSE_REDUNDANCY,
};
use omicron_uuid_kinds::{
    BlueprintUuid, DatasetUuid, ExternalIpUuid, GenericUuid, OmicronZoneUuid,
    PhysicalDiskUuid, SledUuid, ZpoolUuid,
};
use rand::seq::IndexedRandom;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_client::{
    Client as SledAgentClient, Error as SledAgentError, types as SledAgentTypes,
};
use sled_agent_types::rack_init::RackInitializeRequest as Config;
use sled_agent_types::sled::StartSledAgentRequest;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::num::Wrapping;
use thiserror::Error;
use uuid::Uuid;

const MINIMUM_U2_COUNT: usize = 3;

/// Describes errors which may occur while generating a plan for services.
#[derive(Error, Debug)]
pub enum PlanError {
    #[error("I/O error while {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

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

    #[error("Unexpected dataset kind: {0}")]
    UnexpectedDataset(String),

    #[error("invalid private IP configuration")]
    InvalidPrivateIpConfig(#[from] PrivateIpConfigError),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct SledConfig {
    /// Control plane disks configured for this sled
    pub disks: IdOrdMap<BlueprintPhysicalDiskConfig>,

    /// Datasets configured for this sled
    pub datasets: BTreeMap<DatasetUuid, DatasetConfig>,

    /// zones configured for this sled
    pub zones: IdOrdMap<BlueprintZoneConfig>,
}

impl SledConfig {
    /// Adds a zone to the Sled's configuration, as well as any number of
    /// durable datasets.
    pub fn add_zone_and_datasets(
        &mut self,
        zone: BlueprintZoneConfig,
    ) -> Result<(), DuplicateItem<BlueprintZoneConfig>> {
        let fs_dataset_name = DatasetName::new(
            zone.filesystem_pool,
            DatasetKind::TransientZone {
                name: illumos_utils::zone::zone_name(
                    zone.zone_type.kind().zone_prefix(),
                    Some(zone.id),
                ),
            },
        );

        // Always add a transient filesystem dataset.
        let fs_dataset = DatasetConfig {
            id: DatasetUuid::new_v4(),
            name: fs_dataset_name,
            inner: SharedDatasetConfig {
                compression: CompressionAlgorithm::Off,
                quota: None,
                reservation: None,
            },
        };
        self.datasets.insert(fs_dataset.id, fs_dataset);

        // If a durable dataset exists, add it.
        if let Some(dataset) = zone.zone_type.durable_dataset() {
            let id = DatasetUuid::new_v4();
            self.datasets.insert(
                id,
                DatasetConfig {
                    id,
                    name: dataset.into(),
                    inner: SharedDatasetConfig {
                        compression: CompressionAlgorithm::Off,
                        quota: None,
                        reservation: None,
                    },
                },
            );
        }

        // Add the zone.
        self.zones.insert_unique(zone).map_err(|e| e.into_owned())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PlannedSledDescription {
    pub(crate) underlay_address: SocketAddrV6,
    pub(crate) sled_id: SledUuid,
    pub(crate) subnet: Ipv6Subnet<SLED_PREFIX>,
    pub(crate) config: SledConfig,
}

impl iddqd::IdOrdItem for PlannedSledDescription {
    type Key<'a> = SocketAddrV6;

    fn key(&self) -> Self::Key<'_> {
        self.underlay_address
    }

    id_upcast!();
}

#[derive(Clone, Debug)]
pub struct Plan {
    pub all_sleds: IdOrdMap<PlannedSledDescription>,
    pub dns_config: DnsConfigParams,
}

pub fn from_sockaddr_to_external_floating_addr(
    addr: SocketAddr,
) -> OmicronZoneExternalFloatingAddr {
    // This is pretty weird: IP IDs don't exist yet, so it's fine for us
    // to make them up (Nexus will record them as a part of the
    // handoff). We could pass `None` here for some zone types, but it's
    // a little simpler to just always pass a new ID, which will only be
    // used if the zone type has an external IP.
    //
    // This should all go away once RSS starts using blueprints more
    // directly (instead of this conversion after the fact):
    // https://github.com/oxidecomputer/omicron/issues/5272
    OmicronZoneExternalFloatingAddr { id: ExternalIpUuid::new_v4(), addr }
}

pub fn from_ipaddr_to_external_floating_ip(
    ip: IpAddr,
) -> OmicronZoneExternalFloatingIp {
    // This is pretty weird: IP IDs don't exist yet, so it's fine for us
    // to make them up (Nexus will record them as a part of the
    // handoff). We could pass `None` here for some zone types, but it's
    // a little simpler to just always pass a new ID, which will only be
    // used if the zone type has an external IP.
    //
    // This should all go away once RSS starts using blueprints more
    // directly (instead of this conversion after the fact):
    // https://github.com/oxidecomputer/omicron/issues/5272
    OmicronZoneExternalFloatingIp { id: ExternalIpUuid::new_v4(), ip }
}

pub fn from_source_nat_config_to_external_snat_ip(
    snat_cfg: SourceNatConfigGeneric,
) -> OmicronZoneExternalSnatIp {
    // This is pretty weird: IP IDs don't exist yet, so it's fine for us
    // to make them up (Nexus will record them as a part of the
    // handoff). We could pass `None` here for some zone types, but it's
    // a little simpler to just always pass a new ID, which will only be
    // used if the zone type has an external IP.
    //
    // This should all go away once RSS starts using blueprints more
    // directly (instead of this conversion after the fact):
    // https://github.com/oxidecomputer/omicron/issues/5272
    OmicronZoneExternalSnatIp { id: ExternalIpUuid::new_v4(), snat_cfg }
}

impl Plan {
    async fn get_inventory(
        log: &Logger,
        address: SocketAddrV6,
    ) -> Result<Inventory, PlanError> {
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

        let get_inventory = || async {
            let inventory = client
                .inventory()
                .await
                .map(|response| response.into_inner())
                .map_err(|err| {
                    BackoffError::transient(PlanError::SledApi(err))
                })?;

            if inventory
                .disks
                .iter()
                .filter(|disk| matches!(disk.variant, DiskVariant::U2))
                .count()
                < MINIMUM_U2_COUNT
            {
                return Err(BackoffError::transient(
                    PlanError::SledInitialization("Awaiting disks".to_string()),
                ));
            }

            Ok(inventory)
        };

        let log_failure = |error: PlanError, call_count, total_duration| {
            if call_count == 0 {
                info!(log, "failed to get inventory from {address}"; "error" => ?error);
            } else if total_duration > std::time::Duration::from_secs(20) {
                warn!(log, "failed to get inventory from {address}"; "error" => ?error, "total duration" => ?total_duration);
            }
        };
        let inventory = retry_notify_ext(
            retry_policy_internal_service_aggressive(),
            get_inventory,
            log_failure,
        )
        .await?;

        Ok(inventory)
    }

    pub fn create_transient(
        config: &Config,
        mut sled_info: Vec<SledInfo>,
    ) -> Result<Self, PlanError> {
        let mut dns_builder = DnsConfigBuilder::new();
        let mut svc_port_builder = ServicePortBuilder::new(config);

        // All sleds get a DNS entry for Repo Depot.
        for sled in sled_info.iter() {
            let dns_sled = dns_builder
                .host_sled(sled.sled_id, *sled.sled_address.ip())
                .unwrap();
            dns_builder
                .service_backend_sled(
                    ServiceName::RepoDepot,
                    &dns_sled,
                    REPO_DEPOT_PORT,
                )
                .unwrap();
        }

        // Scrimlets get DNS records for running Dendrite.
        let scrimlets: Vec<_> =
            sled_info.iter().filter(|s| s.is_scrimlet).collect();
        if scrimlets.is_empty() {
            return Err(PlanError::SledInitialization(
                "No scrimlets observed".to_string(),
            ));
        }
        for sled in scrimlets.iter() {
            let address = get_switch_zone_address(sled.subnet);
            dns_builder
                .host_zone_switch(
                    sled.sled_id,
                    address,
                    DENDRITE_PORT,
                    MGS_PORT,
                    MGD_PORT,
                )
                .unwrap();
        }

        // Set up storage early, as it'll be necessary for placement of
        // many subsequent services.
        //
        // Our policy at RSS time is currently "adopt all the U.2 disks we can
        // see".
        for sled_info in sled_info.iter_mut() {
            sled_info.request.disks = sled_info
                .inventory
                .disks
                .iter()
                .filter(|disk| matches!(disk.variant, DiskVariant::U2))
                .map(|disk| BlueprintPhysicalDiskConfig {
                    disposition: BlueprintPhysicalDiskDisposition::InService,
                    identity: disk.identity.clone(),
                    id: PhysicalDiskUuid::new_v4(),
                    pool_id: ZpoolUuid::new_v4(),
                })
                .collect();
            sled_info.u2_zpools = sled_info
                .request
                .disks
                .iter()
                .map(|disk| ZpoolName::new_external(disk.pool_id))
                .collect();

            // Add all non-discretionary datasets, self-provisioned on the U.2,
            // to the blueprint.
            for zpool in &sled_info.u2_zpools {
                for intrinsic_dataset in
                    sled_storage::dataset::U2_EXPECTED_DATASETS
                {
                    let name = intrinsic_dataset.get_name();
                    let kind = match name {
                        sled_storage::dataset::ZONE_DATASET => {
                            DatasetKind::TransientZoneRoot
                        }
                        sled_storage::dataset::U2_DEBUG_DATASET => {
                            DatasetKind::Debug
                        }
                        _ => {
                            return Err(PlanError::UnexpectedDataset(
                                name.to_string(),
                            ));
                        }
                    };

                    let config = DatasetConfig {
                        id: DatasetUuid::new_v4(),
                        name: DatasetName::new(*zpool, kind),
                        inner: SharedDatasetConfig {
                            compression: intrinsic_dataset.get_compression(),
                            quota: intrinsic_dataset.get_quota(),
                            reservation: None,
                        },
                    };
                    sled_info.request.datasets.insert(config.id, config);
                }

                // LocalStorage isn't in the U2_EXPECTED_DATASETS list, add it
                // here. We expect Nexus to take over after RSS and not need to
                // make any changes to the resulting current target blueprint.
                // The `rss_blueprint_is_blippy_clean` test will fail if this
                // isn't true, and removing this will cause that to fail.
                let config = DatasetConfig {
                    id: DatasetUuid::new_v4(),
                    name: DatasetName::new(*zpool, DatasetKind::LocalStorage),
                    inner: SharedDatasetConfig {
                        compression: CompressionAlgorithm::Off,
                        quota: None,
                        reservation: None,
                    },
                };
                sled_info.request.datasets.insert(config.id, config);
            }
        }

        // We'll stripe most services across all available Sleds, round-robin
        // style.  In development and CI, this might only be one Sled.  We'll
        // only report `NotEnoughSleds` below if there are zero Sleds or if we
        // ran out of zpools on the available Sleds.
        let mut sled_allocator = (0..sled_info.len()).cycle();

        // Provision internal DNS zones, striping across Sleds.
        let reserved_rack_subnet = ReservedRackSubnet::new(config.az_subnet());
        static_assertions::const_assert!(
            INTERNAL_DNS_REDUNDANCY <= RESERVED_INTERNAL_DNS_REDUNDANCY
        );
        let dns_subnets = reserved_rack_subnet.get_dns_subnets();
        for i in 0..dns_subnets.len() {
            let dns_subnet = &dns_subnets[i];
            let ip = dns_subnet.dns_address();
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let http_address = SocketAddrV6::new(ip, DNS_HTTP_PORT, 0, 0);
            let dns_address = SocketAddrV6::new(ip, DNS_PORT, 0, 0);

            let id = OmicronZoneUuid::new_v4();
            dns_builder
                .host_zone_internal_dns(
                    id,
                    ServiceName::InternalDns,
                    http_address,
                    dns_address,
                )
                .unwrap();
            let dataset_name =
                sled.alloc_dataset_from_u2s(DatasetKind::InternalDns)?;
            let filesystem_pool = *dataset_name.pool();

            sled.request
                .add_zone_and_datasets(BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id,
                    filesystem_pool,
                    zone_type: BlueprintZoneType::InternalDns(
                        blueprint_zone_type::InternalDns {
                            dataset: OmicronZoneDataset {
                                pool_name: *dataset_name.pool(),
                            },
                            http_address,
                            dns_address,
                            gz_address: dns_subnet.gz_address(),
                            gz_address_index: i
                                .try_into()
                                .expect("Giant indices?"),
                        },
                    ),
                    image_source: BlueprintZoneImageSource::InstallDataset,
                })
                .expect("freshly generated zone IDs are unique");
        }

        // Provision CockroachDB zones, continuing to stripe across Sleds.
        for _ in 0..COCKROACHDB_REDUNDANCY {
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let id = OmicronZoneUuid::new_v4();
            let ip = sled.addr_alloc.next().expect("Not enough addrs");
            let port = omicron_common::address::COCKROACH_PORT;
            let address = SocketAddrV6::new(ip, port, 0, 0);
            dns_builder
                .host_zone_with_one_backend(id, ServiceName::Cockroach, address)
                .unwrap();
            let dataset_name =
                sled.alloc_dataset_from_u2s(DatasetKind::Cockroach)?;
            let filesystem_pool = *dataset_name.pool();
            sled.request
                .add_zone_and_datasets(BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id,
                    zone_type: BlueprintZoneType::CockroachDb(
                        blueprint_zone_type::CockroachDb {
                            address,
                            dataset: OmicronZoneDataset {
                                pool_name: *dataset_name.pool(),
                            },
                        },
                    ),
                    filesystem_pool,
                    image_source: BlueprintZoneImageSource::InstallDataset,
                })
                .expect("freshly generated zone IDs are unique");
        }

        // Provision external DNS zones, continuing to stripe across sleds.
        // The number of DNS services depends on the number of external DNS
        // server IP addresses given to us at RSS-time.
        // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
        loop {
            let id = OmicronZoneUuid::new_v4();
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
            // With respect to internal DNS configuration, there's only one
            // address for external DNS that matters: the management (HTTP)
            // interface.
            dns_builder
                .host_zone_with_one_backend(
                    id,
                    ServiceName::ExternalDns,
                    http_address,
                )
                .unwrap();
            let dns_port = omicron_common::address::DNS_PORT;
            let dns_address = from_sockaddr_to_external_floating_addr(
                SocketAddr::new(external_ip, dns_port),
            );
            let dataset_kind = DatasetKind::ExternalDns;
            let dataset_name = sled.alloc_dataset_from_u2s(dataset_kind)?;
            let filesystem_pool = *dataset_name.pool();

            sled.request
                .add_zone_and_datasets(BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id,
                    zone_type: BlueprintZoneType::ExternalDns(
                        blueprint_zone_type::ExternalDns {
                            dataset: OmicronZoneDataset {
                                pool_name: *dataset_name.pool(),
                            },
                            http_address,
                            dns_address,
                            nic,
                        },
                    ),
                    filesystem_pool,
                    image_source: BlueprintZoneImageSource::InstallDataset,
                })
                .expect("freshly generated zone IDs are unique");
        }

        // Provision Nexus zones, continuing to stripe across sleds.
        for _ in 0..NEXUS_REDUNDANCY {
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let id = OmicronZoneUuid::new_v4();
            let ip = sled.addr_alloc.next().expect("Not enough addrs");
            let internal_address =
                SocketAddrV6::new(ip, NEXUS_INTERNAL_PORT, 0, 0);
            dns_builder
                .host_zone_nexus(id, internal_address, NEXUS_LOCKSTEP_PORT)
                .unwrap();
            let (nic, external_ip) = svc_port_builder.next_nexus(id)?;
            let filesystem_pool = sled.alloc_zpool_from_u2s()?;
            sled.request
                .add_zone_and_datasets(BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id,
                    zone_type: BlueprintZoneType::Nexus(
                        blueprint_zone_type::Nexus {
                            internal_address,
                            lockstep_port: NEXUS_LOCKSTEP_PORT,
                            external_ip: from_ipaddr_to_external_floating_ip(
                                external_ip,
                            ),
                            nic,
                            // Tell Nexus to use TLS if and only if the caller
                            // provided TLS certificates.  This effectively
                            // determines the status of TLS for the lifetime of
                            // the rack.  In production-like deployments, we'd
                            // always expect TLS to be enabled.  It's only in
                            // development that it might not be.
                            external_tls: !config
                                .external_certificates
                                .is_empty(),
                            external_dns_servers: config.dns_servers.clone(),
                            nexus_generation: Generation::new(),
                        },
                    ),
                    filesystem_pool,
                    image_source: BlueprintZoneImageSource::InstallDataset,
                })
                .expect("freshly generated zone IDs are unique");
        }

        // Provision Oximeter zones, continuing to stripe across sleds.
        // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
        for _ in 0..OXIMETER_REDUNDANCY {
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let id = OmicronZoneUuid::new_v4();
            let ip = sled.addr_alloc.next().expect("Not enough addrs");
            let address = SocketAddrV6::new(
                ip,
                omicron_common::address::OXIMETER_PORT,
                0,
                0,
            );
            dns_builder
                .host_zone_with_one_backend(id, ServiceName::Oximeter, address)
                .unwrap();
            let filesystem_pool = sled.alloc_zpool_from_u2s()?;
            sled.request
                .add_zone_and_datasets(BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id,
                    zone_type: BlueprintZoneType::Oximeter(
                        blueprint_zone_type::Oximeter { address },
                    ),
                    filesystem_pool,
                    image_source: BlueprintZoneImageSource::InstallDataset,
                })
                .expect("freshly generated zone IDs are unique");
        }

        // Provision Clickhouse zones, continuing to stripe across sleds.
        // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
        for _ in 0..SINGLE_NODE_CLICKHOUSE_REDUNDANCY {
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let id = OmicronZoneUuid::new_v4();
            let ip = sled.addr_alloc.next().expect("Not enough addrs");
            let http_port = omicron_common::address::CLICKHOUSE_HTTP_PORT;
            let http_address = SocketAddrV6::new(ip, http_port, 0, 0);
            let oximeter_read_mode_enabled = true;
            dns_builder
                .host_zone_clickhouse_single_node(
                    id,
                    ServiceName::Clickhouse,
                    http_address,
                    oximeter_read_mode_enabled,
                )
                .unwrap();
            let dataset_name =
                sled.alloc_dataset_from_u2s(DatasetKind::Clickhouse)?;
            let filesystem_pool = *dataset_name.pool();
            sled.request
                .add_zone_and_datasets(BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id,
                    zone_type: BlueprintZoneType::Clickhouse(
                        blueprint_zone_type::Clickhouse {
                            address: http_address,
                            dataset: OmicronZoneDataset {
                                pool_name: *dataset_name.pool(),
                            },
                        },
                    ),
                    filesystem_pool,
                    image_source: BlueprintZoneImageSource::InstallDataset,
                })
                .expect("freshly generated zone IDs are unique");
        }

        // Provision Crucible Pantry zones, continuing to stripe across sleds.
        // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
        for _ in 0..CRUCIBLE_PANTRY_REDUNDANCY {
            let sled = {
                let which_sled =
                    sled_allocator.next().ok_or(PlanError::NotEnoughSleds)?;
                &mut sled_info[which_sled]
            };
            let ip = sled.addr_alloc.next().expect("Not enough addrs");
            let port = omicron_common::address::CRUCIBLE_PANTRY_PORT;
            let address = SocketAddrV6::new(ip, port, 0, 0);
            let id = OmicronZoneUuid::new_v4();
            let filesystem_pool = sled.alloc_zpool_from_u2s()?;
            dns_builder
                .host_zone_with_one_backend(
                    id,
                    ServiceName::CruciblePantry,
                    address,
                )
                .unwrap();
            sled.request
                .add_zone_and_datasets(BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id,
                    zone_type: BlueprintZoneType::CruciblePantry(
                        blueprint_zone_type::CruciblePantry { address },
                    ),
                    filesystem_pool,
                    image_source: BlueprintZoneImageSource::InstallDataset,
                })
                .expect("freshly generated zone IDs are unique");
        }

        // Provision a Crucible zone on every zpool on every Sled.
        // TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove
        for sled in sled_info.iter_mut() {
            for pool in &sled.u2_zpools {
                let ip = sled.addr_alloc.next().expect("Not enough addrs");
                let port = omicron_common::address::CRUCIBLE_PORT;
                let address = SocketAddrV6::new(ip, port, 0, 0);
                let id = OmicronZoneUuid::new_v4();
                dns_builder
                    .host_zone_with_one_backend(
                        id,
                        ServiceName::Crucible(id),
                        address,
                    )
                    .unwrap();

                sled.request
                    .add_zone_and_datasets(BlueprintZoneConfig {
                        disposition: BlueprintZoneDisposition::InService,
                        id,
                        zone_type: BlueprintZoneType::Crucible(
                            blueprint_zone_type::Crucible {
                                address,
                                dataset: OmicronZoneDataset {
                                    pool_name: *pool,
                                },
                            },
                        ),
                        filesystem_pool: *pool,
                        image_source: BlueprintZoneImageSource::InstallDataset,
                    })
                    .expect("freshly generated zone IDs are unique");
            }
        }

        // All sleds get an NTP server, but the first few are nominated as
        // boundary servers, responsible for communicating with the external
        // network.
        let mut boundary_ntp_servers = vec![];
        for (idx, sled) in sled_info.iter_mut().enumerate() {
            let id = OmicronZoneUuid::new_v4();
            let ip = sled.addr_alloc.next().expect("Not enough addrs");
            let ntp_address = SocketAddrV6::new(ip, NTP_PORT, 0, 0);
            let filesystem_pool = sled.alloc_zpool_from_u2s()?;

            let (zone_type, svcname) = if idx < BOUNDARY_NTP_REDUNDANCY {
                boundary_ntp_servers
                    .push(Host::for_zone(Zone::Other(id)).fqdn());
                let (nic, snat_cfg) = svc_port_builder.next_snat(id)?;
                (
                    BlueprintZoneType::BoundaryNtp(
                        blueprint_zone_type::BoundaryNtp {
                            address: ntp_address,
                            ntp_servers: config.ntp_servers.clone(),
                            dns_servers: config.dns_servers.clone(),
                            domain: None,
                            nic,
                            external_ip:
                                from_source_nat_config_to_external_snat_ip(
                                    snat_cfg,
                                ),
                        },
                    ),
                    ServiceName::BoundaryNtp,
                )
            } else {
                (
                    BlueprintZoneType::InternalNtp(
                        blueprint_zone_type::InternalNtp {
                            address: ntp_address,
                        },
                    ),
                    ServiceName::InternalNtp,
                )
            };

            dns_builder
                .host_zone_with_one_backend(id, svcname, ntp_address)
                .unwrap();

            sled.request
                .add_zone_and_datasets(BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id,
                    zone_type,
                    filesystem_pool,
                    image_source: BlueprintZoneImageSource::InstallDataset,
                })
                .expect("freshly generated zone IDs are unique");
        }

        let all_sleds = sled_info
            .into_iter()
            .map(|sled_info| PlannedSledDescription {
                underlay_address: sled_info.sled_address,
                sled_id: sled_info.sled_id,
                subnet: sled_info.subnet,
                config: sled_info.request,
            })
            .collect();

        let dns_config = dns_builder.build_full_config_for_initial_generation();
        Ok(Self { all_sleds, dns_config })
    }

    pub async fn create(
        log: &Logger,
        config: &Config,
        sleds: &BTreeMap<SocketAddrV6, StartSledAgentRequest>,
    ) -> Result<Self, PlanError> {
        // Load the information we need about each Sled to be able to allocate
        // components on it.
        let sled_info = {
            let result: Result<Vec<SledInfo>, PlanError> =
                futures::future::try_join_all(sleds.values().map(
                    |sled_request| async {
                        let subnet = sled_request.body.subnet;
                        let sled_address = get_sled_address(subnet);
                        let inventory =
                            Self::get_inventory(log, sled_address).await?;
                        let is_scrimlet = match inventory.sled_role {
                            SledRole::Gimlet => false,
                            SledRole::Scrimlet => true,
                        };
                        Ok(SledInfo::new(
                            sled_request.body.id,
                            subnet,
                            sled_address,
                            inventory,
                            is_scrimlet,
                        ))
                    },
                ))
                .await;
            result?
        };

        let plan = Self::create_transient(config, sled_info)?;
        Ok(plan)
    }

    pub(crate) fn to_blueprint(
        &self,
        sled_agent_config_generation: Generation,
    ) -> anyhow::Result<Blueprint> {
        let mut blueprint_sleds = BTreeMap::new();
        for sled_description in &self.all_sleds {
            let sled_config = &sled_description.config;
            let mut datasets = IdOrdMap::new();
            for d in sled_config.datasets.values() {
                // Only the "Crucible" dataset needs to know the address
                let address = if *d.name.kind() == DatasetKind::Crucible {
                    let address = sled_config.zones.iter().find_map(|z| {
                        if let BlueprintZoneType::Crucible(
                            blueprint_zone_type::Crucible { address, dataset },
                        ) = &z.zone_type
                        {
                            if &dataset.pool_name == d.name.pool() {
                                return Some(*address);
                            }
                        };
                        None
                    });
                    if address.is_some() {
                        address
                    } else {
                        bail!(
                            "could not find Crucible zone for zpool {}",
                            d.name.pool()
                        )
                    }
                } else {
                    None
                };

                datasets
                    .insert_unique(BlueprintDatasetConfig {
                        disposition: BlueprintDatasetDisposition::InService,
                        id: d.id,
                        pool: *d.name.pool(),
                        kind: d.name.kind().clone(),
                        address,
                        compression: d.inner.compression,
                        quota: d.inner.quota,
                        reservation: d.inner.reservation,
                    })
                    .map_err(|e| {
                        anyhow!(InlineErrorChain::new(&e).to_string())
                    })?;
            }

            blueprint_sleds.insert(
                sled_description.sled_id,
                BlueprintSledConfig {
                    state: SledState::Active,
                    subnet: sled_description.subnet,
                    sled_agent_generation: sled_agent_config_generation,
                    disks: sled_config.disks.clone(),
                    datasets,
                    zones: sled_config.zones.clone(),
                    host_phase_2:
                        BlueprintHostPhase2DesiredSlots::current_contents(),
                    remove_mupdate_override: None,
                },
            );
        }

        let id = BlueprintUuid::new_v4();
        Ok(Blueprint {
            id,
            sleds: blueprint_sleds,
            pending_mgs_updates: PendingMgsUpdates::new(),
            parent_blueprint_id: None,
            internal_dns_version: self.dns_config.generation,
            // We don't configure external DNS during RSS, so set it to an
            // initial generation of 1. Nexus will bump this up when it updates
            // external DNS (including creating the recovery silo).
            external_dns_version: Generation::new(),
            target_release_minimum_generation: Generation::new(),
            nexus_generation: Generation::new(),
            // Nexus will fill in the CockroachDB values during initialization.
            cockroachdb_fingerprint: String::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            // We do not create clickhouse clusters in RSS. We create them via
            // reconfigurator only.
            clickhouse_cluster_config: None,
            // The oximeter read policy always defaults to single node. The
            // initial generation of this policy in the DB is 1
            oximeter_read_mode: OximeterReadMode::SingleNode,
            oximeter_read_version: Generation::new(),
            time_created: Utc::now(),
            creator: "RSS".to_string(),
            comment: "initial blueprint from rack setup".to_string(),
            source: BlueprintSource::Rss,
        })
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
pub struct SledInfo {
    /// unique id for the sled agent
    pub sled_id: SledUuid,
    /// the sled's unique IPv6 subnet
    subnet: Ipv6Subnet<SLED_PREFIX>,
    /// the address of the Sled Agent on the sled's subnet
    pub sled_address: SocketAddrV6,
    /// the inventory returned by the Sled
    inventory: Inventory,
    /// The Zpools available for usage by services
    u2_zpools: Vec<ZpoolName>,
    /// spreads components across a Sled's zpools
    u2_zpool_allocators:
        HashMap<DatasetKind, Box<dyn Iterator<Item = usize> + Send + Sync>>,
    /// whether this Sled is a scrimlet
    is_scrimlet: bool,
    /// allocator for addresses in this Sled's subnet
    addr_alloc: AddressBumpAllocator,
    /// under-construction list of Omicron zones being deployed to a Sled
    request: SledConfig,
}

impl SledInfo {
    pub fn new(
        sled_id: SledUuid,
        subnet: Ipv6Subnet<SLED_PREFIX>,
        sled_address: SocketAddrV6,
        inventory: Inventory,
        is_scrimlet: bool,
    ) -> SledInfo {
        SledInfo {
            sled_id,
            subnet,
            sled_address,
            inventory,
            u2_zpools: vec![],
            u2_zpool_allocators: HashMap::new(),
            is_scrimlet,
            addr_alloc: AddressBumpAllocator::new(subnet),
            request: Default::default(),
        }
    }

    fn alloc_zpool_from_u2s(&self) -> Result<ZpoolName, PlanError> {
        self.u2_zpools
            .choose(&mut rand::rng())
            .map(|z| *z)
            .ok_or_else(|| PlanError::NotEnoughSleds)
    }

    /// Allocates a dataset of the specified type from one of the U.2 pools on
    /// this Sled
    fn alloc_dataset_from_u2s(
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
                Ok(DatasetName::new(self.u2_zpools[which_zpool], kind))
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
        use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
        use omicron_common::address::{
            DNS_OPTE_IPV4_SUBNET, DNS_OPTE_IPV6_SUBNET, NEXUS_OPTE_IPV4_SUBNET,
            NEXUS_OPTE_IPV6_SUBNET, NTP_OPTE_IPV4_SUBNET, NTP_OPTE_IPV6_SUBNET,
        };

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
                .addr_iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
        );
        let dns_v6_ips = Box::new(
            DNS_OPTE_IPV6_SUBNET.iter().skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
        );
        let nexus_v4_ips = Box::new(
            NEXUS_OPTE_IPV4_SUBNET
                .addr_iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
        );
        let nexus_v6_ips = Box::new(
            NEXUS_OPTE_IPV6_SUBNET
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
        );
        let ntp_v4_ips = Box::new(
            NTP_OPTE_IPV4_SUBNET
                .addr_iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
        );
        let ntp_v6_ips = Box::new(
            NTP_OPTE_IPV6_SUBNET.iter().skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
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

    fn next_dns(
        &mut self,
        svc_id: OmicronZoneUuid,
    ) -> Option<(NetworkInterface, IpAddr)> {
        use omicron_common::address::{
            DNS_OPTE_IPV4_SUBNET, DNS_OPTE_IPV6_SUBNET,
        };
        let external_ip = self.external_dns_ips.next()?;

        let ip_config = match external_ip {
            IpAddr::V4(_) => PrivateIpConfig::new_ipv4(
                self.dns_v4_ips.next().unwrap(),
                *DNS_OPTE_IPV4_SUBNET,
            )
            .ok()?,
            IpAddr::V6(_) => PrivateIpConfig::new_ipv6(
                self.dns_v6_ips.next().unwrap(),
                *DNS_OPTE_IPV6_SUBNET,
            )
            .ok()?,
        };

        let nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Service {
                // TODO-cleanup use TypedUuid everywhere
                id: svc_id.into_untyped_uuid(),
            },
            name: format!("external-dns-{svc_id}").parse().unwrap(),
            ip_config,
            mac: self.random_mac(),
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
        };

        Some((nic, external_ip))
    }

    fn next_nexus(
        &mut self,
        svc_id: OmicronZoneUuid,
    ) -> Result<(NetworkInterface, IpAddr), PlanError> {
        use omicron_common::address::{
            NEXUS_OPTE_IPV4_SUBNET, NEXUS_OPTE_IPV6_SUBNET,
        };
        let external_ip = self
            .next_internal_service_ip()
            .ok_or_else(|| PlanError::ServiceIp("Nexus"))?;

        let ip_config = match external_ip {
            IpAddr::V4(_) => PrivateIpConfig::new_ipv4(
                self.nexus_v4_ips.next().unwrap(),
                *NEXUS_OPTE_IPV4_SUBNET,
            )?,
            IpAddr::V6(_) => PrivateIpConfig::new_ipv6(
                self.nexus_v6_ips.next().unwrap(),
                *NEXUS_OPTE_IPV6_SUBNET,
            )?,
        };

        let nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Service {
                // TODO-cleanup use TypedUuid everywhere
                id: svc_id.into_untyped_uuid(),
            },
            name: format!("nexus-{svc_id}").parse().unwrap(),
            ip_config,
            mac: self.random_mac(),
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
        };

        Ok((nic, external_ip))
    }

    fn next_snat(
        &mut self,
        svc_id: OmicronZoneUuid,
    ) -> Result<(NetworkInterface, SourceNatConfigGeneric), PlanError> {
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
        } else {
            self.next_snat_ip = Some(snat_ip);
        }

        let snat_cfg =
            match SourceNatConfigGeneric::new(snat_ip, first_port, last_port) {
                Ok(cfg) => cfg,
                // We know our port pair is aligned, making this unreachable.
                Err(err @ SourceNatConfigError::UnalignedPortPair { .. }) => {
                    unreachable!("{err}");
                }
            };

        let ip_config = match snat_ip {
            IpAddr::V4(_) => PrivateIpConfig::new_ipv4(
                self.ntp_v4_ips.next().unwrap(),
                *NTP_OPTE_IPV4_SUBNET,
            )?,
            IpAddr::V6(_) => PrivateIpConfig::new_ipv6(
                self.ntp_v6_ips.next().unwrap(),
                *NTP_OPTE_IPV6_SUBNET,
            )?,
        };

        let nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Service {
                // TODO-cleanup use TypedUuid everywhere
                id: svc_id.into_untyped_uuid(),
            },
            name: format!("ntp-{svc_id}").parse().unwrap(),
            ip_config,
            mac: self.random_mac(),
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
    use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
    use nexus_sled_agent_shared::inventory::SledCpuFamily;
    use nexus_sled_agent_shared::inventory::ZoneImageResolverInventory;
    use omicron_common::address::IpRange;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::internal::shared::AllowedSourceIps;
    use omicron_common::api::internal::shared::RackNetworkConfig;
    use oxnet::Ipv6Net;
    use sled_agent_types::rack_init::BootstrapAddressDiscovery;
    use sled_agent_types::rack_init::RecoverySiloConfig;
    use sled_hardware_types::Baseboard;

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

    fn test_config(ip_pools: &[(&str, &str)], dns_ips: &[&str]) -> Config {
        Config {
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
                // Generated via `cargo run --example argon2`.
                user_password_hash: "$argon2id$v=19$m=98304,t=23,\
                    p=1$Naz/hHpgS8GXQqT8Zm0Nog$ucAKOsMiq70xtAEaLCY\
                    unjEgDyjSnuXaKTfmMKpKQIA"
                    .parse()
                    .unwrap(),
            },
            rack_network_config: RackNetworkConfig {
                rack_subnet: Ipv6Net::host_net(Ipv6Addr::LOCALHOST),
                infra_ip_first: Ipv4Addr::LOCALHOST,
                infra_ip_last: Ipv4Addr::LOCALHOST,
                ports: Vec::new(),
                bgp: Vec::new(),
                bfd: Vec::new(),
            },
            allowed_source_ips: AllowedSourceIps::Any,
        }
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
        let config = test_config(&ip_pools, &dns_ips);

        let mut svp = ServicePortBuilder::new(&config);

        // We should only get back the 5 DNS IPs we specified.
        let mut svp_dns_ips = Vec::new();
        while let Some((_interface, ip)) =
            svp.next_dns(OmicronZoneUuid::new_v4())
        {
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
    fn test_dataset_and_zone_count() {
        // We still need these values to provision external services
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

        let config = test_config(&ip_pools, &dns_ips);

        // Confirm that this fails with no sleds
        let sleds = vec![];
        Plan::create_transient(&config, sleds)
            .expect_err("Should have failed to create plan");

        // Try again, with a sled that has ten U.2 disks
        let sled_id = SledUuid::new_v4();
        let address = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0);
        let subnet = Ipv6Subnet::<SLED_PREFIX>::new(address);
        let sled_address = get_sled_address(subnet);
        let is_scrimlet = true;

        const DISK_COUNT: usize = 10;
        let disks: Vec<_> = (0..DISK_COUNT)
            .map(|i| nexus_sled_agent_shared::inventory::InventoryDisk {
                identity: omicron_common::disk::DiskIdentity {
                    vendor: "vendor".to_string(),
                    model: "model".to_string(),
                    serial: format!("test-{i}"),
                },
                variant: DiskVariant::U2,
                slot: i as i64,
                active_firmware_slot: 0,
                next_active_firmware_slot: None,
                number_of_firmware_slots: 8,
                slot1_is_read_only: false,
                slot_firmware_versions: vec![],
            })
            .collect();

        let sleds = vec![SledInfo::new(
            sled_id,
            subnet,
            sled_address,
            Inventory {
                sled_id,
                sled_agent_address: sled_address,
                sled_role: SledRole::Scrimlet,
                baseboard: Baseboard::Unknown,
                usable_hardware_threads: 32,
                usable_physical_ram: ByteCount::try_from(1_u64 << 40).unwrap(),
                cpu_family: SledCpuFamily::AmdMilan,
                reservoir_size: ByteCount::try_from(1_u64 << 40).unwrap(),
                disks,
                zpools: vec![],
                datasets: vec![],
                ledgered_sled_config: None,
                reconciler_status: ConfigReconcilerInventoryStatus::NotYetRun,
                last_reconciliation: None,
                zone_image_resolver: ZoneImageResolverInventory::new_fake(),
            },
            is_scrimlet,
        )];

        let plan = Plan::create_transient(&config, sleds)
            .expect("Should have created plan");

        assert_eq!(plan.all_sleds.len(), 1);

        let sled_config = &plan.all_sleds.iter().next().unwrap().config;
        assert_eq!(sled_config.disks.len(), DISK_COUNT);

        let zone_count = sled_config.zones.len();

        let expected_zone_count = INTERNAL_DNS_REDUNDANCY
            + COCKROACHDB_REDUNDANCY
            + dns_ips.len()
            + NEXUS_REDUNDANCY
            + OXIMETER_REDUNDANCY
            + SINGLE_NODE_CLICKHOUSE_REDUNDANCY
            + CRUCIBLE_PANTRY_REDUNDANCY
            + DISK_COUNT // (Crucible)
            + 1; // (NTP)
        assert_eq!(
            zone_count, expected_zone_count,
            "Saw: {:#?}, expected {expected_zone_count}",
            sled_config.zones
        );

        let expected_dataset_count = expected_zone_count // Transient zones
            + INTERNAL_DNS_REDUNDANCY
            + COCKROACHDB_REDUNDANCY
            + SINGLE_NODE_CLICKHOUSE_REDUNDANCY
            + dns_ips.len()
            + DISK_COUNT * 4; // (Debug, Root, Local Storage, Crucible)
        assert_eq!(
            sled_config.datasets.len(),
            expected_dataset_count,
            "Saw: {:#?}, expected {expected_dataset_count}",
            sled_config.datasets
        );
    }
}
