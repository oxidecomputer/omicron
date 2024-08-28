// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for representing the deployed software and configuration in the
//! database

use crate::inventory::ZoneType;
use crate::omicron_zone_config::{self, OmicronZoneNic};
use crate::schema::{
    blueprint, bp_omicron_physical_disk, bp_omicron_zone, bp_omicron_zone_nic,
    bp_sled_omicron_physical_disks, bp_sled_omicron_zones, bp_sled_state,
    bp_target,
};
use crate::typed_uuid::DbTypedUuid;
use crate::{
    impl_enum_type, ipv6, Generation, MacAddr, Name, SledState, SqlU16, SqlU32,
    SqlU8,
};
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use ipnetwork::IpNetwork;
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZonesConfig;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use nexus_types::deployment::{
    blueprint_zone_type, BlueprintPhysicalDisksConfig,
};
use nexus_types::deployment::{BlueprintPhysicalDiskConfig, BlueprintZoneType};
use nexus_types::deployment::{
    OmicronZoneExternalFloatingAddr, OmicronZoneExternalFloatingIp,
    OmicronZoneExternalSnatIp,
};
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::disk::DiskIdentity;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use omicron_uuid_kinds::{ExternalIpKind, SledKind, ZpoolKind};
use omicron_uuid_kinds::{ExternalIpUuid, GenericUuid, OmicronZoneUuid};
use std::net::{IpAddr, SocketAddrV6};
use uuid::Uuid;

/// See [`nexus_types::deployment::Blueprint`].
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = blueprint)]
pub struct Blueprint {
    pub id: Uuid,
    pub parent_blueprint_id: Option<Uuid>,
    pub internal_dns_version: Generation,
    pub external_dns_version: Generation,
    pub cockroachdb_fingerprint: String,
    pub cockroachdb_setting_preserve_downgrade: Option<String>,
    pub time_created: DateTime<Utc>,
    pub creator: String,
    pub comment: String,
}

impl From<&'_ nexus_types::deployment::Blueprint> for Blueprint {
    fn from(bp: &'_ nexus_types::deployment::Blueprint) -> Self {
        Self {
            id: bp.id,
            parent_blueprint_id: bp.parent_blueprint_id,
            internal_dns_version: Generation(bp.internal_dns_version),
            external_dns_version: Generation(bp.external_dns_version),
            cockroachdb_fingerprint: bp.cockroachdb_fingerprint.clone(),
            cockroachdb_setting_preserve_downgrade: bp
                .cockroachdb_setting_preserve_downgrade
                .to_optional_string(),
            time_created: bp.time_created,
            creator: bp.creator.clone(),
            comment: bp.comment.clone(),
        }
    }
}

impl From<Blueprint> for nexus_types::deployment::BlueprintMetadata {
    fn from(value: Blueprint) -> Self {
        Self {
            id: value.id,
            parent_blueprint_id: value.parent_blueprint_id,
            internal_dns_version: *value.internal_dns_version,
            external_dns_version: *value.external_dns_version,
            cockroachdb_fingerprint: value.cockroachdb_fingerprint,
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::from_optional_string(
                    &value.cockroachdb_setting_preserve_downgrade,
                )
                .ok(),
            time_created: value.time_created,
            creator: value.creator,
            comment: value.comment,
        }
    }
}

/// See [`nexus_types::deployment::BlueprintTarget`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_target)]
pub struct BpTarget {
    pub version: SqlU32,
    pub blueprint_id: Uuid,
    pub enabled: bool,
    pub time_made_target: DateTime<Utc>,
}

impl BpTarget {
    pub fn new(version: u32, target: BlueprintTarget) -> Self {
        Self {
            version: version.into(),
            blueprint_id: target.target_id,
            enabled: target.enabled,
            time_made_target: target.time_made_target,
        }
    }
}

impl From<BpTarget> for nexus_types::deployment::BlueprintTarget {
    fn from(value: BpTarget) -> Self {
        Self {
            target_id: value.blueprint_id,
            enabled: value.enabled,
            time_made_target: value.time_made_target,
        }
    }
}

/// See [`nexus_types::deployment::Blueprint::sled_state`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_sled_state)]
pub struct BpSledState {
    pub blueprint_id: Uuid,
    pub sled_id: DbTypedUuid<SledKind>,
    pub sled_state: SledState,
}

/// See [`nexus_types::deployment::BlueprintPhysicalDisksConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_sled_omicron_physical_disks)]
pub struct BpSledOmicronPhysicalDisks {
    pub blueprint_id: Uuid,
    pub sled_id: Uuid,
    pub generation: Generation,
}

impl BpSledOmicronPhysicalDisks {
    pub fn new(
        blueprint_id: Uuid,
        sled_id: Uuid,
        disks_config: &BlueprintPhysicalDisksConfig,
    ) -> Self {
        Self {
            blueprint_id,
            sled_id,
            generation: Generation(disks_config.generation),
        }
    }
}

/// See [`nexus_types::deployment::BlueprintPhysicalDiskConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_omicron_physical_disk)]
pub struct BpOmicronPhysicalDisk {
    pub blueprint_id: Uuid,
    pub sled_id: Uuid,

    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub id: Uuid,
    pub pool_id: Uuid,
}

impl BpOmicronPhysicalDisk {
    pub fn new(
        blueprint_id: Uuid,
        sled_id: Uuid,
        disk_config: &BlueprintPhysicalDiskConfig,
    ) -> Self {
        Self {
            blueprint_id,
            sled_id,
            vendor: disk_config.identity.vendor.clone(),
            serial: disk_config.identity.serial.clone(),
            model: disk_config.identity.model.clone(),
            id: disk_config.id,
            pool_id: disk_config.pool_id.into_untyped_uuid(),
        }
    }
}

impl From<BpOmicronPhysicalDisk> for BlueprintPhysicalDiskConfig {
    fn from(disk: BpOmicronPhysicalDisk) -> Self {
        Self {
            identity: DiskIdentity {
                vendor: disk.vendor,
                serial: disk.serial,
                model: disk.model,
            },
            id: disk.id,
            pool_id: ZpoolUuid::from_untyped_uuid(disk.pool_id),
        }
    }
}

/// See [`nexus_types::deployment::BlueprintZonesConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_sled_omicron_zones)]
pub struct BpSledOmicronZones {
    pub blueprint_id: Uuid,
    pub sled_id: DbTypedUuid<SledKind>,
    pub generation: Generation,
}

impl BpSledOmicronZones {
    pub fn new(
        blueprint_id: Uuid,
        sled_id: SledUuid,
        zones_config: &BlueprintZonesConfig,
    ) -> Self {
        Self {
            blueprint_id,
            sled_id: sled_id.into(),
            generation: Generation(zones_config.generation),
        }
    }
}
/// See [`nexus_types::deployment::BlueprintZoneConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_omicron_zone)]
pub struct BpOmicronZone {
    pub blueprint_id: Uuid,
    pub sled_id: DbTypedUuid<SledKind>,
    pub id: Uuid,
    pub underlay_address: ipv6::Ipv6Addr,
    pub zone_type: ZoneType,
    pub primary_service_ip: ipv6::Ipv6Addr,
    pub primary_service_port: SqlU16,
    pub second_service_ip: Option<IpNetwork>,
    pub second_service_port: Option<SqlU16>,
    pub dataset_zpool_name: Option<String>,
    pub bp_nic_id: Option<Uuid>,
    pub dns_gz_address: Option<ipv6::Ipv6Addr>,
    pub dns_gz_address_index: Option<SqlU32>,
    pub ntp_ntp_servers: Option<Vec<String>>,
    pub ntp_dns_servers: Option<Vec<IpNetwork>>,
    pub ntp_domain: Option<String>,
    pub nexus_external_tls: Option<bool>,
    pub nexus_external_dns_servers: Option<Vec<IpNetwork>>,
    pub snat_ip: Option<IpNetwork>,
    pub snat_first_port: Option<SqlU16>,
    pub snat_last_port: Option<SqlU16>,

    disposition: DbBpZoneDisposition,

    pub external_ip_id: Option<DbTypedUuid<ExternalIpKind>>,
    pub filesystem_pool: Option<DbTypedUuid<ZpoolKind>>,
}

impl BpOmicronZone {
    pub fn new(
        blueprint_id: Uuid,
        sled_id: SledUuid,
        blueprint_zone: &BlueprintZoneConfig,
    ) -> anyhow::Result<Self> {
        let external_ip_id = blueprint_zone
            .zone_type
            .external_networking()
            .map(|(ip, _)| ip.id().into());

        // Create a dummy record to start, then fill in the rest
        let mut bp_omicron_zone = BpOmicronZone {
            // Fill in the known fields that don't require inspecting
            // `blueprint_zone.zone_type`
            blueprint_id,
            sled_id: sled_id.into(),
            id: blueprint_zone.id.into_untyped_uuid(),
            underlay_address: blueprint_zone.underlay_address.into(),
            external_ip_id,
            filesystem_pool: blueprint_zone
                .filesystem_pool
                .as_ref()
                .map(|pool| pool.id().into()),

            // Set the remainder of the fields to a default
            disposition: DbBpZoneDisposition::InService,
            zone_type: ZoneType::BoundaryNtp,
            primary_service_ip: "::1"
                .parse::<std::net::Ipv6Addr>()
                .unwrap()
                .into(),
            primary_service_port: 0.into(),
            second_service_ip: None,
            second_service_port: None,
            dataset_zpool_name: None,
            bp_nic_id: None,
            dns_gz_address: None,
            dns_gz_address_index: None,
            ntp_ntp_servers: None,
            ntp_dns_servers: None,
            ntp_domain: None,
            nexus_external_tls: None,
            nexus_external_dns_servers: None,
            snat_ip: None,
            snat_first_port: None,
            snat_last_port: None,
        };

        match &blueprint_zone.zone_type {
            BlueprintZoneType::BoundaryNtp(
                blueprint_zone_type::BoundaryNtp {
                    address,
                    ntp_servers,
                    dns_servers,
                    domain,
                    nic,
                    external_ip,
                },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.zone_type = ZoneType::BoundaryNtp;

                // Set the zone specific fields
                let snat_cfg = external_ip.snat_cfg;
                let (first_port, last_port) = snat_cfg.port_range_raw();
                bp_omicron_zone.ntp_ntp_servers = Some(ntp_servers.clone());
                bp_omicron_zone.ntp_dns_servers = Some(
                    dns_servers
                        .into_iter()
                        .cloned()
                        .map(IpNetwork::from)
                        .collect(),
                );
                bp_omicron_zone.ntp_domain.clone_from(domain);
                bp_omicron_zone.snat_ip = Some(IpNetwork::from(snat_cfg.ip));
                bp_omicron_zone.snat_first_port =
                    Some(SqlU16::from(first_port));
                bp_omicron_zone.snat_last_port = Some(SqlU16::from(last_port));
                bp_omicron_zone.bp_nic_id = Some(nic.id);
            }
            BlueprintZoneType::Clickhouse(
                blueprint_zone_type::Clickhouse { address, dataset },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.set_zpool_name(dataset);
                bp_omicron_zone.zone_type = ZoneType::Clickhouse;
            }
            BlueprintZoneType::ClickhouseKeeper(
                blueprint_zone_type::ClickhouseKeeper { address, dataset },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.set_zpool_name(dataset);
                bp_omicron_zone.zone_type = ZoneType::ClickhouseKeeper;
            }
            BlueprintZoneType::ClickhouseServer(
                blueprint_zone_type::ClickhouseServer { address, dataset },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.set_zpool_name(dataset);
                bp_omicron_zone.zone_type = ZoneType::ClickhouseServer;
            }
            BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb { address, dataset },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.set_zpool_name(dataset);
                bp_omicron_zone.zone_type = ZoneType::CockroachDb;
            }
            BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                address,
                dataset,
            }) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.set_zpool_name(dataset);
                bp_omicron_zone.zone_type = ZoneType::Crucible;
            }
            BlueprintZoneType::CruciblePantry(
                blueprint_zone_type::CruciblePantry { address },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.zone_type = ZoneType::CruciblePantry;
            }
            BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns {
                    dataset,
                    http_address,
                    dns_address,
                    nic,
                },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(http_address);
                bp_omicron_zone.set_zpool_name(dataset);
                bp_omicron_zone.zone_type = ZoneType::ExternalDns;

                // Set the zone specific fields
                bp_omicron_zone.bp_nic_id = Some(nic.id);
                bp_omicron_zone.second_service_ip =
                    Some(IpNetwork::from(dns_address.addr.ip()));
                bp_omicron_zone.second_service_port =
                    Some(SqlU16::from(dns_address.addr.port()));
            }
            BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns {
                    dataset,
                    http_address,
                    dns_address,
                    gz_address,
                    gz_address_index,
                },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(http_address);
                bp_omicron_zone.set_zpool_name(dataset);
                bp_omicron_zone.zone_type = ZoneType::InternalDns;

                // Set the zone specific fields
                bp_omicron_zone.second_service_ip =
                    Some(IpNetwork::from(IpAddr::V6(*dns_address.ip())));
                bp_omicron_zone.second_service_port =
                    Some(SqlU16::from(dns_address.port()));

                bp_omicron_zone.dns_gz_address =
                    Some(ipv6::Ipv6Addr::from(gz_address));
                bp_omicron_zone.dns_gz_address_index =
                    Some(SqlU32::from(*gz_address_index));
            }
            BlueprintZoneType::InternalNtp(
                blueprint_zone_type::InternalNtp {
                    address,
                    ntp_servers,
                    dns_servers,
                    domain,
                },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.zone_type = ZoneType::InternalNtp;

                // Set the zone specific fields
                bp_omicron_zone.ntp_ntp_servers = Some(ntp_servers.clone());
                bp_omicron_zone.ntp_dns_servers = Some(
                    dns_servers.iter().cloned().map(IpNetwork::from).collect(),
                );
                bp_omicron_zone.ntp_domain.clone_from(domain);
            }
            BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                internal_address,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            }) => {
                // Set the common fields
                bp_omicron_zone
                    .set_primary_service_ip_and_port(internal_address);
                bp_omicron_zone.zone_type = ZoneType::Nexus;

                // Set the zone specific fields
                bp_omicron_zone.bp_nic_id = Some(nic.id);
                bp_omicron_zone.second_service_ip =
                    Some(IpNetwork::from(external_ip.ip));
                bp_omicron_zone.nexus_external_tls = Some(*external_tls);
                bp_omicron_zone.nexus_external_dns_servers = Some(
                    external_dns_servers
                        .iter()
                        .cloned()
                        .map(IpNetwork::from)
                        .collect(),
                );
            }
            BlueprintZoneType::Oximeter(blueprint_zone_type::Oximeter {
                address,
            }) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.zone_type = ZoneType::Oximeter;
            }
        }

        Ok(bp_omicron_zone)
    }

    fn set_primary_service_ip_and_port(&mut self, address: &SocketAddrV6) {
        let (primary_service_ip, primary_service_port) =
            (ipv6::Ipv6Addr::from(*address.ip()), SqlU16::from(address.port()));
        self.primary_service_ip = primary_service_ip;
        self.primary_service_port = primary_service_port;
    }

    fn set_zpool_name(&mut self, dataset: &OmicronZoneDataset) {
        self.dataset_zpool_name = Some(dataset.pool_name.to_string());
    }
    /// Convert an external ip from a `BpOmicronZone` to a `BlueprintZoneType`
    /// representation.
    fn external_ip_to_blueprint_zone_type(
        external_ip: Option<DbTypedUuid<ExternalIpKind>>,
    ) -> anyhow::Result<ExternalIpUuid> {
        external_ip
            .map(Into::into)
            .ok_or_else(|| anyhow!("expected an external IP ID"))
    }

    pub fn into_blueprint_zone_config(
        self,
        nic_row: Option<BpOmicronZoneNic>,
    ) -> anyhow::Result<BlueprintZoneConfig> {
        // Build up a set of common fields for our `BlueprintZoneType`s
        //
        // Some of these are results that we only evaluate when used, because
        // not all zone types use all common fields.
        let primary_address =
            omicron_zone_config::primary_ip_and_port_to_socketaddr_v6(
                self.primary_service_ip.into(),
                self.primary_service_port,
            );
        let dataset =
            omicron_zone_config::dataset_zpool_name_to_omicron_zone_dataset(
                self.dataset_zpool_name,
            );

        let nic = omicron_zone_config::nic_row_to_network_interface(
            self.id,
            self.bp_nic_id,
            nic_row.map(Into::into),
        );
        let external_ip_id =
            Self::external_ip_to_blueprint_zone_type(self.external_ip_id);

        let dns_address =
            omicron_zone_config::secondary_ip_and_port_to_dns_address(
                self.second_service_ip,
                self.second_service_port,
            );

        let ntp_dns_servers =
            omicron_zone_config::ntp_dns_servers_to_omicron_internal(
                self.ntp_dns_servers,
            );

        let ntp_servers = omicron_zone_config::ntp_servers_to_omicron_internal(
            self.ntp_ntp_servers,
        );

        let zone_type = match self.zone_type {
            ZoneType::BoundaryNtp => {
                let snat_cfg = match (
                    self.snat_ip,
                    self.snat_first_port,
                    self.snat_last_port,
                ) {
                    (Some(ip), Some(first_port), Some(last_port)) => {
                        nexus_types::inventory::SourceNatConfig::new(
                            ip.ip(),
                            *first_port,
                            *last_port,
                        )
                        .context("bad SNAT config for boundary NTP")?
                    }
                    _ => bail!(
                        "expected non-NULL snat properties, \
                         found at least one NULL"
                    ),
                };
                BlueprintZoneType::BoundaryNtp(
                    blueprint_zone_type::BoundaryNtp {
                        address: primary_address,
                        ntp_servers: ntp_servers?,
                        dns_servers: ntp_dns_servers?,
                        domain: self.ntp_domain,
                        nic: nic?,
                        external_ip: OmicronZoneExternalSnatIp {
                            id: external_ip_id?,
                            snat_cfg,
                        },
                    },
                )
            }
            ZoneType::Clickhouse => {
                BlueprintZoneType::Clickhouse(blueprint_zone_type::Clickhouse {
                    address: primary_address,
                    dataset: dataset?,
                })
            }
            ZoneType::ClickhouseKeeper => BlueprintZoneType::ClickhouseKeeper(
                blueprint_zone_type::ClickhouseKeeper {
                    address: primary_address,
                    dataset: dataset?,
                },
            ),
            ZoneType::ClickhouseServer => BlueprintZoneType::ClickhouseServer(
                blueprint_zone_type::ClickhouseServer {
                    address: primary_address,
                    dataset: dataset?,
                },
            ),

            ZoneType::CockroachDb => BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb {
                    address: primary_address,
                    dataset: dataset?,
                },
            ),
            ZoneType::Crucible => {
                BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                    address: primary_address,
                    dataset: dataset?,
                })
            }
            ZoneType::CruciblePantry => BlueprintZoneType::CruciblePantry(
                blueprint_zone_type::CruciblePantry {
                    address: primary_address,
                },
            ),
            ZoneType::ExternalDns => BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns {
                    dataset: dataset?,
                    http_address: primary_address,
                    dns_address: OmicronZoneExternalFloatingAddr {
                        id: external_ip_id?,
                        addr: dns_address?,
                    },
                    nic: nic?,
                },
            ),
            ZoneType::InternalDns => BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns {
                    dataset: dataset?,
                    http_address: primary_address,
                    dns_address: omicron_zone_config::to_internal_dns_address(
                        dns_address?,
                    )?,
                    gz_address: self
                        .dns_gz_address
                        .map(Into::into)
                        .ok_or_else(|| {
                            anyhow!("expected dns_gz_address, found none")
                        })?,
                    gz_address_index: *self.dns_gz_address_index.ok_or_else(
                        || anyhow!("expected dns_gz_address_index, found none"),
                    )?,
                },
            ),
            ZoneType::InternalNtp => BlueprintZoneType::InternalNtp(
                blueprint_zone_type::InternalNtp {
                    address: primary_address,
                    ntp_servers: ntp_servers?,
                    dns_servers: ntp_dns_servers?,
                    domain: self.ntp_domain,
                },
            ),
            ZoneType::Nexus => {
                BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    internal_address: primary_address,
                    external_ip: OmicronZoneExternalFloatingIp {
                        id: external_ip_id?,
                        ip: self
                            .second_service_ip
                            .ok_or_else(|| {
                                anyhow!("expected second service IP")
                            })?
                            .ip(),
                    },
                    nic: nic?,
                    external_tls: self
                        .nexus_external_tls
                        .ok_or_else(|| anyhow!("expected 'external_tls'"))?,
                    external_dns_servers: self
                        .nexus_external_dns_servers
                        .ok_or_else(|| {
                            anyhow!("expected 'external_dns_servers'")
                        })?
                        .into_iter()
                        .map(|i| i.ip())
                        .collect(),
                })
            }
            ZoneType::Oximeter => {
                BlueprintZoneType::Oximeter(blueprint_zone_type::Oximeter {
                    address: primary_address,
                })
            }
        };

        Ok(BlueprintZoneConfig {
            disposition: self.disposition.into(),
            id: OmicronZoneUuid::from_untyped_uuid(self.id),
            underlay_address: self.underlay_address.into(),
            filesystem_pool: self
                .filesystem_pool
                .map(|id| ZpoolName::new_external(id.into())),
            zone_type,
        })
    }
}

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "bp_zone_disposition", schema = "public"))]
    pub struct DbBpZoneDispositionEnum;

    /// This type is not actually public, because [`BlueprintZoneDisposition`]
    /// interacts with external logic.
    ///
    /// However, it must be marked `pub` to avoid errors like `crate-private
    /// type `BpZoneDispositionEnum` in public interface`. Marking this type `pub`,
    /// without actually making it public, tricks rustc in a desirable way.
    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = DbBpZoneDispositionEnum)]
    pub enum DbBpZoneDisposition;

    // Enum values
    InService => b"in_service"
    Quiesced => b"quiesced"
    Expunged => b"expunged"
);

/// Converts a [`BlueprintZoneDisposition`] to a version that can be inserted
/// into a database.
pub fn to_db_bp_zone_disposition(
    disposition: BlueprintZoneDisposition,
) -> DbBpZoneDisposition {
    match disposition {
        BlueprintZoneDisposition::InService => DbBpZoneDisposition::InService,
        BlueprintZoneDisposition::Quiesced => DbBpZoneDisposition::Quiesced,
        BlueprintZoneDisposition::Expunged => DbBpZoneDisposition::Expunged,
    }
}

impl From<DbBpZoneDisposition> for BlueprintZoneDisposition {
    fn from(disposition: DbBpZoneDisposition) -> Self {
        match disposition {
            DbBpZoneDisposition::InService => {
                BlueprintZoneDisposition::InService
            }
            DbBpZoneDisposition::Quiesced => BlueprintZoneDisposition::Quiesced,
            DbBpZoneDisposition::Expunged => BlueprintZoneDisposition::Expunged,
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_omicron_zone_nic)]
pub struct BpOmicronZoneNic {
    blueprint_id: Uuid,
    pub id: Uuid,
    name: Name,
    ip: IpNetwork,
    mac: MacAddr,
    subnet: IpNetwork,
    vni: SqlU32,
    is_primary: bool,
    slot: SqlU8,
}

impl BpOmicronZoneNic {
    pub fn new(
        blueprint_id: Uuid,
        zone: &BlueprintZoneConfig,
    ) -> Result<Option<BpOmicronZoneNic>, anyhow::Error> {
        let Some((_, nic)) = zone.zone_type.external_networking() else {
            return Ok(None);
        };
        let nic = OmicronZoneNic::new(zone.id.into_untyped_uuid(), nic)?;
        Ok(Some(Self {
            blueprint_id,
            id: nic.id,
            name: nic.name,
            ip: nic.ip,
            mac: nic.mac,
            subnet: nic.subnet,
            vni: nic.vni,
            is_primary: nic.is_primary,
            slot: nic.slot,
        }))
    }

    pub fn into_network_interface_for_zone(
        self,
        zone_id: Uuid,
    ) -> Result<NetworkInterface, anyhow::Error> {
        let zone_nic = OmicronZoneNic::from(self);
        zone_nic.into_network_interface_for_zone(zone_id)
    }
}

impl From<BpOmicronZoneNic> for OmicronZoneNic {
    fn from(value: BpOmicronZoneNic) -> Self {
        OmicronZoneNic {
            id: value.id,
            name: value.name,
            ip: value.ip,
            mac: value.mac,
            subnet: value.subnet,
            vni: value.vni,
            is_primary: value.is_primary,
            slot: value.slot,
        }
    }
}

mod diesel_util {
    use crate::{
        schema::bp_omicron_zone::disposition, to_db_bp_zone_disposition,
        DbBpZoneDisposition,
    };
    use diesel::{
        helper_types::EqAny, prelude::*, query_dsl::methods::FilterDsl,
    };
    use nexus_types::deployment::{
        BlueprintZoneDisposition, BlueprintZoneFilter,
    };

    /// An extension trait to apply a [`BlueprintZoneFilter`] to a Diesel
    /// expression.
    ///
    /// This is applicable to any Diesel expression which includes the
    /// `bp_omicron_zone` table.
    ///
    /// This needs to live here, rather than in `nexus-db-queries`, because it
    /// names the `DbBpZoneDisposition` type which is private to this crate.
    pub trait ApplyBlueprintZoneFilterExt {
        type Output;

        /// Applies a [`BlueprintZoneFilter`] to a Diesel expression.
        fn blueprint_zone_filter(
            self,
            filter: BlueprintZoneFilter,
        ) -> Self::Output;
    }

    impl<E> ApplyBlueprintZoneFilterExt for E
    where
        E: FilterDsl<BlueprintZoneFilterQuery>,
    {
        type Output = E::Output;

        fn blueprint_zone_filter(
            self,
            filter: BlueprintZoneFilter,
        ) -> Self::Output {
            // This is only boxed for ease of reference above.
            let all_matching_dispositions: BoxedIterator<DbBpZoneDisposition> =
                Box::new(
                    BlueprintZoneDisposition::all_matching(filter)
                        .map(to_db_bp_zone_disposition),
                );

            FilterDsl::filter(
                self,
                disposition.eq_any(all_matching_dispositions),
            )
        }
    }

    type BoxedIterator<T> = Box<dyn Iterator<Item = T>>;
    type BlueprintZoneFilterQuery =
        EqAny<disposition, BoxedIterator<DbBpZoneDisposition>>;
}

pub use diesel_util::ApplyBlueprintZoneFilterExt;
