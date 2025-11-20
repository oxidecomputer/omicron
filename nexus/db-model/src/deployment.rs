// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for representing the deployed software and configuration in the
//! database

use crate::inventory::{HwRotSlot, SpMgsSlot, SpType, ZoneType};
use crate::omicron_zone_config::{self, OmicronZoneNic};
use crate::typed_uuid::DbTypedUuid;
use crate::{
    ArtifactHash, ByteCount, DbArtifactVersion, DbOximeterReadMode, Generation,
    HwM2Slot, MacAddr, Name, SledState, SqlU8, SqlU16, SqlU32, TufArtifact,
    impl_enum_type, ipv6,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clickhouse_admin_types::{KeeperId, ServerId};
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::{
    blueprint, bp_clickhouse_cluster_config,
    bp_clickhouse_keeper_zone_id_to_node_id,
    bp_clickhouse_server_zone_id_to_node_id, bp_omicron_dataset,
    bp_omicron_physical_disk, bp_omicron_zone, bp_omicron_zone_nic,
    bp_oximeter_read_policy, bp_pending_mgs_update_host_phase_1,
    bp_pending_mgs_update_rot, bp_pending_mgs_update_rot_bootloader,
    bp_pending_mgs_update_sp, bp_sled_metadata, bp_target,
    debug_log_blueprint_planning,
};
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::ClickhouseClusterConfig;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use nexus_types::deployment::ExpectedActiveRotSlot;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::deployment::PendingMgsUpdateRotBootloaderDetails;
use nexus_types::deployment::PendingMgsUpdateRotDetails;
use nexus_types::deployment::PendingMgsUpdateSpDetails;
use nexus_types::deployment::{
    BlueprintArtifactVersion, BlueprintDatasetConfig, OximeterReadMode,
};
use nexus_types::deployment::{BlueprintDatasetDisposition, ExpectedVersion};
use nexus_types::deployment::{
    BlueprintHostPhase2DesiredContents, PendingMgsUpdateHostPhase1Details,
};
use nexus_types::deployment::{
    BlueprintHostPhase2DesiredSlots, PlanningReport,
};
use nexus_types::deployment::{BlueprintPhysicalDiskConfig, BlueprintSource};
use nexus_types::deployment::{BlueprintZoneImageSource, blueprint_zone_type};
use nexus_types::deployment::{
    OmicronZoneExternalFloatingAddr, OmicronZoneExternalFloatingIp,
    OmicronZoneExternalSnatIp,
};
use nexus_types::inventory::BaseboardId;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::disk::DiskIdentity;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::{
    BlueprintKind, BlueprintUuid, DatasetKind, ExternalIpKind, ExternalIpUuid,
    GenericUuid, MupdateOverrideKind, OmicronZoneKind, OmicronZoneUuid,
    PhysicalDiskKind, SledKind, SledUuid, ZpoolKind, ZpoolUuid,
};
use std::net::{IpAddr, SocketAddrV6};
use std::sync::Arc;
use uuid::Uuid;

/// See [`nexus_types::deployment::Blueprint`].
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = blueprint)]
pub struct Blueprint {
    pub id: DbTypedUuid<BlueprintKind>,
    pub parent_blueprint_id: Option<DbTypedUuid<BlueprintKind>>,
    pub internal_dns_version: Generation,
    pub external_dns_version: Generation,
    pub cockroachdb_fingerprint: String,
    pub cockroachdb_setting_preserve_downgrade: Option<String>,
    pub time_created: DateTime<Utc>,
    pub creator: String,
    pub comment: String,
    pub target_release_minimum_generation: Generation,
    pub nexus_generation: Generation,
    pub source: DbBpSource,
}

impl From<&'_ nexus_types::deployment::Blueprint> for Blueprint {
    fn from(bp: &'_ nexus_types::deployment::Blueprint) -> Self {
        Self {
            id: bp.id.into(),
            parent_blueprint_id: bp.parent_blueprint_id.map(From::from),
            internal_dns_version: Generation(bp.internal_dns_version),
            external_dns_version: Generation(bp.external_dns_version),
            cockroachdb_fingerprint: bp.cockroachdb_fingerprint.clone(),
            cockroachdb_setting_preserve_downgrade: bp
                .cockroachdb_setting_preserve_downgrade
                .to_optional_string(),
            time_created: bp.time_created,
            creator: bp.creator.clone(),
            comment: bp.comment.clone(),
            target_release_minimum_generation: Generation(
                bp.target_release_minimum_generation,
            ),
            nexus_generation: Generation(bp.nexus_generation),
            source: DbBpSource::from(&bp.source),
        }
    }
}

impl From<Blueprint> for nexus_types::deployment::BlueprintMetadata {
    fn from(value: Blueprint) -> Self {
        Self {
            id: value.id.into(),
            parent_blueprint_id: value.parent_blueprint_id.map(From::from),
            internal_dns_version: *value.internal_dns_version,
            external_dns_version: *value.external_dns_version,
            target_release_minimum_generation: *value
                .target_release_minimum_generation,
            nexus_generation: *value.nexus_generation,
            cockroachdb_fingerprint: value.cockroachdb_fingerprint,
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::from_optional_string(
                    &value.cockroachdb_setting_preserve_downgrade,
                )
                .ok(),
            time_created: value.time_created,
            creator: value.creator,
            comment: value.comment,
            source: value.source.into(),
        }
    }
}

impl_enum_type!(
    BpSourceEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum DbBpSource;

    // Enum values
    Rss => b"rss"
    Planner => b"planner"
    ReconfiguratorCliEdit => b"reconfigurator_cli_edit"
    Test => b"test"
);

impl From<&'_ BlueprintSource> for DbBpSource {
    fn from(value: &'_ BlueprintSource) -> Self {
        match value {
            BlueprintSource::Rss => Self::Rss,
            // We don't store planning reports, so both of these variants squish
            // to `Planner`.
            BlueprintSource::Planner(_)
            | BlueprintSource::PlannerLoadedFromDatabase => Self::Planner,
            BlueprintSource::ReconfiguratorCliEdit => {
                Self::ReconfiguratorCliEdit
            }
            BlueprintSource::Test => Self::Test,
        }
    }
}

impl From<DbBpSource> for BlueprintSource {
    fn from(value: DbBpSource) -> Self {
        match value {
            DbBpSource::Rss => Self::Rss,
            DbBpSource::Planner => Self::PlannerLoadedFromDatabase,
            DbBpSource::ReconfiguratorCliEdit => Self::ReconfiguratorCliEdit,
            DbBpSource::Test => Self::Test,
        }
    }
}

/// See [`nexus_types::deployment::BlueprintTarget`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_target)]
pub struct BpTarget {
    pub version: SqlU32,
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub enabled: bool,
    pub time_made_target: DateTime<Utc>,
}

impl BpTarget {
    pub fn new(version: u32, target: BlueprintTarget) -> Self {
        Self {
            version: version.into(),
            blueprint_id: target.target_id.into(),
            enabled: target.enabled,
            time_made_target: target.time_made_target,
        }
    }
}

impl From<BpTarget> for nexus_types::deployment::BlueprintTarget {
    fn from(value: BpTarget) -> Self {
        Self {
            target_id: value.blueprint_id.into(),
            enabled: value.enabled,
            time_made_target: value.time_made_target,
        }
    }
}

/// See [`nexus_types::deployment::BlueprintSledConfig::state`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_sled_metadata)]
pub struct BpSledMetadata {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub sled_state: SledState,
    pub sled_agent_generation: Generation,
    pub remove_mupdate_override: Option<DbTypedUuid<MupdateOverrideKind>>,
    pub host_phase_2_desired_slot_a: Option<ArtifactHash>,
    pub host_phase_2_desired_slot_b: Option<ArtifactHash>,
    /// Public only for easy of writing queries; consumers should prefer the
    /// `subnet()` method.
    pub subnet: IpNetwork,
}

impl BpSledMetadata {
    pub fn subnet(&self) -> anyhow::Result<Ipv6Subnet<SLED_PREFIX>> {
        let subnet = match self.subnet {
            IpNetwork::V4(subnet) => bail!(
                "invalid subnet for sled {}: {subnet} (should be Ipv6)",
                self.sled_id
            ),
            IpNetwork::V6(subnet) => subnet,
        };

        Ok(subnet.into())
    }

    pub fn host_phase_2(
        &self,
        slot_a_artifact_version: Option<DbArtifactVersion>,
        slot_b_artifact_version: Option<DbArtifactVersion>,
    ) -> BlueprintHostPhase2DesiredSlots {
        // If we found an artifact version, use it; otherwise, use `Unknown`.
        fn interpret_version(
            maybe_version: Option<DbArtifactVersion>,
        ) -> BlueprintArtifactVersion {
            match maybe_version {
                Some(version) => {
                    BlueprintArtifactVersion::Available { version: version.0 }
                }
                None => BlueprintArtifactVersion::Unknown,
            }
        }

        // If we have an artifact hash, use it (and attach a version).
        // Otherwise, use `CurrentContents`.
        fn interpret_slot(
            maybe_hash: Option<ArtifactHash>,
            maybe_version: Option<DbArtifactVersion>,
        ) -> BlueprintHostPhase2DesiredContents {
            match maybe_hash {
                Some(hash) => BlueprintHostPhase2DesiredContents::Artifact {
                    version: interpret_version(maybe_version),
                    hash: hash.0,
                },
                None => BlueprintHostPhase2DesiredContents::CurrentContents,
            }
        }

        BlueprintHostPhase2DesiredSlots {
            slot_a: interpret_slot(
                self.host_phase_2_desired_slot_a,
                slot_a_artifact_version,
            ),
            slot_b: interpret_slot(
                self.host_phase_2_desired_slot_b,
                slot_b_artifact_version,
            ),
        }
    }
}

impl_enum_type!(
    BpPhysicalDiskDispositionEnum:

    /// This type is not actually public, because [`BlueprintPhysicalDiskDisposition`]
    /// interacts with external logic.
    ///
    /// However, it must be marked `pub` to avoid errors like `crate-private
    /// type `BpPhysicalDiskDispositionEnum` in public interface`. Marking this type `pub`,
    /// without actually making it public, tricks rustc in a desirable way.
    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum DbBpPhysicalDiskDisposition;

    // Enum values
    InService => b"in_service"
    Expunged => b"expunged"
);

struct DbBpPhysicalDiskDispositionColumns {
    disposition: DbBpPhysicalDiskDisposition,
    expunged_as_of_generation: Option<Generation>,
    expunged_ready_for_cleanup: bool,
}

impl From<BlueprintPhysicalDiskDisposition>
    for DbBpPhysicalDiskDispositionColumns
{
    fn from(value: BlueprintPhysicalDiskDisposition) -> Self {
        let (
            disposition,
            disposition_expunged_as_of_generation,
            disposition_expunged_ready_for_cleanup,
        ) = match value {
            BlueprintPhysicalDiskDisposition::InService => {
                (DbBpPhysicalDiskDisposition::InService, None, false)
            }
            BlueprintPhysicalDiskDisposition::Expunged {
                as_of_generation,
                ready_for_cleanup,
            } => (
                DbBpPhysicalDiskDisposition::Expunged,
                Some(Generation(as_of_generation)),
                ready_for_cleanup,
            ),
        };
        Self {
            disposition,
            expunged_as_of_generation: disposition_expunged_as_of_generation,
            expunged_ready_for_cleanup: disposition_expunged_ready_for_cleanup,
        }
    }
}

impl TryFrom<DbBpPhysicalDiskDispositionColumns>
    for BlueprintPhysicalDiskDisposition
{
    type Error = anyhow::Error;

    fn try_from(
        value: DbBpPhysicalDiskDispositionColumns,
    ) -> Result<Self, Self::Error> {
        match (value.disposition, value.expunged_as_of_generation) {
            (DbBpPhysicalDiskDisposition::InService, None) => {
                Ok(Self::InService)
            }
            (DbBpPhysicalDiskDisposition::Expunged, Some(as_of_generation)) => {
                Ok(Self::Expunged {
                    as_of_generation: *as_of_generation,
                    ready_for_cleanup: value.expunged_ready_for_cleanup,
                })
            }
            (DbBpPhysicalDiskDisposition::InService, Some(_))
            | (DbBpPhysicalDiskDisposition::Expunged, None) => Err(anyhow!(
                "illegal database state (CHECK constraint broken?!): \
                 disposition {:?}, disposition_expunged_as_of_generation {:?}",
                value.disposition,
                value.expunged_as_of_generation,
            )),
        }
    }
}

/// See [`nexus_types::deployment::BlueprintPhysicalDiskConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_omicron_physical_disk)]
pub struct BpOmicronPhysicalDisk {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub sled_id: DbTypedUuid<SledKind>,

    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub id: DbTypedUuid<PhysicalDiskKind>,
    pub pool_id: Uuid,

    disposition: DbBpPhysicalDiskDisposition,
    disposition_expunged_as_of_generation: Option<Generation>,
    disposition_expunged_ready_for_cleanup: bool,
}

impl BpOmicronPhysicalDisk {
    pub fn new(
        blueprint_id: BlueprintUuid,
        sled_id: SledUuid,
        disk_config: &BlueprintPhysicalDiskConfig,
    ) -> Self {
        let DbBpPhysicalDiskDispositionColumns {
            disposition,
            expunged_as_of_generation: disposition_expunged_as_of_generation,
            expunged_ready_for_cleanup: disposition_expunged_ready_for_cleanup,
        } = disk_config.disposition.into();
        Self {
            blueprint_id: blueprint_id.into(),
            sled_id: sled_id.into(),
            vendor: disk_config.identity.vendor.clone(),
            serial: disk_config.identity.serial.clone(),
            model: disk_config.identity.model.clone(),
            id: disk_config.id.into(),
            pool_id: disk_config.pool_id.into_untyped_uuid(),
            disposition,
            disposition_expunged_as_of_generation,
            disposition_expunged_ready_for_cleanup,
        }
    }
}

impl TryFrom<BpOmicronPhysicalDisk> for BlueprintPhysicalDiskConfig {
    type Error = anyhow::Error;

    fn try_from(disk: BpOmicronPhysicalDisk) -> Result<Self, Self::Error> {
        let disposition_cols = DbBpPhysicalDiskDispositionColumns {
            disposition: disk.disposition,
            expunged_as_of_generation: disk
                .disposition_expunged_as_of_generation,
            expunged_ready_for_cleanup: disk
                .disposition_expunged_ready_for_cleanup,
        };

        Ok(Self {
            disposition: disposition_cols.try_into()?,
            identity: DiskIdentity {
                vendor: disk.vendor,
                serial: disk.serial,
                model: disk.model,
            },
            id: disk.id.into(),
            pool_id: ZpoolUuid::from_untyped_uuid(disk.pool_id),
        })
    }
}

impl_enum_type!(
    BpDatasetDispositionEnum:

    /// This type is not actually public, because [`BlueprintDatasetDisposition`]
    /// interacts with external logic.
    ///
    /// However, it must be marked `pub` to avoid errors like `crate-private
    /// type `BpDatasetDispositionEnum` in public interface`. Marking this type `pub`,
    /// without actually making it public, tricks rustc in a desirable way.
    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum DbBpDatasetDisposition;

    // Enum values
    InService => b"in_service"
    Expunged => b"expunged"
);

/// Converts a [`BlueprintDatasetDisposition`] to a version that can be inserted
/// into a database.
pub fn to_db_bp_dataset_disposition(
    disposition: BlueprintDatasetDisposition,
) -> DbBpDatasetDisposition {
    match disposition {
        BlueprintDatasetDisposition::InService => {
            DbBpDatasetDisposition::InService
        }
        BlueprintDatasetDisposition::Expunged => {
            DbBpDatasetDisposition::Expunged
        }
    }
}

impl From<DbBpDatasetDisposition> for BlueprintDatasetDisposition {
    fn from(disposition: DbBpDatasetDisposition) -> Self {
        match disposition {
            DbBpDatasetDisposition::InService => {
                BlueprintDatasetDisposition::InService
            }
            DbBpDatasetDisposition::Expunged => {
                BlueprintDatasetDisposition::Expunged
            }
        }
    }
}

/// DB representation of [BlueprintDatasetConfig]
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_omicron_dataset)]
pub struct BpOmicronDataset {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub id: DbTypedUuid<DatasetKind>,

    pub disposition: DbBpDatasetDisposition,

    pub pool_id: DbTypedUuid<ZpoolKind>,
    pub kind: crate::DatasetKind,
    zone_name: Option<String>,
    pub ip: Option<ipv6::Ipv6Addr>,
    pub port: Option<SqlU16>,

    pub quota: Option<ByteCount>,
    pub reservation: Option<ByteCount>,
    pub compression: String,
}

impl BpOmicronDataset {
    pub fn new(
        blueprint_id: BlueprintUuid,
        sled_id: SledUuid,
        dataset_config: &BlueprintDatasetConfig,
    ) -> Self {
        Self {
            blueprint_id: blueprint_id.into(),
            sled_id: sled_id.into(),
            id: dataset_config.id.into(),
            disposition: to_db_bp_dataset_disposition(
                dataset_config.disposition,
            ),
            pool_id: dataset_config.pool.id().into(),
            kind: (&dataset_config.kind).into(),
            zone_name: dataset_config.kind.zone_name().map(String::from),
            ip: dataset_config.address.map(|addr| addr.ip().into()),
            port: dataset_config.address.map(|addr| addr.port().into()),
            quota: dataset_config.quota.map(|q| q.into()),
            reservation: dataset_config.reservation.map(|r| r.into()),
            compression: dataset_config.compression.to_string(),
        }
    }
}

impl TryFrom<BpOmicronDataset> for BlueprintDatasetConfig {
    type Error = anyhow::Error;

    fn try_from(dataset: BpOmicronDataset) -> Result<Self, Self::Error> {
        let address = match (dataset.ip, dataset.port) {
            (Some(ip), Some(port)) => {
                Some(SocketAddrV6::new(ip.into(), port.into(), 0, 0))
            }
            (None, None) => None,
            (_, _) => anyhow::bail!(
                "Either both 'ip' and 'port' should be set, or neither"
            ),
        };

        Ok(Self {
            disposition: dataset.disposition.into(),
            id: dataset.id.into(),
            pool: omicron_common::zpool_name::ZpoolName::new_external(
                dataset.pool_id.into(),
            ),
            kind: crate::DatasetKind::try_into_api(
                dataset.kind,
                dataset.zone_name,
            )?,
            address,
            quota: dataset.quota.map(|b| b.into()),
            reservation: dataset.reservation.map(|b| b.into()),
            compression: dataset.compression.parse()?,
        })
    }
}

/// See [`nexus_types::deployment::BlueprintZoneConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_omicron_zone)]
pub struct BpOmicronZone {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub id: DbTypedUuid<OmicronZoneKind>,
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
    disposition_expunged_as_of_generation: Option<Generation>,
    disposition_expunged_ready_for_cleanup: bool,

    pub external_ip_id: Option<DbTypedUuid<ExternalIpKind>>,
    pub filesystem_pool: DbTypedUuid<ZpoolKind>,

    pub image_source: DbBpZoneImageSource,
    pub image_artifact_sha256: Option<ArtifactHash>,
    pub nexus_generation: Option<Generation>,
    pub nexus_lockstep_port: Option<SqlU16>,
}

impl BpOmicronZone {
    pub fn new(
        blueprint_id: BlueprintUuid,
        sled_id: SledUuid,
        blueprint_zone: &BlueprintZoneConfig,
    ) -> anyhow::Result<Self> {
        let external_ip_id = blueprint_zone
            .zone_type
            .external_networking()
            .map(|(ip, _)| ip.id().into());

        let DbBpZoneDispositionColumns {
            disposition,
            expunged_as_of_generation: disposition_expunged_as_of_generation,
            expunged_ready_for_cleanup: disposition_expunged_ready_for_cleanup,
        } = blueprint_zone.disposition.into();

        let DbBpZoneImageSourceColumns { image_source, image_artifact_data } =
            blueprint_zone.image_source.clone().into();

        // Create a dummy record to start, then fill in the rest
        let mut bp_omicron_zone = BpOmicronZone {
            // Fill in the known fields that don't require inspecting
            // `blueprint_zone.zone_type`
            blueprint_id: blueprint_id.into(),
            sled_id: sled_id.into(),
            id: blueprint_zone.id.into(),
            external_ip_id,
            filesystem_pool: blueprint_zone.filesystem_pool.id().into(),
            disposition,
            disposition_expunged_as_of_generation,
            disposition_expunged_ready_for_cleanup,
            zone_type: blueprint_zone.zone_type.kind().into(),
            image_source,
            // The version is not preserved here -- instead, it is looked up
            // from the tuf_artifact table.
            image_artifact_sha256: image_artifact_data
                .map(|(_version, hash)| hash),

            // Set the remainder of the fields to a default
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
            nexus_generation: None,
            nexus_lockstep_port: None,
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
            }
            BlueprintZoneType::ClickhouseKeeper(
                blueprint_zone_type::ClickhouseKeeper { address, dataset },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.set_zpool_name(dataset);
            }
            BlueprintZoneType::ClickhouseServer(
                blueprint_zone_type::ClickhouseServer { address, dataset },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.set_zpool_name(dataset);
            }
            BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb { address, dataset },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.set_zpool_name(dataset);
            }
            BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                address,
                dataset,
            }) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
                bp_omicron_zone.set_zpool_name(dataset);
            }
            BlueprintZoneType::CruciblePantry(
                blueprint_zone_type::CruciblePantry { address },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
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
                blueprint_zone_type::InternalNtp { address },
            ) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
            }
            BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                internal_address,
                lockstep_port,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
                nexus_generation,
            }) => {
                // Set the common fields
                bp_omicron_zone
                    .set_primary_service_ip_and_port(internal_address);

                // Set the zone specific fields
                bp_omicron_zone.bp_nic_id = Some(nic.id);
                bp_omicron_zone.second_service_ip =
                    Some(IpNetwork::from(external_ip.ip));
                bp_omicron_zone.nexus_lockstep_port =
                    Some(SqlU16::from(*lockstep_port));
                bp_omicron_zone.nexus_external_tls = Some(*external_tls);
                bp_omicron_zone.nexus_external_dns_servers = Some(
                    external_dns_servers
                        .iter()
                        .cloned()
                        .map(IpNetwork::from)
                        .collect(),
                );
                bp_omicron_zone.nexus_generation =
                    Some(Generation::from(*nexus_generation));
            }
            BlueprintZoneType::Oximeter(blueprint_zone_type::Oximeter {
                address,
            }) => {
                // Set the common fields
                bp_omicron_zone.set_primary_service_ip_and_port(address);
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
        image_artifact_row: Option<TufArtifact>,
    ) -> anyhow::Result<BlueprintZoneConfig> {
        // Build up a set of common fields for our `BlueprintZoneType`s
        //
        // Some of these are results that we only evaluate when used, because
        // not all zone types use all common fields.
        let primary_address = SocketAddrV6::new(
            self.primary_service_ip.into(),
            *self.primary_service_port,
            0,
            0,
        );
        let dataset =
            omicron_zone_config::dataset_zpool_name_to_omicron_zone_dataset(
                self.dataset_zpool_name,
            );

        // There is a nested result here. If there is a caller error (the outer
        // Result) we immediately return. We check the inner result later, but
        // only if some code path tries to use `nic` and it's not present.
        let nic = omicron_zone_config::nic_row_to_network_interface(
            self.id.into(),
            self.bp_nic_id,
            nic_row.map(Into::into),
        )?;

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
                blueprint_zone_type::InternalNtp { address: primary_address },
            ),
            ZoneType::Nexus => {
                BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    internal_address: primary_address,
                    lockstep_port: *self.nexus_lockstep_port.ok_or_else(
                        || anyhow!("expected 'nexus_lockstep_port'"),
                    )?,
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
                    nexus_generation: *self.nexus_generation.ok_or_else(
                        || anyhow!("expected 'nexus_generation'"),
                    )?,
                })
            }
            ZoneType::Oximeter => {
                BlueprintZoneType::Oximeter(blueprint_zone_type::Oximeter {
                    address: primary_address,
                })
            }
        };

        let disposition_cols = DbBpZoneDispositionColumns {
            disposition: self.disposition,
            expunged_as_of_generation: self
                .disposition_expunged_as_of_generation,
            expunged_ready_for_cleanup: self
                .disposition_expunged_ready_for_cleanup,
        };

        let image_source_cols = DbBpZoneImageSourceColumns::new(
            self.image_source,
            self.image_artifact_sha256,
            image_artifact_row,
        );

        Ok(BlueprintZoneConfig {
            disposition: disposition_cols.try_into()?,
            id: self.id.into(),
            filesystem_pool: ZpoolName::new_external(
                self.filesystem_pool.into(),
            ),
            zone_type,
            image_source: image_source_cols.try_into()?,
        })
    }
}

impl_enum_type!(
    BpZoneDispositionEnum:

    /// This type is not actually public, because [`BlueprintZoneDisposition`]
    /// interacts with external logic.
    ///
    /// However, it must be marked `pub` to avoid errors like `crate-private
    /// type `BpZoneDispositionEnum` in public interface`. Marking this type `pub`,
    /// without actually making it public, tricks rustc in a desirable way.
    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum DbBpZoneDisposition;

    // Enum values
    InService => b"in_service"
    Expunged => b"expunged"
);

struct DbBpZoneDispositionColumns {
    disposition: DbBpZoneDisposition,
    expunged_as_of_generation: Option<Generation>,
    expunged_ready_for_cleanup: bool,
}

impl From<BlueprintZoneDisposition> for DbBpZoneDispositionColumns {
    fn from(value: BlueprintZoneDisposition) -> Self {
        let (
            disposition,
            disposition_expunged_as_of_generation,
            disposition_expunged_ready_for_cleanup,
        ) = match value {
            BlueprintZoneDisposition::InService => {
                (DbBpZoneDisposition::InService, None, false)
            }
            BlueprintZoneDisposition::Expunged {
                as_of_generation,
                ready_for_cleanup,
            } => (
                DbBpZoneDisposition::Expunged,
                Some(Generation(as_of_generation)),
                ready_for_cleanup,
            ),
        };
        Self {
            disposition,
            expunged_as_of_generation: disposition_expunged_as_of_generation,
            expunged_ready_for_cleanup: disposition_expunged_ready_for_cleanup,
        }
    }
}

impl TryFrom<DbBpZoneDispositionColumns> for BlueprintZoneDisposition {
    type Error = anyhow::Error;

    fn try_from(
        value: DbBpZoneDispositionColumns,
    ) -> Result<Self, Self::Error> {
        match (value.disposition, value.expunged_as_of_generation) {
            (DbBpZoneDisposition::InService, None) => Ok(Self::InService),
            (DbBpZoneDisposition::Expunged, Some(as_of_generation)) => {
                Ok(Self::Expunged {
                    as_of_generation: *as_of_generation,
                    ready_for_cleanup: value.expunged_ready_for_cleanup,
                })
            }
            (DbBpZoneDisposition::InService, Some(_))
            | (DbBpZoneDisposition::Expunged, None) => Err(anyhow!(
                "illegal database state (CHECK constraint broken?!): \
                 disposition {:?}, disposition_expunged_as_of_generation {:?}",
                value.disposition,
                value.expunged_as_of_generation,
            )),
        }
    }
}

impl_enum_type!(
    BpZoneImageSourceEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum DbBpZoneImageSource;

    // Enum values
    InstallDataset => b"install_dataset"
    Artifact => b"artifact"
);

struct DbBpZoneImageSourceColumns {
    image_source: DbBpZoneImageSource,
    // image_artifact_data is Some if and only if image_source is Artifact.
    //
    // The BlueprintZoneImageVersion is not actually stored in bp_omicron_zone
    // table directly, but is instead looked up from the tuf_artifact table at
    // blueprint load time.
    image_artifact_data: Option<(BlueprintArtifactVersion, ArtifactHash)>,
}

impl DbBpZoneImageSourceColumns {
    fn new(
        image_source: DbBpZoneImageSource,
        image_artifact_sha256: Option<ArtifactHash>,
        image_artifact_row: Option<TufArtifact>,
    ) -> Self {
        // Note that artifact_row can only be Some if image_artifact_sha256 is
        // Some.
        let image_artifact_data = image_artifact_sha256.map(|hash| {
            let version = match image_artifact_row {
                Some(artifact_row) => BlueprintArtifactVersion::Available {
                    version: artifact_row.version.0,
                },
                None => BlueprintArtifactVersion::Unknown,
            };
            (version, hash)
        });
        Self { image_source, image_artifact_data }
    }
}

impl From<BlueprintZoneImageSource> for DbBpZoneImageSourceColumns {
    fn from(image_source: BlueprintZoneImageSource) -> Self {
        match image_source {
            BlueprintZoneImageSource::InstallDataset => Self {
                image_source: DbBpZoneImageSource::InstallDataset,
                image_artifact_data: None,
            },
            BlueprintZoneImageSource::Artifact { version, hash } => Self {
                image_source: DbBpZoneImageSource::Artifact,
                image_artifact_data: Some((version, hash.into())),
            },
        }
    }
}

impl TryFrom<DbBpZoneImageSourceColumns> for BlueprintZoneImageSource {
    type Error = anyhow::Error;

    fn try_from(
        value: DbBpZoneImageSourceColumns,
    ) -> Result<Self, Self::Error> {
        match (value.image_source, value.image_artifact_data) {
            (DbBpZoneImageSource::Artifact, Some((version, hash))) => {
                Ok(Self::Artifact { version, hash: hash.into() })
            }
            (DbBpZoneImageSource::Artifact, None) => Err(anyhow!(
                "illegal database state (CHECK constraint broken?!): \
                 image_source {:?}, image_artifact_data None",
                value.image_source,
            )),
            (DbBpZoneImageSource::InstallDataset, data @ Some(_)) => {
                Err(anyhow!(
                    "illegal database state (CHECK constraint broken?!): \
                 image_source {:?}, image_artifact_data {:?}",
                    value.image_source,
                    data,
                ))
            }
            (DbBpZoneImageSource::InstallDataset, None) => {
                Ok(Self::InstallDataset)
            }
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_omicron_zone_nic)]
pub struct BpOmicronZoneNic {
    blueprint_id: DbTypedUuid<BlueprintKind>,
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
        blueprint_id: BlueprintUuid,
        zone: &BlueprintZoneConfig,
    ) -> Result<Option<BpOmicronZoneNic>, anyhow::Error> {
        let Some((_, nic)) = zone.zone_type.external_networking() else {
            return Ok(None);
        };
        let nic = OmicronZoneNic::new(zone.id, nic)?;
        Ok(Some(Self {
            blueprint_id: blueprint_id.into(),
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
        zone_id: OmicronZoneUuid,
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

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_clickhouse_cluster_config)]
pub struct BpClickhouseClusterConfig {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub generation: Generation,
    pub max_used_server_id: i64,
    pub max_used_keeper_id: i64,
    pub cluster_name: String,
    pub cluster_secret: String,
    pub highest_seen_keeper_leader_committed_log_index: i64,
}

impl BpClickhouseClusterConfig {
    pub fn new(
        blueprint_id: BlueprintUuid,
        config: &ClickhouseClusterConfig,
    ) -> anyhow::Result<BpClickhouseClusterConfig> {
        Ok(BpClickhouseClusterConfig {
            blueprint_id: blueprint_id.into(),
            generation: Generation(config.generation),
            max_used_server_id: config
                .max_used_server_id
                .0
                .try_into()
                .context("more than 2^63 clickhouse server IDs in use")?,
            max_used_keeper_id: config
                .max_used_keeper_id
                .0
                .try_into()
                .context("more than 2^63 clickhouse keeper IDs in use")?,
            cluster_name: config.cluster_name.clone(),
            cluster_secret: config.cluster_secret.clone(),
            highest_seen_keeper_leader_committed_log_index: config
                .highest_seen_keeper_leader_committed_log_index
                .try_into()
                .context(
                    "more than 2^63 clickhouse keeper log indexes in use",
                )?,
        })
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_clickhouse_keeper_zone_id_to_node_id)]
pub struct BpClickhouseKeeperZoneIdToNodeId {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub omicron_zone_id: DbTypedUuid<OmicronZoneKind>,
    pub keeper_id: i64,
}

impl BpClickhouseKeeperZoneIdToNodeId {
    pub fn new(
        blueprint_id: BlueprintUuid,
        omicron_zone_id: OmicronZoneUuid,
        keeper_id: KeeperId,
    ) -> anyhow::Result<BpClickhouseKeeperZoneIdToNodeId> {
        Ok(BpClickhouseKeeperZoneIdToNodeId {
            blueprint_id: blueprint_id.into(),
            omicron_zone_id: omicron_zone_id.into(),
            keeper_id: keeper_id
                .0
                .try_into()
                .context("more than 2^63 IDs in use")?,
        })
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_clickhouse_server_zone_id_to_node_id)]
pub struct BpClickhouseServerZoneIdToNodeId {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub omicron_zone_id: DbTypedUuid<OmicronZoneKind>,
    pub server_id: i64,
}

impl BpClickhouseServerZoneIdToNodeId {
    pub fn new(
        blueprint_id: BlueprintUuid,
        omicron_zone_id: OmicronZoneUuid,
        server_id: ServerId,
    ) -> anyhow::Result<BpClickhouseServerZoneIdToNodeId> {
        Ok(BpClickhouseServerZoneIdToNodeId {
            blueprint_id: blueprint_id.into(),
            omicron_zone_id: omicron_zone_id.into(),
            server_id: server_id
                .0
                .try_into()
                .context("more than 2^63 IDs in use")?,
        })
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_oximeter_read_policy)]
pub struct BpOximeterReadPolicy {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub version: Generation,
    pub oximeter_read_mode: DbOximeterReadMode,
}

impl BpOximeterReadPolicy {
    pub fn new(
        blueprint_id: BlueprintUuid,
        version: Generation,
        read_mode: &OximeterReadMode,
    ) -> BpOximeterReadPolicy {
        BpOximeterReadPolicy {
            blueprint_id: blueprint_id.into(),
            version,
            oximeter_read_mode: DbOximeterReadMode::from(read_mode),
        }
    }
}

pub trait BpPendingMgsUpdateComponent {
    /// Converts a BpMgsUpdate into a PendingMgsUpdate
    fn into_generic(self, baseboard_id: Arc<BaseboardId>) -> PendingMgsUpdate;

    /// Retrieves the baseboard ID
    fn hw_baseboard_id(&self) -> &Uuid;
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_pending_mgs_update_rot_bootloader)]
pub struct BpPendingMgsUpdateRotBootloader {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub hw_baseboard_id: Uuid,
    pub sp_type: SpType,
    pub sp_slot: SpMgsSlot,
    pub artifact_sha256: ArtifactHash,
    pub artifact_version: DbArtifactVersion,
    pub expected_stage0_version: DbArtifactVersion,
    pub expected_stage0_next_version: Option<DbArtifactVersion>,
}

impl BpPendingMgsUpdateComponent for BpPendingMgsUpdateRotBootloader {
    fn hw_baseboard_id(&self) -> &Uuid {
        &self.hw_baseboard_id
    }

    fn into_generic(self, baseboard_id: Arc<BaseboardId>) -> PendingMgsUpdate {
        PendingMgsUpdate {
            baseboard_id,
            sp_type: self.sp_type.into(),
            slot_id: **self.sp_slot,
            artifact_hash: self.artifact_sha256.into(),
            artifact_version: (*self.artifact_version).clone(),
            details: PendingMgsUpdateDetails::RotBootloader(
                PendingMgsUpdateRotBootloaderDetails {
                    expected_stage0_version: (*self.expected_stage0_version)
                        .clone(),
                    expected_stage0_next_version: match self
                        .expected_stage0_next_version
                    {
                        Some(v) => ExpectedVersion::Version((*v).clone()),
                        None => ExpectedVersion::NoValidVersion,
                    },
                },
            ),
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_pending_mgs_update_sp)]
pub struct BpPendingMgsUpdateSp {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub hw_baseboard_id: Uuid,
    pub sp_type: SpType,
    pub sp_slot: SpMgsSlot,
    pub artifact_sha256: ArtifactHash,
    pub artifact_version: DbArtifactVersion,
    pub expected_active_version: DbArtifactVersion,
    pub expected_inactive_version: Option<DbArtifactVersion>,
}

impl BpPendingMgsUpdateComponent for BpPendingMgsUpdateSp {
    fn hw_baseboard_id(&self) -> &Uuid {
        &self.hw_baseboard_id
    }

    fn into_generic(self, baseboard_id: Arc<BaseboardId>) -> PendingMgsUpdate {
        PendingMgsUpdate {
            baseboard_id,
            sp_type: self.sp_type.into(),
            slot_id: **self.sp_slot,
            artifact_hash: self.artifact_sha256.into(),
            artifact_version: (*self.artifact_version).clone(),
            details: PendingMgsUpdateDetails::Sp(PendingMgsUpdateSpDetails {
                expected_active_version: (*self.expected_active_version)
                    .clone(),
                expected_inactive_version: match self.expected_inactive_version
                {
                    Some(v) => ExpectedVersion::Version((*v).clone()),
                    None => ExpectedVersion::NoValidVersion,
                },
            }),
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_pending_mgs_update_rot)]
pub struct BpPendingMgsUpdateRot {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub hw_baseboard_id: Uuid,
    pub sp_type: SpType,
    pub sp_slot: SpMgsSlot,
    pub artifact_sha256: ArtifactHash,
    pub artifact_version: DbArtifactVersion,
    pub expected_active_slot: HwRotSlot,
    pub expected_active_version: DbArtifactVersion,
    pub expected_inactive_version: Option<DbArtifactVersion>,
    pub expected_persistent_boot_preference: HwRotSlot,
    pub expected_pending_persistent_boot_preference: Option<HwRotSlot>,
    pub expected_transient_boot_preference: Option<HwRotSlot>,
}

impl BpPendingMgsUpdateComponent for BpPendingMgsUpdateRot {
    fn hw_baseboard_id(&self) -> &Uuid {
        &self.hw_baseboard_id
    }

    fn into_generic(self, baseboard_id: Arc<BaseboardId>) -> PendingMgsUpdate {
        PendingMgsUpdate {
            baseboard_id,
            sp_type: self.sp_type.into(),
            slot_id: **self.sp_slot,
            artifact_hash: self.artifact_sha256.into(),
            artifact_version: (*self.artifact_version).clone(),
            details: PendingMgsUpdateDetails::Rot(PendingMgsUpdateRotDetails {
                expected_active_slot: ExpectedActiveRotSlot {
                    slot: self.expected_active_slot.into(),
                    version: (*self.expected_active_version).clone(),
                },
                expected_inactive_version: self
                    .expected_inactive_version
                    .map(|v| ExpectedVersion::Version(v.into()))
                    .unwrap_or(ExpectedVersion::NoValidVersion),
                expected_persistent_boot_preference: self
                    .expected_persistent_boot_preference
                    .into(),
                expected_pending_persistent_boot_preference: self
                    .expected_pending_persistent_boot_preference
                    .map(|s| s.into()),
                expected_transient_boot_preference: self
                    .expected_transient_boot_preference
                    .map(|s| s.into()),
            }),
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_pending_mgs_update_host_phase_1)]
pub struct BpPendingMgsUpdateHostPhase1 {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub hw_baseboard_id: Uuid,
    pub sp_type: SpType,
    pub sp_slot: SpMgsSlot,
    pub artifact_sha256: ArtifactHash,
    pub artifact_version: DbArtifactVersion,
    pub expected_active_phase_1_slot: HwM2Slot,
    pub expected_boot_disk: HwM2Slot,
    pub expected_active_phase_1_hash: ArtifactHash,
    pub expected_active_phase_2_hash: ArtifactHash,
    pub expected_inactive_phase_1_hash: ArtifactHash,
    pub expected_inactive_phase_2_hash: ArtifactHash,
    sled_agent_ip: ipv6::Ipv6Addr,
    sled_agent_port: SqlU16,
}

impl BpPendingMgsUpdateComponent for BpPendingMgsUpdateHostPhase1 {
    fn hw_baseboard_id(&self) -> &Uuid {
        &self.hw_baseboard_id
    }

    fn into_generic(self, baseboard_id: Arc<BaseboardId>) -> PendingMgsUpdate {
        PendingMgsUpdate {
            baseboard_id,
            sp_type: self.sp_type.into(),
            slot_id: **self.sp_slot,
            artifact_hash: self.artifact_sha256.into(),
            artifact_version: (*self.artifact_version).clone(),
            details: PendingMgsUpdateDetails::HostPhase1(
                PendingMgsUpdateHostPhase1Details {
                    expected_active_phase_1_slot: self
                        .expected_active_phase_1_slot
                        .into(),
                    expected_boot_disk: self.expected_boot_disk.into(),
                    expected_active_phase_1_hash: self
                        .expected_active_phase_1_hash
                        .into(),
                    expected_active_phase_2_hash: self
                        .expected_active_phase_2_hash
                        .into(),
                    expected_inactive_phase_1_hash: self
                        .expected_inactive_phase_1_hash
                        .into(),
                    expected_inactive_phase_2_hash: self
                        .expected_inactive_phase_2_hash
                        .into(),
                    sled_agent_address: SocketAddrV6::new(
                        self.sled_agent_ip.into(),
                        *self.sled_agent_port,
                        0,
                        0,
                    ),
                },
            ),
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = debug_log_blueprint_planning)]
pub struct DebugLogBlueprintPlanning {
    pub blueprint_id: DbTypedUuid<BlueprintKind>,
    pub debug_blob: serde_json::Value,
}

impl DebugLogBlueprintPlanning {
    pub fn new(
        blueprint_id: BlueprintUuid,
        report: Arc<PlanningReport>,
    ) -> Result<Self, serde_json::Error> {
        let report = serde_json::to_value(report)?;

        // We explicitly _don't_ define a struct describing the format of
        // `debug_blob`, because we don't want anyone to attempt to parse it. It
        // should only be useful to humans, potentially via omdb, and they (and
        // omdb) can duplicate these fields to understand it.
        let git_commit = if env!("VERGEN_GIT_DIRTY") == "true" {
            concat!(env!("VERGEN_GIT_SHA"), "-dirty")
        } else {
            env!("VERGEN_GIT_SHA")
        };

        let debug_blob = serde_json::json!({
            "git-commit": git_commit,
            "report": report,
        });

        Ok(Self { blueprint_id: blueprint_id.into(), debug_blob })
    }
}
