// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for representing the deployed software and configuration in the
//! database

use crate::inventory::ZoneType;
use crate::omicron_zone_config::{OmicronZone, OmicronZoneNic};
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
use chrono::{DateTime, Utc};
use ipnetwork::IpNetwork;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZonesConfig;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::disk::DiskIdentity;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use omicron_uuid_kinds::{ExternalIpKind, SledKind, ZpoolKind};
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

/// See [`nexus_types::deployment::OmicronZonesConfig`].
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
/// See [`nexus_types::deployment::OmicronZoneConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_omicron_zone)]
pub struct BpOmicronZone {
    pub blueprint_id: Uuid,
    pub sled_id: DbTypedUuid<SledKind>,
    pub id: Uuid,
    pub underlay_address: ipv6::Ipv6Addr,
    pub filesystem_pool: Option<DbTypedUuid<ZpoolKind>>,
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
}

impl BpOmicronZone {
    pub fn new(
        blueprint_id: Uuid,
        sled_id: SledUuid,
        blueprint_zone: &BlueprintZoneConfig,
    ) -> Result<Self, anyhow::Error> {
        let external_ip_id = blueprint_zone
            .zone_type
            .external_networking()
            .map(|(ip, _)| ip.id());
        let zone = OmicronZone::new(
            sled_id,
            blueprint_zone.id.into_untyped_uuid(),
            blueprint_zone.underlay_address,
            blueprint_zone.filesystem_pool.id(),
            &blueprint_zone.zone_type.clone().into(),
            external_ip_id,
        )?;
        Ok(Self {
            blueprint_id,
            sled_id: zone.sled_id.into(),
            id: zone.id,
            underlay_address: zone.underlay_address,
            filesystem_pool: blueprint_zone.filesystem_pool.id().into(),
            zone_type: zone.zone_type,
            primary_service_ip: zone.primary_service_ip,
            primary_service_port: zone.primary_service_port,
            second_service_ip: zone.second_service_ip,
            second_service_port: zone.second_service_port,
            dataset_zpool_name: zone.dataset_zpool_name,
            bp_nic_id: zone.nic_id,
            dns_gz_address: zone.dns_gz_address,
            dns_gz_address_index: zone.dns_gz_address_index,
            ntp_ntp_servers: zone.ntp_ntp_servers,
            ntp_dns_servers: zone.ntp_dns_servers,
            ntp_domain: zone.ntp_domain,
            nexus_external_tls: zone.nexus_external_tls,
            nexus_external_dns_servers: zone.nexus_external_dns_servers,
            snat_ip: zone.snat_ip,
            snat_first_port: zone.snat_first_port,
            snat_last_port: zone.snat_last_port,
            disposition: to_db_bp_zone_disposition(blueprint_zone.disposition),
            external_ip_id: zone.external_ip_id.map(From::from),
        })
    }

    pub fn into_blueprint_zone_config(
        self,
        nic_row: Option<BpOmicronZoneNic>,
    ) -> Result<BlueprintZoneConfig, anyhow::Error> {
        let zone = OmicronZone {
            sled_id: self.sled_id.into(),
            id: self.id,
            underlay_address: self.underlay_address,
            filesystem_pool: self.filesystem_pool.into(),
            zone_type: self.zone_type,
            primary_service_ip: self.primary_service_ip,
            primary_service_port: self.primary_service_port,
            second_service_ip: self.second_service_ip,
            second_service_port: self.second_service_port,
            dataset_zpool_name: self.dataset_zpool_name,
            nic_id: self.bp_nic_id,
            dns_gz_address: self.dns_gz_address,
            dns_gz_address_index: self.dns_gz_address_index,
            ntp_ntp_servers: self.ntp_ntp_servers,
            ntp_dns_servers: self.ntp_dns_servers,
            ntp_domain: self.ntp_domain,
            nexus_external_tls: self.nexus_external_tls,
            nexus_external_dns_servers: self.nexus_external_dns_servers,
            snat_ip: self.snat_ip,
            snat_first_port: self.snat_first_port,
            snat_last_port: self.snat_last_port,
            external_ip_id: self.external_ip_id.map(From::from),
        };
        zone.into_blueprint_zone_config(
            self.disposition.into(),
            nic_row.map(OmicronZoneNic::from),
        )
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
