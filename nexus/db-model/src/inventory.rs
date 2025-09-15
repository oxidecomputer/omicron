// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for representing the hardware/software inventory in the database

use crate::ArtifactHash;
use crate::Generation;
use crate::PhysicalDiskKind;
use crate::omicron_zone_config::{self, OmicronZoneNic};
use crate::sled_cpu_family::SledCpuFamily;
use crate::typed_uuid::DbTypedUuid;
use crate::{
    ByteCount, MacAddr, Name, ServiceKind, SqlU8, SqlU16, SqlU32,
    impl_enum_type, ipv6,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::DateTime;
use chrono::Utc;
use clickhouse_admin_types::{ClickhouseKeeperClusterMembership, KeeperId};
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::expression::AsExpression;
use diesel::pg::Pg;
use diesel::serialize::ToSql;
use diesel::{serialize, sql_types};
use iddqd::IdOrdMap;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::inv_zone_manifest_non_boot;
use nexus_db_schema::schema::inv_zone_manifest_zone;
use nexus_db_schema::schema::{
    hw_baseboard_id, inv_caboose, inv_clickhouse_keeper_membership,
    inv_cockroachdb_status, inv_collection, inv_collection_error, inv_dataset,
    inv_host_phase_1_active_slot, inv_host_phase_1_flash_hash,
    inv_internal_dns, inv_last_reconciliation_dataset_result,
    inv_last_reconciliation_disk_result,
    inv_last_reconciliation_orphaned_dataset,
    inv_last_reconciliation_zone_result, inv_mupdate_override_non_boot,
    inv_ntp_timesync, inv_nvme_disk_firmware, inv_omicron_sled_config,
    inv_omicron_sled_config_dataset, inv_omicron_sled_config_disk,
    inv_omicron_sled_config_zone, inv_omicron_sled_config_zone_nic,
    inv_physical_disk, inv_root_of_trust, inv_root_of_trust_page,
    inv_service_processor, inv_sled_agent, inv_sled_boot_partition,
    inv_sled_config_reconciler, inv_zpool, sw_caboose, sw_root_of_trust_page,
};
use nexus_sled_agent_shared::inventory::BootImageHeader;
use nexus_sled_agent_shared::inventory::BootPartitionDetails;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
use nexus_sled_agent_shared::inventory::HostPhase2DesiredContents;
use nexus_sled_agent_shared::inventory::HostPhase2DesiredSlots;
use nexus_sled_agent_shared::inventory::MupdateOverrideBootInventory;
use nexus_sled_agent_shared::inventory::MupdateOverrideInventory;
use nexus_sled_agent_shared::inventory::MupdateOverrideNonBootInventory;
use nexus_sled_agent_shared::inventory::OrphanedDataset;
use nexus_sled_agent_shared::inventory::RemoveMupdateOverrideBootSuccessInventory;
use nexus_sled_agent_shared::inventory::RemoveMupdateOverrideInventory;
use nexus_sled_agent_shared::inventory::ZoneArtifactInventory;
use nexus_sled_agent_shared::inventory::ZoneImageResolverInventory;
use nexus_sled_agent_shared::inventory::ZoneManifestBootInventory;
use nexus_sled_agent_shared::inventory::ZoneManifestInventory;
use nexus_sled_agent_shared::inventory::ZoneManifestNonBootInventory;
use nexus_sled_agent_shared::inventory::{
    ConfigReconcilerInventoryResult, OmicronSledConfig, OmicronZoneConfig,
    OmicronZoneDataset, OmicronZoneImageSource, OmicronZoneType,
};
use nexus_types::inventory::HostPhase1ActiveSlot;
use nexus_types::inventory::{
    BaseboardId, Caboose, CockroachStatus, Collection,
    InternalDnsGenerationStatus, NvmeFirmware, PowerState, RotPage, RotSlot,
    TimeSync,
};
use omicron_common::api::external;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetName;
use omicron_common::disk::DiskIdentity;
use omicron_common::disk::M2Slot;
use omicron_common::disk::OmicronPhysicalDiskConfig;
use omicron_common::update::OmicronZoneManifestSource;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::InternalZpoolKind;
use omicron_uuid_kinds::MupdateKind;
use omicron_uuid_kinds::MupdateOverrideKind;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::OmicronSledConfigKind;
use omicron_uuid_kinds::OmicronSledConfigUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolKind;
use omicron_uuid_kinds::{CollectionKind, OmicronZoneKind};
use omicron_uuid_kinds::{CollectionUuid, OmicronZoneUuid};
use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddrV6};
use std::time::Duration;
use thiserror::Error;
use tufaceous_artifact::ArtifactHash as ExternalArtifactHash;
use uuid::Uuid;

// See [`nexus_types::inventory::PowerState`].
impl_enum_type!(
    HwPowerStateEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum HwPowerState;

    // Enum values
    A0 => b"A0"
    A1 => b"A1"
    A2 => b"A2"
);

impl From<PowerState> for HwPowerState {
    fn from(p: PowerState) -> Self {
        match p {
            PowerState::A0 => HwPowerState::A0,
            PowerState::A1 => HwPowerState::A1,
            PowerState::A2 => HwPowerState::A2,
        }
    }
}

impl From<HwPowerState> for PowerState {
    fn from(value: HwPowerState) -> Self {
        match value {
            HwPowerState::A0 => PowerState::A0,
            HwPowerState::A1 => PowerState::A1,
            HwPowerState::A2 => PowerState::A2,
        }
    }
}

// See [`nexus_types::inventory::RotSlot`].
impl_enum_type!(
    HwRotSlotEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum HwRotSlot;

    // Enum values
    A => b"A"
    B => b"B"
);

impl From<RotSlot> for HwRotSlot {
    fn from(value: RotSlot) -> Self {
        match value {
            RotSlot::A => HwRotSlot::A,
            RotSlot::B => HwRotSlot::B,
        }
    }
}

impl From<HwRotSlot> for RotSlot {
    fn from(value: HwRotSlot) -> RotSlot {
        match value {
            HwRotSlot::A => RotSlot::A,
            HwRotSlot::B => RotSlot::B,
        }
    }
}

// See [`M2Slot`].
impl_enum_type!(
    HwM2SlotEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum HwM2Slot;

    // Enum values
    A => b"A"
    B => b"B"
);

impl From<HwM2Slot> for M2Slot {
    fn from(value: HwM2Slot) -> Self {
        match value {
            HwM2Slot::A => Self::A,
            HwM2Slot::B => Self::B,
        }
    }
}

impl From<M2Slot> for HwM2Slot {
    fn from(value: M2Slot) -> Self {
        match value {
            M2Slot::A => Self::A,
            M2Slot::B => Self::B,
        }
    }
}

// See [`nexus_types::inventory::CabooseWhich`].
impl_enum_type!(
    CabooseWhichEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum CabooseWhich;

    // Enum values
    SpSlot0 => b"sp_slot_0"
    SpSlot1 => b"sp_slot_1"
    RotSlotA => b"rot_slot_A"
    RotSlotB => b"rot_slot_B"
    Stage0 => b"stage0"
    Stage0Next => b"stage0next"
);

impl From<nexus_types::inventory::CabooseWhich> for CabooseWhich {
    fn from(c: nexus_types::inventory::CabooseWhich) -> Self {
        use nexus_types::inventory as nexus_inventory;
        match c {
            nexus_inventory::CabooseWhich::SpSlot0 => CabooseWhich::SpSlot0,
            nexus_inventory::CabooseWhich::SpSlot1 => CabooseWhich::SpSlot1,
            nexus_inventory::CabooseWhich::RotSlotA => CabooseWhich::RotSlotA,
            nexus_inventory::CabooseWhich::RotSlotB => CabooseWhich::RotSlotB,
            nexus_inventory::CabooseWhich::Stage0 => CabooseWhich::Stage0,
            nexus_inventory::CabooseWhich::Stage0Next => {
                CabooseWhich::Stage0Next
            }
        }
    }
}

impl From<CabooseWhich> for nexus_types::inventory::CabooseWhich {
    fn from(row: CabooseWhich) -> Self {
        use nexus_types::inventory as nexus_inventory;
        match row {
            CabooseWhich::SpSlot0 => nexus_inventory::CabooseWhich::SpSlot0,
            CabooseWhich::SpSlot1 => nexus_inventory::CabooseWhich::SpSlot1,
            CabooseWhich::RotSlotA => nexus_inventory::CabooseWhich::RotSlotA,
            CabooseWhich::RotSlotB => nexus_inventory::CabooseWhich::RotSlotB,
            CabooseWhich::Stage0 => nexus_inventory::CabooseWhich::Stage0,
            CabooseWhich::Stage0Next => {
                nexus_inventory::CabooseWhich::Stage0Next
            }
        }
    }
}

// See [`nexus_types::inventory::RotPageWhich`].
impl_enum_type!(
    RotPageWhichEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum RotPageWhich;

    // Enum values
    Cmpa => b"cmpa"
    CfpaActive => b"cfpa_active"
    CfpaInactive => b"cfpa_inactive"
    CfpaScratch => b"cfpa_scratch"
);

impl From<nexus_types::inventory::RotPageWhich> for RotPageWhich {
    fn from(c: nexus_types::inventory::RotPageWhich) -> Self {
        use nexus_types::inventory as nexus_inventory;
        match c {
            nexus_inventory::RotPageWhich::Cmpa => RotPageWhich::Cmpa,
            nexus_inventory::RotPageWhich::CfpaActive => {
                RotPageWhich::CfpaActive
            }
            nexus_inventory::RotPageWhich::CfpaInactive => {
                RotPageWhich::CfpaInactive
            }
            nexus_inventory::RotPageWhich::CfpaScratch => {
                RotPageWhich::CfpaScratch
            }
        }
    }
}

impl From<RotPageWhich> for nexus_types::inventory::RotPageWhich {
    fn from(row: RotPageWhich) -> Self {
        use nexus_types::inventory as nexus_inventory;
        match row {
            RotPageWhich::Cmpa => nexus_inventory::RotPageWhich::Cmpa,
            RotPageWhich::CfpaActive => {
                nexus_inventory::RotPageWhich::CfpaActive
            }
            RotPageWhich::CfpaInactive => {
                nexus_inventory::RotPageWhich::CfpaInactive
            }
            RotPageWhich::CfpaScratch => {
                nexus_inventory::RotPageWhich::CfpaScratch
            }
        }
    }
}

// See [`nexus_types::inventory::RotImageError`].
impl_enum_type!(
    RotImageErrorEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum RotImageError;

    // Enum values
    Unchecked => b"unchecked"
    FirstPageErased => b"first_page_erased"
    PartiallyProgrammed => b"partially_programmed"
    InvalidLength => b"invalid_length"
    HeaderNotProgrammed => b"header_not_programmed"
    BootloaderTooSmall => b"bootloader_too_small"
    BadMagic => b"bad_magic"
    HeaderImageSize => b"header_image_size"
    UnalignedLength => b"unaligned_length"
    UnsupportedType => b"unsupported_type"
    ResetVectorNotThumb2 => b"not_thumb2"
    ResetVector => b"reset_vector"
    Signature => b"signature"

);

impl From<nexus_types::inventory::RotImageError> for RotImageError {
    fn from(c: nexus_types::inventory::RotImageError) -> Self {
        match c {
            nexus_types::inventory::RotImageError::Unchecked => {
                RotImageError::Unchecked
            }
            nexus_types::inventory::RotImageError::FirstPageErased => {
                RotImageError::FirstPageErased
            }
            nexus_types::inventory::RotImageError::PartiallyProgrammed => {
                RotImageError::PartiallyProgrammed
            }
            nexus_types::inventory::RotImageError::InvalidLength => {
                RotImageError::InvalidLength
            }
            nexus_types::inventory::RotImageError::HeaderNotProgrammed => {
                RotImageError::HeaderNotProgrammed
            }
            nexus_types::inventory::RotImageError::BootloaderTooSmall => {
                RotImageError::BootloaderTooSmall
            }
            nexus_types::inventory::RotImageError::BadMagic => {
                RotImageError::BadMagic
            }
            nexus_types::inventory::RotImageError::HeaderImageSize => {
                RotImageError::HeaderImageSize
            }
            nexus_types::inventory::RotImageError::UnalignedLength => {
                RotImageError::UnalignedLength
            }
            nexus_types::inventory::RotImageError::UnsupportedType => {
                RotImageError::UnsupportedType
            }
            nexus_types::inventory::RotImageError::ResetVectorNotThumb2 => {
                RotImageError::ResetVectorNotThumb2
            }
            nexus_types::inventory::RotImageError::ResetVector => {
                RotImageError::ResetVector
            }
            nexus_types::inventory::RotImageError::Signature => {
                RotImageError::Signature
            }
        }
    }
}

impl From<RotImageError> for nexus_types::inventory::RotImageError {
    fn from(row: RotImageError) -> Self {
        match row {
            RotImageError::Unchecked => {
                nexus_types::inventory::RotImageError::Unchecked
            }
            RotImageError::FirstPageErased => {
                nexus_types::inventory::RotImageError::FirstPageErased
            }
            RotImageError::PartiallyProgrammed => {
                nexus_types::inventory::RotImageError::PartiallyProgrammed
            }
            RotImageError::InvalidLength => {
                nexus_types::inventory::RotImageError::InvalidLength
            }
            RotImageError::HeaderNotProgrammed => {
                nexus_types::inventory::RotImageError::HeaderNotProgrammed
            }
            RotImageError::BootloaderTooSmall => {
                nexus_types::inventory::RotImageError::BootloaderTooSmall
            }
            RotImageError::BadMagic => {
                nexus_types::inventory::RotImageError::BadMagic
            }
            RotImageError::HeaderImageSize => {
                nexus_types::inventory::RotImageError::HeaderImageSize
            }
            RotImageError::UnalignedLength => {
                nexus_types::inventory::RotImageError::UnalignedLength
            }
            RotImageError::UnsupportedType => {
                nexus_types::inventory::RotImageError::UnsupportedType
            }
            RotImageError::ResetVectorNotThumb2 => {
                nexus_types::inventory::RotImageError::ResetVectorNotThumb2
            }
            RotImageError::ResetVector => {
                nexus_types::inventory::RotImageError::ResetVector
            }
            RotImageError::Signature => {
                nexus_types::inventory::RotImageError::Signature
            }
        }
    }
}

// See [`nexus_types::inventory::SpType`].
impl_enum_type!(
    SpTypeEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        AsExpression,
        FromSqlRow,
        PartialOrd,
        Ord,
        PartialEq,
        Eq
    )]
    pub enum SpType;

    // Enum values
    Sled => b"sled"
    Switch =>  b"switch"
    Power => b"power"
);

impl From<nexus_types::inventory::SpType> for SpType {
    fn from(value: nexus_types::inventory::SpType) -> Self {
        match value {
            nexus_types::inventory::SpType::Sled => SpType::Sled,
            nexus_types::inventory::SpType::Power => SpType::Power,
            nexus_types::inventory::SpType::Switch => SpType::Switch,
        }
    }
}

impl From<SpType> for nexus_types::inventory::SpType {
    fn from(value: SpType) -> Self {
        match value {
            SpType::Sled => nexus_types::inventory::SpType::Sled,
            SpType::Switch => nexus_types::inventory::SpType::Switch,
            SpType::Power => nexus_types::inventory::SpType::Power,
        }
    }
}

/// See [`nexus_types::inventory::Collection`].
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_collection)]
pub struct InvCollection {
    pub id: DbTypedUuid<CollectionKind>,
    pub time_started: DateTime<Utc>,
    pub time_done: DateTime<Utc>,
    pub collector: String,
}

impl InvCollection {
    /// Creates a new `InvCollection`.
    pub fn new(
        id: CollectionUuid,
        time_started: DateTime<Utc>,
        time_done: DateTime<Utc>,
        collector: String,
    ) -> Self {
        InvCollection { id: id.into(), time_started, time_done, collector }
    }

    /// Returns the ID.
    pub fn id(&self) -> CollectionUuid {
        self.id.into()
    }
}

impl<'a> From<&'a Collection> for InvCollection {
    fn from(c: &'a Collection) -> Self {
        InvCollection {
            id: c.id.into(),
            time_started: c.time_started,
            time_done: c.time_done,
            collector: c.collector.clone(),
        }
    }
}

/// See [`nexus_types::inventory::BaseboardId`].
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = hw_baseboard_id)]
pub struct HwBaseboardId {
    pub id: Uuid,
    pub part_number: String,
    pub serial_number: String,
}

impl From<BaseboardId> for HwBaseboardId {
    fn from(c: BaseboardId) -> Self {
        HwBaseboardId {
            id: Uuid::new_v4(),
            part_number: c.part_number,
            serial_number: c.serial_number,
        }
    }
}

impl From<HwBaseboardId> for BaseboardId {
    fn from(row: HwBaseboardId) -> Self {
        BaseboardId {
            part_number: row.part_number,
            serial_number: row.serial_number,
        }
    }
}

/// See [`nexus_types::inventory::Caboose`].
#[derive(
    Queryable,
    Insertable,
    Clone,
    Debug,
    Selectable,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
)]
#[diesel(table_name = sw_caboose)]
pub struct SwCaboose {
    pub id: Uuid,
    pub board: String,
    pub git_commit: String,
    pub name: String,
    pub version: String,
    pub sign: Option<String>,
}

impl From<Caboose> for SwCaboose {
    fn from(c: Caboose) -> Self {
        SwCaboose {
            id: Uuid::new_v4(),
            board: c.board,
            git_commit: c.git_commit,
            name: c.name,
            version: c.version,
            sign: c.sign,
        }
    }
}

impl From<SwCaboose> for Caboose {
    fn from(row: SwCaboose) -> Self {
        Self {
            board: row.board,
            git_commit: row.git_commit,
            name: row.name,
            version: row.version,
            sign: row.sign,
        }
    }
}

/// See [`nexus_types::inventory::RotPage`].
#[derive(
    Queryable,
    Insertable,
    Clone,
    Debug,
    Selectable,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
)]
#[diesel(table_name = sw_root_of_trust_page)]
pub struct SwRotPage {
    pub id: Uuid,
    pub data_base64: String,
}

impl From<RotPage> for SwRotPage {
    fn from(p: RotPage) -> Self {
        Self { id: Uuid::new_v4(), data_base64: p.data_base64 }
    }
}

impl From<SwRotPage> for RotPage {
    fn from(row: SwRotPage) -> Self {
        Self { data_base64: row.data_base64 }
    }
}

/// See [`nexus_types::inventory::Collection`].
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_collection_error)]
pub struct InvCollectionError {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub idx: SqlU16,
    pub message: String,
}

impl InvCollectionError {
    pub fn new(
        inv_collection_id: CollectionUuid,
        idx: u16,
        message: String,
    ) -> Self {
        InvCollectionError {
            inv_collection_id: inv_collection_id.into(),
            idx: SqlU16::from(idx),
            message,
        }
    }

    pub fn inv_collection_id(&self) -> CollectionUuid {
        self.inv_collection_id.into()
    }
}

/// See [`nexus_types::inventory::ServiceProcessor`].
#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_service_processor)]
pub struct InvServiceProcessor {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub hw_baseboard_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub sp_type: SpType,
    pub sp_slot: SpMgsSlot,

    pub baseboard_revision: BaseboardRevision,
    pub hubris_archive_id: String,
    pub power_state: HwPowerState,
}

impl From<InvServiceProcessor> for nexus_types::inventory::ServiceProcessor {
    fn from(row: InvServiceProcessor) -> Self {
        Self {
            time_collected: row.time_collected,
            source: row.source,
            sp_type: nexus_types::inventory::SpType::from(row.sp_type),
            sp_slot: **row.sp_slot,
            baseboard_revision: **row.baseboard_revision,
            hubris_archive: row.hubris_archive_id,
            power_state: PowerState::from(row.power_state),
        }
    }
}

/// Newtype wrapping the MGS-reported slot number for an SP
///
/// Current racks only have 32 slots for any given SP type.  MGS represents the
/// slot number with a u32.  We truncate it to a u16 (which still requires
/// storing it as an i32 in the database, since the database doesn't natively
/// support signed integers).
#[derive(Copy, Clone, Debug, AsExpression, FromSqlRow)]
#[diesel(sql_type = sql_types::Int4)]
pub struct SpMgsSlot(SqlU16);

NewtypeFrom! { () pub struct SpMgsSlot(SqlU16); }
NewtypeDeref! { () pub struct SpMgsSlot(SqlU16); }
NewtypeDisplay! { () pub struct SpMgsSlot(SqlU16); }

impl ToSql<sql_types::Int4, Pg> for SpMgsSlot {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <SqlU16 as ToSql<sql_types::Int4, Pg>>::to_sql(
            &self.0,
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Int4, DB> for SpMgsSlot
where
    DB: Backend,
    SqlU16: FromSql<sql_types::Int4, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        Ok(SpMgsSlot(SqlU16::from_sql(bytes)?))
    }
}

impl From<u16> for SpMgsSlot {
    fn from(slot: u16) -> Self {
        Self(SqlU16::new(slot))
    }
}

/// Newtype wrapping the revision number for a particular baseboard
///
/// MGS reports this as a u32 and we represent it the same way, though that
/// would be quite a lot of hardware revisions to go through!
#[derive(Copy, Clone, Debug, AsExpression, FromSqlRow)]
#[diesel(sql_type = sql_types::Int8)]
pub struct BaseboardRevision(SqlU32);

NewtypeFrom! { () pub struct BaseboardRevision(SqlU32); }
NewtypeDeref! { () pub struct BaseboardRevision(SqlU32); }
NewtypeDisplay! { () pub struct BaseboardRevision(SqlU32); }

impl ToSql<sql_types::Int8, Pg> for BaseboardRevision {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <SqlU32 as ToSql<sql_types::Int8, Pg>>::to_sql(
            &self.0,
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Int8, DB> for BaseboardRevision
where
    DB: Backend,
    SqlU32: FromSql<sql_types::Int8, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        Ok(BaseboardRevision(SqlU32::from_sql(bytes)?))
    }
}

/// See [`nexus_types::inventory::RotState`].
#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_root_of_trust)]
pub struct InvRootOfTrust {
    pub inv_collection_id: Uuid,
    pub hw_baseboard_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub slot_active: HwRotSlot,
    pub slot_boot_pref_transient: Option<HwRotSlot>,
    pub slot_boot_pref_persistent: HwRotSlot,
    pub slot_boot_pref_persistent_pending: Option<HwRotSlot>,
    pub slot_a_sha3_256: Option<String>,
    pub slot_b_sha3_256: Option<String>,
    pub stage0_fwid: Option<String>,
    pub stage0next_fwid: Option<String>,

    pub slot_a_error: Option<RotImageError>,
    pub slot_b_error: Option<RotImageError>,
    pub stage0_error: Option<RotImageError>,
    pub stage0next_error: Option<RotImageError>,
}

impl From<InvRootOfTrust> for nexus_types::inventory::RotState {
    fn from(row: InvRootOfTrust) -> Self {
        Self {
            time_collected: row.time_collected,
            source: row.source,
            active_slot: RotSlot::from(row.slot_active),
            persistent_boot_preference: RotSlot::from(
                row.slot_boot_pref_persistent,
            ),
            pending_persistent_boot_preference: row
                .slot_boot_pref_persistent_pending
                .map(RotSlot::from),
            transient_boot_preference: row
                .slot_boot_pref_transient
                .map(RotSlot::from),
            slot_a_sha3_256_digest: row.slot_a_sha3_256,
            slot_b_sha3_256_digest: row.slot_b_sha3_256,
            stage0_digest: row.stage0_fwid,
            stage0next_digest: row.stage0next_fwid,

            slot_a_error: row
                .slot_a_error
                .map(nexus_types::inventory::RotImageError::from),
            slot_b_error: row
                .slot_b_error
                .map(nexus_types::inventory::RotImageError::from),
            stage0_error: row
                .stage0_error
                .map(nexus_types::inventory::RotImageError::from),
            stage0next_error: row
                .stage0next_error
                .map(nexus_types::inventory::RotImageError::from),
        }
    }
}

/// See [`nexus_types::inventory::HostPhase1ActiveSlot`].
#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_host_phase_1_active_slot)]
pub struct InvHostPhase1ActiveSlot {
    pub inv_collection_id: Uuid,
    pub hw_baseboard_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub slot: HwM2Slot,
}

impl From<InvHostPhase1ActiveSlot> for HostPhase1ActiveSlot {
    fn from(value: InvHostPhase1ActiveSlot) -> Self {
        Self {
            time_collected: value.time_collected,
            source: value.source,
            slot: value.slot.into(),
        }
    }
}

/// See [`nexus_types::inventory::HostPhase1FlashHash`].
#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_host_phase_1_flash_hash)]
pub struct InvHostPhase1FlashHash {
    pub inv_collection_id: Uuid,
    pub hw_baseboard_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub slot: HwM2Slot,
    pub hash: ArtifactHash,
}

/// See [`nexus_types::inventory::CabooseFound`].
#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_caboose)]
pub struct InvCaboose {
    pub inv_collection_id: Uuid,
    pub hw_baseboard_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub which: CabooseWhich,
    pub sw_caboose_id: Uuid,
}

/// See [`nexus_types::inventory::RotPageFound`].
#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_root_of_trust_page)]
pub struct InvRotPage {
    pub inv_collection_id: Uuid,
    pub hw_baseboard_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub which: RotPageWhich,
    pub sw_root_of_trust_page_id: Uuid,
}

// See [`nexus_types::inventory::SledRole`].
impl_enum_type!(
    SledRoleEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        AsExpression,
        FromSqlRow,
        PartialOrd,
        Ord,
        PartialEq,
        Eq
    )]
    pub enum SledRole;

    // Enum values
    Gimlet => b"gimlet"
    Scrimlet =>  b"scrimlet"
);

impl From<nexus_sled_agent_shared::inventory::SledRole> for SledRole {
    fn from(value: nexus_sled_agent_shared::inventory::SledRole) -> Self {
        match value {
            nexus_sled_agent_shared::inventory::SledRole::Gimlet => {
                SledRole::Gimlet
            }
            nexus_sled_agent_shared::inventory::SledRole::Scrimlet => {
                SledRole::Scrimlet
            }
        }
    }
}

impl From<SledRole> for nexus_sled_agent_shared::inventory::SledRole {
    fn from(value: SledRole) -> Self {
        match value {
            SledRole::Gimlet => {
                nexus_sled_agent_shared::inventory::SledRole::Gimlet
            }
            SledRole::Scrimlet => {
                nexus_sled_agent_shared::inventory::SledRole::Scrimlet
            }
        }
    }
}

/// See [`nexus_types::inventory::SledAgent`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_sled_agent)]
pub struct InvSledAgent {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub time_collected: DateTime<Utc>,
    pub source: String,
    pub sled_id: DbTypedUuid<SledKind>,
    pub hw_baseboard_id: Option<Uuid>,
    pub sled_agent_ip: ipv6::Ipv6Addr,
    pub sled_agent_port: SqlU16,
    pub sled_role: SledRole,
    pub usable_hardware_threads: SqlU32,
    pub usable_physical_ram: ByteCount,
    pub cpu_family: SledCpuFamily,
    pub reservoir_size: ByteCount,
    // Soft foreign key to an `InvOmicronSledConfig`
    pub ledgered_sled_config: Option<DbTypedUuid<OmicronSledConfigKind>>,

    #[diesel(embed)]
    pub reconciler_status: InvConfigReconcilerStatus,

    #[diesel(embed)]
    pub zone_image_resolver: InvZoneImageResolver,
}

/// See [`nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_sled_agent)]
pub struct InvConfigReconcilerStatus {
    pub reconciler_status_kind: InvConfigReconcilerStatusKind,
    // Soft foreign key to an `InvOmicronSledConfig`. May or may not be the same
    // as either of the other sled config keys above. Only populated if
    // `reconciler_status_kind` is `Running`.
    pub reconciler_status_sled_config:
        Option<DbTypedUuid<OmicronSledConfigKind>>,
    // Interpretation varies based on `reconciler_status_kind`:
    //
    // * `NotYetRun` - always `None`
    // * `Running` - `started_at` time
    // * `Idle` - `completed_at` time
    pub reconciler_status_timestamp: Option<DateTime<Utc>>,
    // Interpretation varies based on `reconciler_status_kind`:
    //
    // * `NotYetRun` - always `None`
    // * `Running` - `running_for` duration
    // * `Idle` - `ran_for` duration
    pub reconciler_status_duration_secs: Option<f64>,
}

impl InvConfigReconcilerStatus {
    /// Convert `self` to a [`ConfigReconcilerInventoryStatus`].
    ///
    /// `get_config` should perform the lookup of a serialized
    /// `OmicronSledConfig` given its ID. It will be called only if `self.kind`
    /// is `InvConfigReconcilerStatusKind::Running`; the other variants do not
    /// carry a sled config ID.
    pub fn to_status<F>(
        &self,
        get_config: F,
    ) -> anyhow::Result<ConfigReconcilerInventoryStatus>
    where
        F: FnOnce(&OmicronSledConfigUuid) -> Option<OmicronSledConfig>,
    {
        let status = match self.reconciler_status_kind {
            InvConfigReconcilerStatusKind::NotYetRun => {
                ConfigReconcilerInventoryStatus::NotYetRun
            }
            InvConfigReconcilerStatusKind::Running => {
                let config_id = self.reconciler_status_sled_config.context(
                    "missing reconciler status sled config for kind 'running'",
                )?;
                let config = get_config(&config_id.into())
                    .context("missing sled config we should have fetched")?;
                ConfigReconcilerInventoryStatus::Running {
                    config,
                    started_at: self.reconciler_status_timestamp.context(
                        "missing reconciler status timestamp \
                         for kind 'running'",
                    )?,
                    running_for: Duration::from_secs_f64(
                        self.reconciler_status_duration_secs.context(
                            "missing reconciler status duration \
                             for kind 'running'",
                        )?,
                    ),
                }
            }
            InvConfigReconcilerStatusKind::Idle => {
                ConfigReconcilerInventoryStatus::Idle {
                    completed_at: self.reconciler_status_timestamp.context(
                        "missing reconciler status timestamp for kind 'idle'",
                    )?,
                    ran_for: Duration::from_secs_f64(
                        self.reconciler_status_duration_secs.context(
                            "missing reconciler status duration \
                             for kind 'idle'",
                        )?,
                    ),
                }
            }
        };
        Ok(status)
    }
}

// See [`nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus`].
impl_enum_type!(
    InvConfigReconcilerStatusKindEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum InvConfigReconcilerStatusKind;

    // Enum values
    NotYetRun => b"not-yet-run"
    Running => b"running"
    Idle => b"idle"
);

/// See [`nexus_sled_agent_shared::inventory::ConfigReconcilerInventory`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_sled_config_reconciler)]
pub struct InvSledConfigReconciler {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub last_reconciled_config: DbTypedUuid<OmicronSledConfigKind>,
    boot_disk_slot: Option<SqlU8>,
    boot_disk_error: Option<String>,
    pub boot_partition_a_error: Option<String>,
    pub boot_partition_b_error: Option<String>,
    #[diesel(embed)]
    pub remove_mupdate_override: InvRemoveMupdateOverride,
}

impl InvSledConfigReconciler {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_id: SledUuid,
        last_reconciled_config: OmicronSledConfigUuid,
        boot_disk: Result<M2Slot, String>,
        boot_partition_a_error: Option<String>,
        boot_partition_b_error: Option<String>,
        remove_mupdate_override: InvRemoveMupdateOverride,
    ) -> Self {
        // TODO-cleanup We should use `HwM2Slot` instead of integers for this
        // column: https://github.com/oxidecomputer/omicron/issues/8642
        let (boot_disk_slot, boot_disk_error) = match boot_disk {
            Ok(M2Slot::A) => (Some(SqlU8(0)), None),
            Ok(M2Slot::B) => (Some(SqlU8(1)), None),
            Err(err) => (None, Some(err)),
        };

        Self {
            inv_collection_id: inv_collection_id.into(),
            sled_id: sled_id.into(),
            last_reconciled_config: last_reconciled_config.into(),
            boot_disk_slot,
            boot_disk_error,
            boot_partition_a_error,
            boot_partition_b_error,
            remove_mupdate_override,
        }
    }

    pub fn boot_disk(&self) -> anyhow::Result<Result<M2Slot, String>> {
        match (self.boot_disk_slot.as_deref(), self.boot_disk_error.as_ref()) {
            (Some(0), None) => Ok(Ok(M2Slot::A)),
            (Some(1), None) => Ok(Ok(M2Slot::B)),
            (Some(n), None) => {
                bail!(
                    "inv_sled_config_reconciler CHECK constraint violated \
                     (collection {}, sled {}): \
                     boot_disk_slot other than 0 or 1: {n}",
                    self.inv_collection_id,
                    self.sled_id,
                );
            }
            (None, Some(err)) => Ok(Err(err.clone())),
            (None, None) => {
                bail!(
                    "inv_sled_config_reconciler CHECK constraint violated \
                     (collection {}, sled {}): \
                     neither boot_disk_slot nor boot_disk_error set",
                    self.inv_collection_id,
                    self.sled_id,
                );
            }
            (Some(_), Some(_)) => {
                bail!(
                    "inv_sled_config_reconciler CHECK constraint violated \
                     (collection {}, sled {}): \
                     both boot_disk_slot and boot_disk_error set",
                    self.inv_collection_id,
                    self.sled_id,
                );
            }
        }
    }
}

// See [`nexus_sled_agent_shared::inventory::DbRemoveMupdateOverrideBootSuccess`].
impl_enum_type!(
    RemoveMupdateOverrideBootSuccessEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum DbRemoveMupdateOverrideBootSuccess;

    // Enum values
    Cleared => b"cleared"
    NoOverride => b"no-override"
);

impl From<RemoveMupdateOverrideBootSuccessInventory>
    for DbRemoveMupdateOverrideBootSuccess
{
    fn from(value: RemoveMupdateOverrideBootSuccessInventory) -> Self {
        match value {
            RemoveMupdateOverrideBootSuccessInventory::Removed => Self::Cleared,
            RemoveMupdateOverrideBootSuccessInventory::NoOverride => {
                Self::NoOverride
            }
        }
    }
}

impl From<DbRemoveMupdateOverrideBootSuccess>
    for RemoveMupdateOverrideBootSuccessInventory
{
    fn from(value: DbRemoveMupdateOverrideBootSuccess) -> Self {
        match value {
            DbRemoveMupdateOverrideBootSuccess::Cleared => Self::Removed,
            DbRemoveMupdateOverrideBootSuccess::NoOverride => Self::NoOverride,
        }
    }
}

/// See [`nexus_sled_agent_shared::inventory::RemoveMupdateOverrideInventory`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_sled_config_reconciler)]
pub struct InvRemoveMupdateOverride {
    // NOTE: the column names start with "clear_" for legacy reasons. Prefer
    // "remove" in the future.
    #[diesel(column_name = clear_mupdate_override_boot_success)]
    pub boot_success: Option<DbRemoveMupdateOverrideBootSuccess>,

    #[diesel(column_name = clear_mupdate_override_boot_error)]
    pub boot_error: Option<String>,

    #[diesel(column_name = clear_mupdate_override_non_boot_message)]
    pub non_boot_message: Option<String>,
}

impl InvRemoveMupdateOverride {
    pub fn new(
        remove_mupdate_override: Option<&RemoveMupdateOverrideInventory>,
    ) -> Self {
        let boot_success = remove_mupdate_override.and_then(|inv| {
            inv.boot_disk_result.as_ref().ok().map(|v| v.clone().into())
        });
        let boot_error = remove_mupdate_override
            .and_then(|inv| inv.boot_disk_result.as_ref().err().cloned());
        let non_boot_message =
            remove_mupdate_override.map(|inv| inv.non_boot_message.clone());

        Self { boot_success, boot_error, non_boot_message }
    }

    pub fn into_inventory(
        self,
    ) -> anyhow::Result<Option<RemoveMupdateOverrideInventory>> {
        match self {
            Self {
                boot_success: Some(success),
                boot_error: None,
                non_boot_message: Some(non_boot_message),
            } => Ok(Some(RemoveMupdateOverrideInventory {
                boot_disk_result: Ok(success.into()),
                non_boot_message,
            })),
            Self {
                boot_success: None,
                boot_error: Some(boot_error),
                non_boot_message: Some(non_boot_message),
            } => Ok(Some(RemoveMupdateOverrideInventory {
                boot_disk_result: Err(boot_error),
                non_boot_message,
            })),
            Self {
                boot_success: None,
                boot_error: None,
                non_boot_message: None,
            } => Ok(None),
            this => Err(anyhow!(
                "inv_sled_config_reconciler CHECK constraint violated: \
                 clear mupdate override columns are not consistent: {this:?}"
            )),
        }
    }
}

/// See [`nexus_sled_agent_shared::inventory::BootPartitionDetails`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_sled_boot_partition)]
pub struct InvSledBootPartition {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    /// This field is `pub` so `nexus-db-queries` can use it during pagination;
    /// consumers of this type probably want the `slot()` method instead to
    /// convert this value to an [`M2Slot`].
    pub boot_disk_slot: SqlU8,
    artifact_hash: ArtifactHash,
    artifact_size: i64,
    header_flags: i64,
    header_data_size: i64,
    header_image_size: i64,
    header_target_size: i64,
    header_sha256: ArtifactHash,
    header_image_name: String,
}

impl InvSledBootPartition {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_id: SledUuid,
        boot_disk_slot: M2Slot,
        details: BootPartitionDetails,
    ) -> Self {
        let boot_disk_slot = match boot_disk_slot {
            M2Slot::A => 0,
            M2Slot::B => 1,
        };
        Self {
            inv_collection_id: inv_collection_id.into(),
            sled_id: sled_id.into(),
            boot_disk_slot: SqlU8(boot_disk_slot),
            artifact_hash: ArtifactHash(details.artifact_hash),
            // We use `as i64` because we don't want to throw errors if any of
            // these fields happen to have their high bit set, and we don't care
            // that that might produce negative numbers in the DB. E.g., we
            // never need to order columns based on any of these values; we only
            // want to faithfully store and load them. My kingdom for unsigned
            // integers in SQL.
            artifact_size: details.artifact_size as i64,
            header_flags: details.header.flags as i64,
            header_data_size: details.header.data_size as i64,
            header_image_size: details.header.image_size as i64,
            header_target_size: details.header.target_size as i64,
            header_sha256: ArtifactHash(ExternalArtifactHash(
                details.header.sha256,
            )),
            header_image_name: details.header.image_name,
        }
    }

    pub fn slot(&self) -> anyhow::Result<M2Slot> {
        match *self.boot_disk_slot {
            0 => Ok(M2Slot::A),
            1 => Ok(M2Slot::B),
            n => bail!(
                "inv_sled_boot_partition CHECK constraint violated \
                 (collection {}, sled {}): \
                 boot_disk_slot other than 0 or 1: {n}",
                self.inv_collection_id,
                self.sled_id,
            ),
        }
    }
}

impl From<InvSledBootPartition> for BootPartitionDetails {
    fn from(value: InvSledBootPartition) -> Self {
        Self {
            artifact_hash: *value.artifact_hash,
            artifact_size: value.artifact_size as usize,
            header: BootImageHeader {
                flags: value.header_flags as u64,
                data_size: value.header_data_size as u64,
                image_size: value.header_image_size as u64,
                target_size: value.header_target_size as u64,
                sha256: value.header_sha256.0.0,
                image_name: value.header_image_name,
            },
        }
    }
}

impl InvSledAgent {
    /// Construct a new `InvSledAgent`.
    pub fn new_without_baseboard(
        collection_id: CollectionUuid,
        sled_agent: &nexus_types::inventory::SledAgent,
        ledgered_sled_config: Option<OmicronSledConfigUuid>,
        reconciler_status: InvConfigReconcilerStatus,
        zone_image_resolver: InvZoneImageResolver,
    ) -> Result<InvSledAgent, anyhow::Error> {
        // It's irritating to have to check this case at runtime.  The challenge
        // is that if this sled agent does have a baseboard id, we don't know
        // what its (SQL) id is.  The only way to get it is to query it from
        // the database.  As a result, the caller takes a wholly different code
        // path for that case that doesn't even involve constructing one of
        // these objects.  (In fact, we never see the id in Rust.)
        //
        // To check this at compile time, we'd have to bifurcate
        // `nexus_types::inventory::SledAgent` into an enum with two variants:
        // one with a baseboard id and one without.  This would muck up all the
        // other consumers of this type, just for a highly database-specific
        // concern.
        if sled_agent.baseboard_id.is_some() {
            Err(anyhow!(
                "attempted to directly insert InvSledAgent with \
                non-null baseboard id"
            ))
        } else {
            Ok(InvSledAgent {
                inv_collection_id: collection_id.into(),
                time_collected: sled_agent.time_collected,
                source: sled_agent.source.clone(),
                sled_id: sled_agent.sled_id.into(),
                hw_baseboard_id: None,
                sled_agent_ip: ipv6::Ipv6Addr::from(
                    *sled_agent.sled_agent_address.ip(),
                ),
                sled_agent_port: SqlU16(sled_agent.sled_agent_address.port()),
                sled_role: SledRole::from(sled_agent.sled_role),
                usable_hardware_threads: SqlU32(
                    sled_agent.usable_hardware_threads,
                ),
                usable_physical_ram: ByteCount::from(
                    sled_agent.usable_physical_ram,
                ),
                cpu_family: sled_agent.cpu_family.into(),
                reservoir_size: ByteCount::from(sled_agent.reservoir_size),
                ledgered_sled_config: ledgered_sled_config.map(From::from),
                reconciler_status,
                zone_image_resolver,
            })
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_last_reconciliation_disk_result)]
pub struct InvLastReconciliationDiskResult {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub disk_id: DbTypedUuid<omicron_uuid_kinds::PhysicalDiskKind>,
    pub error_message: Option<String>,
}

impl InvLastReconciliationDiskResult {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_id: SledUuid,
        disk_id: PhysicalDiskUuid,
        result: ConfigReconcilerInventoryResult,
    ) -> Self {
        let error_message = match result {
            ConfigReconcilerInventoryResult::Ok => None,
            ConfigReconcilerInventoryResult::Err { message } => Some(message),
        };
        Self {
            inv_collection_id: inv_collection_id.into(),
            sled_id: sled_id.into(),
            disk_id: disk_id.into(),
            error_message,
        }
    }
}

impl From<InvLastReconciliationDiskResult> for ConfigReconcilerInventoryResult {
    fn from(result: InvLastReconciliationDiskResult) -> Self {
        match result.error_message {
            None => Self::Ok,
            Some(message) => Self::Err { message },
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_last_reconciliation_dataset_result)]
pub struct InvLastReconciliationDatasetResult {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub dataset_id: DbTypedUuid<DatasetKind>,
    pub error_message: Option<String>,
}

impl InvLastReconciliationDatasetResult {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_id: SledUuid,
        dataset_id: DatasetUuid,
        result: ConfigReconcilerInventoryResult,
    ) -> Self {
        let error_message = match result {
            ConfigReconcilerInventoryResult::Ok => None,
            ConfigReconcilerInventoryResult::Err { message } => Some(message),
        };
        Self {
            inv_collection_id: inv_collection_id.into(),
            sled_id: sled_id.into(),
            dataset_id: dataset_id.into(),
            error_message,
        }
    }
}

impl From<InvLastReconciliationDatasetResult>
    for ConfigReconcilerInventoryResult
{
    fn from(result: InvLastReconciliationDatasetResult) -> Self {
        match result.error_message {
            None => Self::Ok,
            Some(message) => Self::Err { message },
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_last_reconciliation_orphaned_dataset)]
pub struct InvLastReconciliationOrphanedDataset {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pool_id: DbTypedUuid<ZpoolKind>,
    kind: crate::DatasetKind,
    zone_name: String,
    pub reason: String,
    pub id: Option<DbTypedUuid<DatasetKind>>,
    pub mounted: bool,
    pub available: ByteCount,
    pub used: ByteCount,
}

impl InvLastReconciliationOrphanedDataset {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_id: SledUuid,
        orphan: OrphanedDataset,
    ) -> Self {
        let OrphanedDataset { name, reason, id, mounted, available, used } =
            orphan;
        let (pool, kind) = name.into_parts();

        Self {
            inv_collection_id: inv_collection_id.into(),
            sled_id: sled_id.into(),
            pool_id: pool.id().into(),
            kind: (&kind).into(),
            zone_name: kind.zone_name().map(String::from).unwrap_or_default(),
            reason,
            id: id.map(From::from),
            mounted,
            available: ByteCount(available),
            used: ByteCount(used),
        }
    }
}

impl TryFrom<InvLastReconciliationOrphanedDataset> for OrphanedDataset {
    type Error = omicron_common::api::external::Error;

    fn try_from(
        row: InvLastReconciliationOrphanedDataset,
    ) -> Result<Self, Self::Error> {
        let pool = ZpoolName::new_external(row.pool_id.into());
        // See comment in `dbinit.sql`; we treat the empty string as "no zone
        // name" so the zone name can be a part of the primary key, but need to
        // convert that back into an option here for `try_into_api()`.
        let zone_name =
            if row.zone_name.is_empty() { None } else { Some(row.zone_name) };
        let kind = crate::DatasetKind::try_into_api(row.kind, zone_name)?;
        let name = DatasetName::new(pool, kind);
        Ok(Self {
            name,
            reason: row.reason,
            id: row.id.map(From::from),
            mounted: row.mounted,
            available: *row.available,
            used: *row.used,
        })
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_last_reconciliation_zone_result)]
pub struct InvLastReconciliationZoneResult {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub zone_id: DbTypedUuid<omicron_uuid_kinds::OmicronZoneKind>,
    pub error_message: Option<String>,
}

impl InvLastReconciliationZoneResult {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_id: SledUuid,
        zone_id: OmicronZoneUuid,
        result: ConfigReconcilerInventoryResult,
    ) -> Self {
        let error_message = match result {
            ConfigReconcilerInventoryResult::Ok => None,
            ConfigReconcilerInventoryResult::Err { message } => Some(message),
        };
        Self {
            inv_collection_id: inv_collection_id.into(),
            sled_id: sled_id.into(),
            zone_id: zone_id.into(),
            error_message,
        }
    }
}

impl From<InvLastReconciliationZoneResult> for ConfigReconcilerInventoryResult {
    fn from(result: InvLastReconciliationZoneResult) -> Self {
        match result.error_message {
            None => Self::Ok,
            Some(message) => Self::Err { message },
        }
    }
}

// See [`omicron_common::update::OmicronZoneManifestSource`].
impl_enum_type!(
    InvZoneManifestSourceEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum InvZoneManifestSourceEnum;

    // Enum values
    Installinator => b"installinator"
    SledAgent => b"sled-agent"
);

/// Rows corresponding to the zone image resolver in `inv_sled_agent`.
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_sled_agent)]
pub struct InvZoneImageResolver {
    pub zone_manifest_boot_disk_path: String,
    pub zone_manifest_source: Option<InvZoneManifestSourceEnum>,
    pub zone_manifest_mupdate_id: Option<DbTypedUuid<MupdateKind>>,
    pub zone_manifest_boot_disk_error: Option<String>,

    pub mupdate_override_boot_disk_path: String,
    pub mupdate_override_id: Option<DbTypedUuid<MupdateOverrideKind>>,
    pub mupdate_override_boot_disk_error: Option<String>,
}

impl InvZoneImageResolver {
    /// Construct a new `InvZoneImageResolver`.
    pub fn new(inv: &ZoneImageResolverInventory) -> Self {
        let zone_manifest_boot_disk_path =
            inv.zone_manifest.boot_disk_path.clone().into();
        let (
            zone_manifest_source,
            zone_manifest_mupdate_id,
            zone_manifest_boot_disk_error,
        ) = match &inv.zone_manifest.boot_inventory {
            Ok(manifest) => match manifest.source {
                OmicronZoneManifestSource::Installinator { mupdate_id } => (
                    Some(InvZoneManifestSourceEnum::Installinator),
                    Some(mupdate_id.into()),
                    None,
                ),
                OmicronZoneManifestSource::SledAgent => {
                    (Some(InvZoneManifestSourceEnum::SledAgent), None, None)
                }
            },
            Err(error) => (None, None, Some(error.to_string())),
        };

        let mupdate_override_boot_disk_path =
            inv.mupdate_override.boot_disk_path.clone().into();
        let mupdate_override_id = inv
            .mupdate_override
            .boot_override
            .as_ref()
            .ok()
            .cloned()
            .flatten()
            .map(|inv| inv.mupdate_override_id.into());
        let mupdate_override_boot_disk_error =
            inv.mupdate_override.boot_override.as_ref().err().cloned();

        Self {
            zone_manifest_boot_disk_path,
            zone_manifest_source,
            zone_manifest_mupdate_id,
            zone_manifest_boot_disk_error,
            mupdate_override_boot_disk_path,
            mupdate_override_id,
            mupdate_override_boot_disk_error,
        }
    }

    /// Convert self into the inventory type.
    pub fn into_inventory(
        self,
        artifacts: Option<IdOrdMap<ZoneArtifactInventory>>,
        zone_manifest_non_boot: Option<IdOrdMap<ZoneManifestNonBootInventory>>,
        mupdate_override_non_boot: Option<
            IdOrdMap<MupdateOverrideNonBootInventory>,
        >,
    ) -> anyhow::Result<ZoneImageResolverInventory> {
        // Build up the ZoneManifestInventory struct.
        let zone_manifest = {
            let boot_inventory = if let Some(error) =
                self.zone_manifest_boot_disk_error
            {
                Err(error)
            } else {
                let source = match self.zone_manifest_source {
                    Some(InvZoneManifestSourceEnum::Installinator) => {
                        OmicronZoneManifestSource::Installinator {
                            mupdate_id: self
                                .zone_manifest_mupdate_id
                                .context(
                                    "illegal database state (CHECK constraint broken?!): \
                                     if the source is Installinator, then the \
                                     db schema guarantees that mupdate_id is Some",
                                )?
                                .into(),
                        }
                    }
                    Some(InvZoneManifestSourceEnum::SledAgent) => {
                        OmicronZoneManifestSource::SledAgent
                    }
                    None => {
                        bail!(
                            "illegal database state (CHECK constraint broken?!): \
                             if the source is None, then the db schema guarantees \
                             that there was an error",
                        )
                    }
                };

                Ok(ZoneManifestBootInventory {
                    source,
                    // Artifacts might really be None in case no zones were found.
                    // (This is unusual but permitted by the data model, so any
                    // checks around this should happen at a higher level.)
                    artifacts: artifacts.unwrap_or_default(),
                })
            };

            ZoneManifestInventory {
                boot_disk_path: self.zone_manifest_boot_disk_path.into(),
                boot_inventory,
                // This might be None if no non-boot disks were found.
                non_boot_status: zone_manifest_non_boot.unwrap_or_default(),
            }
        };

        // Build up the mupdate override struct.
        let boot_override = if let Some(error) =
            self.mupdate_override_boot_disk_error
        {
            Err(error)
        } else {
            let info = self.mupdate_override_id.map(|id| {
                MupdateOverrideBootInventory { mupdate_override_id: id.into() }
            });
            Ok(info)
        };

        let mupdate_override = MupdateOverrideInventory {
            boot_disk_path: self.mupdate_override_boot_disk_path.into(),
            boot_override,
            // This might be None if no non-boot disks were found.
            non_boot_status: mupdate_override_non_boot.unwrap_or_default(),
        };

        Ok(ZoneImageResolverInventory { zone_manifest, mupdate_override })
    }
}

/// Represents a zone file entry from the zone manifest on a sled.
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_zone_manifest_zone)]
pub struct InvZoneManifestZone {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub zone_file_name: String,
    pub path: String,
    pub expected_size: i64,
    pub expected_sha256: ArtifactHash,
    pub error: Option<String>,
}

impl InvZoneManifestZone {
    pub fn new(
        collection_id: CollectionUuid,
        sled_id: SledUuid,
        artifact: &ZoneArtifactInventory,
    ) -> Self {
        Self {
            inv_collection_id: collection_id.into(),
            sled_id: sled_id.into(),
            zone_file_name: artifact.file_name.clone(),
            path: artifact.path.clone().into(),
            expected_size: artifact.expected_size as i64,
            expected_sha256: artifact.expected_hash.into(),
            error: artifact.status.as_ref().err().cloned(),
        }
    }
}

impl From<InvZoneManifestZone> for ZoneArtifactInventory {
    fn from(row: InvZoneManifestZone) -> Self {
        Self {
            file_name: row.zone_file_name,
            path: row.path.into(),
            expected_size: row.expected_size as u64,
            expected_hash: row.expected_sha256.into(),
            status: match row.error {
                None => Ok(()),
                Some(error) => Err(error),
            },
        }
    }
}

/// Represents a non-boot zpool entry from the zone manifest on a sled.
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_zone_manifest_non_boot)]
pub struct InvZoneManifestNonBoot {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub non_boot_zpool_id: DbTypedUuid<InternalZpoolKind>,
    pub path: String,
    pub is_valid: bool,
    pub message: String,
}

impl InvZoneManifestNonBoot {
    pub fn new(
        collection_id: CollectionUuid,
        sled_id: SledUuid,
        non_boot: &ZoneManifestNonBootInventory,
    ) -> Self {
        Self {
            inv_collection_id: collection_id.into(),
            sled_id: sled_id.into(),
            non_boot_zpool_id: non_boot.zpool_id.into(),
            path: non_boot.path.clone().into(),
            is_valid: non_boot.is_valid,
            message: non_boot.message.clone(),
        }
    }
}

impl From<InvZoneManifestNonBoot> for ZoneManifestNonBootInventory {
    fn from(row: InvZoneManifestNonBoot) -> Self {
        Self {
            zpool_id: row.non_boot_zpool_id.into(),
            path: row.path.into(),
            is_valid: row.is_valid,
            message: row.message,
        }
    }
}

/// Represents a non-boot zpool entry from the mupdate override on a sled.
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_mupdate_override_non_boot)]
pub struct InvMupdateOverrideNonBoot {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub non_boot_zpool_id: DbTypedUuid<InternalZpoolKind>,
    pub path: String,
    pub is_valid: bool,
    pub message: String,
}

impl InvMupdateOverrideNonBoot {
    pub fn new(
        collection_id: CollectionUuid,
        sled_id: SledUuid,
        non_boot: &MupdateOverrideNonBootInventory,
    ) -> Self {
        Self {
            inv_collection_id: collection_id.into(),
            sled_id: sled_id.into(),
            non_boot_zpool_id: non_boot.zpool_id.into(),
            path: non_boot.path.clone().into(),
            is_valid: non_boot.is_valid,
            message: non_boot.message.clone(),
        }
    }
}

impl From<InvMupdateOverrideNonBoot> for MupdateOverrideNonBootInventory {
    fn from(row: InvMupdateOverrideNonBoot) -> Self {
        Self {
            zpool_id: row.non_boot_zpool_id.into(),
            path: row.path.into(),
            is_valid: row.is_valid,
            message: row.message,
        }
    }
}

/// See [`nexus_types::inventory::PhysicalDisk`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_physical_disk)]
pub struct InvPhysicalDisk {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub slot: i64,
    pub vendor: String,
    pub model: String,
    pub serial: String,
    pub variant: PhysicalDiskKind,
}

impl InvPhysicalDisk {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_id: SledUuid,
        disk: nexus_types::inventory::PhysicalDisk,
    ) -> Self {
        Self {
            inv_collection_id: inv_collection_id.into(),
            sled_id: sled_id.into(),
            slot: disk.slot,
            vendor: disk.identity.vendor,
            model: disk.identity.model,
            serial: disk.identity.serial,
            variant: disk.variant.into(),
        }
    }
}

#[derive(Clone, Debug, Error)]
pub enum InvNvmeDiskFirmwareError {
    #[error("active slot must be between 1 and 7, found `{0}`")]
    InvalidActiveSlot(u8),
    #[error("next active slot must be between 1 and 7, found `{0}`")]
    InvalidNextActiveSlot(u8),
    #[error("number of slots must be between 1 and 7, found `{0}`")]
    InvalidNumberOfSlots(u8),
    #[error("`{slots}` slots should match `{len}` firmware version entries")]
    SlotFirmwareVersionsLengthMismatch { slots: u8, len: usize },
    #[error("firmware version string `{0}` contains non ascii characters")]
    FirmwareVersionNotAscii(String),
    #[error("firmware version string `{0}` must be 8 bytes or less")]
    FirmwareVersionTooLong(String),
    #[error("active firmware at slot `{0}` maps to empty slot")]
    InvalidActiveSlotFirmware(u8),
    #[error("next active firmware at slot `{0}` maps to empty slot")]
    InvalidNextActiveSlotFirmware(u8),
}

/// See [`nexus_types::inventory::PhysicalDiskFirmware::Nvme`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_nvme_disk_firmware)]
pub struct InvNvmeDiskFirmware {
    inv_collection_id: DbTypedUuid<CollectionKind>,
    sled_id: DbTypedUuid<SledKind>,
    slot: i64,
    active_slot: SqlU8,
    next_active_slot: Option<SqlU8>,
    number_of_slots: SqlU8,
    slot1_is_read_only: bool,
    slot_firmware_versions: Vec<Option<String>>,
}

impl InvNvmeDiskFirmware {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_id: SledUuid,
        sled_slot: i64,
        firmware: &NvmeFirmware,
    ) -> Result<Self, InvNvmeDiskFirmwareError> {
        // NB: We first validate that the data given to us from an NVMe disk
        // actually makes sense. For the purposes of testing we ensure that
        // number of slots is validated first before any other field.

        // Valid NVMe slots are between 1 and 7.
        let valid_slot = 1..=7;
        if !valid_slot.contains(&firmware.number_of_slots) {
            return Err(InvNvmeDiskFirmwareError::InvalidNumberOfSlots(
                firmware.number_of_slots,
            ));
        }
        if usize::from(firmware.number_of_slots)
            != firmware.slot_firmware_versions.len()
        {
            return Err(
                InvNvmeDiskFirmwareError::SlotFirmwareVersionsLengthMismatch {
                    slots: firmware.number_of_slots,
                    len: firmware.slot_firmware_versions.len(),
                },
            );
        }
        if !valid_slot.contains(&firmware.active_slot) {
            return Err(InvNvmeDiskFirmwareError::InvalidActiveSlot(
                firmware.active_slot,
            ));
        }
        // active_slot maps to a populated version
        let Some(Some(_fw_string)) = firmware
            .slot_firmware_versions
            .get(usize::from(firmware.active_slot) - 1)
        else {
            return Err(InvNvmeDiskFirmwareError::InvalidActiveSlotFirmware(
                firmware.active_slot,
            ));
        };
        if let Some(next_active_slot) = firmware.next_active_slot {
            if !valid_slot.contains(&next_active_slot) {
                return Err(InvNvmeDiskFirmwareError::InvalidNextActiveSlot(
                    next_active_slot,
                ));
            }

            // next_active_slot maps to a populated version
            let Some(Some(_fw_string)) = firmware
                .slot_firmware_versions
                .get(usize::from(next_active_slot) - 1)
            else {
                return Err(
                    InvNvmeDiskFirmwareError::InvalidNextActiveSlotFirmware(
                        next_active_slot,
                    ),
                );
            };
        }
        // slot fw strings must be a max of 8 bytes and must be ascii characters
        for fw_string in firmware.slot_firmware_versions.iter().flatten() {
            if !fw_string.is_ascii() {
                return Err(InvNvmeDiskFirmwareError::FirmwareVersionNotAscii(
                    fw_string.clone(),
                ));
            }
            if fw_string.len() > 8 {
                return Err(InvNvmeDiskFirmwareError::FirmwareVersionTooLong(
                    fw_string.clone(),
                ));
            }
        }

        Ok(Self {
            inv_collection_id: inv_collection_id.into(),
            sled_id: sled_id.into(),
            slot: sled_slot,
            active_slot: firmware.active_slot.into(),
            next_active_slot: firmware.next_active_slot.map(|nas| nas.into()),
            number_of_slots: firmware.number_of_slots.into(),
            slot1_is_read_only: firmware.slot1_is_read_only,
            slot_firmware_versions: firmware.slot_firmware_versions.clone(),
        })
    }

    /// Attempt to read the current firmware version.
    pub fn current_version(&self) -> Option<&str> {
        match self.active_slot.0 {
            // be paranoid that we have a value within the NVMe spec
            slot @ 1..=7 => self
                .slot_firmware_versions
                .get(usize::from(slot) - 1)
                .and_then(|v| v.as_deref()),
            _ => None,
        }
    }

    /// Attempt to read the staged firmware version that will be active upon
    /// next device reset.
    pub fn next_version(&self) -> Option<&str> {
        match self.next_active_slot {
            // be paranoid that we have a value within the NVMe spec
            Some(slot) if slot.0 <= 7 && slot.0 >= 1 => self
                .slot_firmware_versions
                .get(usize::from(slot.0) - 1)
                .and_then(|v| v.as_deref()),
            _ => None,
        }
    }

    pub fn inv_collection_id(&self) -> DbTypedUuid<CollectionKind> {
        self.inv_collection_id
    }

    pub fn sled_id(&self) -> DbTypedUuid<SledKind> {
        self.sled_id
    }

    pub fn slot(&self) -> i64 {
        self.slot
    }

    pub fn number_of_slots(&self) -> SqlU8 {
        self.number_of_slots
    }

    pub fn active_slot(&self) -> SqlU8 {
        self.active_slot
    }

    pub fn next_active_slot(&self) -> Option<SqlU8> {
        self.next_active_slot
    }

    pub fn slot1_is_read_only(&self) -> bool {
        self.slot1_is_read_only
    }

    pub fn slot_firmware_versions(&self) -> &[Option<String>] {
        &self.slot_firmware_versions
    }
}

impl From<InvNvmeDiskFirmware>
    for nexus_types::inventory::PhysicalDiskFirmware
{
    fn from(
        nvme_firmware: InvNvmeDiskFirmware,
    ) -> nexus_types::inventory::PhysicalDiskFirmware {
        use nexus_types::inventory as nexus_inventory;

        nexus_inventory::PhysicalDiskFirmware::Nvme(
            nexus_inventory::NvmeFirmware {
                active_slot: nvme_firmware.active_slot.0,
                next_active_slot: nvme_firmware
                    .next_active_slot
                    .map(|nas| nas.0),
                number_of_slots: nvme_firmware.number_of_slots.0,
                slot1_is_read_only: nvme_firmware.slot1_is_read_only,
                slot_firmware_versions: nvme_firmware.slot_firmware_versions,
            },
        )
    }
}

/// See [`nexus_types::inventory::Zpool`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_zpool)]
pub struct InvZpool {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub time_collected: DateTime<Utc>,
    pub id: DbTypedUuid<ZpoolKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub total_size: ByteCount,
}

impl InvZpool {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_id: SledUuid,
        zpool: &nexus_types::inventory::Zpool,
    ) -> Self {
        Self {
            inv_collection_id: inv_collection_id.into(),
            time_collected: zpool.time_collected,
            id: zpool.id.into(),
            sled_id: sled_id.into(),
            total_size: zpool.total_size.into(),
        }
    }
}

impl From<InvZpool> for nexus_types::inventory::Zpool {
    fn from(pool: InvZpool) -> Self {
        Self {
            time_collected: pool.time_collected,
            id: pool.id.into(),
            total_size: *pool.total_size,
        }
    }
}

/// See [`nexus_types::inventory::Dataset`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_dataset)]
pub struct InvDataset {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub id: Option<DbTypedUuid<DatasetKind>>,
    pub name: String,
    pub available: ByteCount,
    pub used: ByteCount,
    pub quota: Option<ByteCount>,
    pub reservation: Option<ByteCount>,
    pub compression: String,
}

impl InvDataset {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_id: SledUuid,
        dataset: &nexus_types::inventory::Dataset,
    ) -> Self {
        Self {
            inv_collection_id: inv_collection_id.into(),
            sled_id: sled_id.into(),

            id: dataset.id.map(|id| id.into()),
            name: dataset.name.clone(),
            available: dataset.available.into(),
            used: dataset.used.into(),
            quota: dataset.quota.map(|q| q.into()),
            reservation: dataset.reservation.map(|r| r.into()),
            compression: dataset.compression.clone(),
        }
    }
}

impl From<InvDataset> for nexus_types::inventory::Dataset {
    fn from(dataset: InvDataset) -> Self {
        Self {
            id: dataset.id.map(|id| id.0),
            name: dataset.name,
            available: *dataset.available,
            used: *dataset.used,
            quota: dataset.quota.map(|q| *q),
            reservation: dataset.reservation.map(|r| *r),
            compression: dataset.compression,
        }
    }
}

/// Top-level information contained in an [`OmicronSledConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_omicron_sled_config)]
pub struct InvOmicronSledConfig {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub id: DbTypedUuid<OmicronSledConfigKind>,
    pub generation: Generation,
    pub remove_mupdate_override: Option<DbTypedUuid<MupdateOverrideKind>>,

    #[diesel(embed)]
    pub host_phase_2: DbHostPhase2DesiredSlots,
}

impl InvOmicronSledConfig {
    pub fn new(
        inv_collection_id: CollectionUuid,
        id: OmicronSledConfigUuid,
        generation: external::Generation,
        remove_mupdate_override: Option<MupdateOverrideUuid>,
        host_phase_2: HostPhase2DesiredSlots,
    ) -> Self {
        Self {
            inv_collection_id: inv_collection_id.into(),
            id: id.into(),
            generation: Generation(generation),
            remove_mupdate_override: remove_mupdate_override.map(From::from),
            host_phase_2: host_phase_2.into(),
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_omicron_sled_config)]
pub struct DbHostPhase2DesiredSlots {
    pub host_phase_2_desired_slot_a: Option<ArtifactHash>,
    pub host_phase_2_desired_slot_b: Option<ArtifactHash>,
}

impl From<HostPhase2DesiredSlots> for DbHostPhase2DesiredSlots {
    fn from(value: HostPhase2DesiredSlots) -> Self {
        let remap = |desired| match desired {
            HostPhase2DesiredContents::CurrentContents => None,
            HostPhase2DesiredContents::Artifact { hash } => {
                Some(ArtifactHash(hash))
            }
        };
        Self {
            host_phase_2_desired_slot_a: remap(value.slot_a),
            host_phase_2_desired_slot_b: remap(value.slot_b),
        }
    }
}

impl From<DbHostPhase2DesiredSlots> for HostPhase2DesiredSlots {
    fn from(value: DbHostPhase2DesiredSlots) -> Self {
        let remap = |maybe_artifact| match maybe_artifact {
            None => HostPhase2DesiredContents::CurrentContents,
            Some(ArtifactHash(hash)) => {
                HostPhase2DesiredContents::Artifact { hash }
            }
        };
        Self {
            slot_a: remap(value.host_phase_2_desired_slot_a),
            slot_b: remap(value.host_phase_2_desired_slot_b),
        }
    }
}

impl_enum_type!(
    ZoneTypeEnum:

    #[derive(Clone, Copy, Debug, Eq, AsExpression, FromSqlRow, PartialEq)]
    pub enum ZoneType;

    // Enum values
    BoundaryNtp => b"boundary_ntp"
    Clickhouse => b"clickhouse"
    ClickhouseKeeper => b"clickhouse_keeper"
    ClickhouseServer => b"clickhouse_server"
    CockroachDb => b"cockroach_db"
    Crucible => b"crucible"
    CruciblePantry => b"crucible_pantry"
    ExternalDns => b"external_dns"
    InternalDns => b"internal_dns"
    InternalNtp => b"internal_ntp"
    Nexus => b"nexus"
    Oximeter => b"oximeter"
);

impl From<ZoneType> for ServiceKind {
    fn from(zone_type: ZoneType) -> Self {
        match zone_type {
            ZoneType::BoundaryNtp | ZoneType::InternalNtp => Self::Ntp,
            ZoneType::Clickhouse => Self::Clickhouse,
            ZoneType::ClickhouseKeeper => Self::ClickhouseKeeper,
            ZoneType::ClickhouseServer => Self::ClickhouseServer,
            ZoneType::CockroachDb => Self::Cockroach,
            ZoneType::Crucible => Self::Crucible,
            ZoneType::CruciblePantry => Self::CruciblePantry,
            ZoneType::ExternalDns => Self::ExternalDns,
            ZoneType::InternalDns => Self::InternalDns,
            ZoneType::Nexus => Self::Nexus,
            ZoneType::Oximeter => Self::Oximeter,
        }
    }
}

impl From<ZoneType> for nexus_sled_agent_shared::inventory::ZoneKind {
    fn from(zone_type: ZoneType) -> Self {
        use nexus_sled_agent_shared::inventory::ZoneKind::*;

        match zone_type {
            ZoneType::BoundaryNtp => BoundaryNtp,
            ZoneType::Clickhouse => Clickhouse,
            ZoneType::ClickhouseKeeper => ClickhouseKeeper,
            ZoneType::ClickhouseServer => ClickhouseServer,
            ZoneType::CockroachDb => CockroachDb,
            ZoneType::Crucible => Crucible,
            ZoneType::CruciblePantry => CruciblePantry,
            ZoneType::ExternalDns => ExternalDns,
            ZoneType::InternalDns => InternalDns,
            ZoneType::InternalNtp => InternalNtp,
            ZoneType::Nexus => Nexus,
            ZoneType::Oximeter => Oximeter,
        }
    }
}

impl From<nexus_sled_agent_shared::inventory::ZoneKind> for ZoneType {
    fn from(zone_kind: nexus_sled_agent_shared::inventory::ZoneKind) -> Self {
        use nexus_sled_agent_shared::inventory::ZoneKind::*;

        match zone_kind {
            BoundaryNtp => ZoneType::BoundaryNtp,
            Clickhouse => ZoneType::Clickhouse,
            ClickhouseKeeper => ZoneType::ClickhouseKeeper,
            ClickhouseServer => ZoneType::ClickhouseServer,
            CockroachDb => ZoneType::CockroachDb,
            Crucible => ZoneType::Crucible,
            CruciblePantry => ZoneType::CruciblePantry,
            ExternalDns => ZoneType::ExternalDns,
            InternalDns => ZoneType::InternalDns,
            InternalNtp => ZoneType::InternalNtp,
            Nexus => ZoneType::Nexus,
            Oximeter => ZoneType::Oximeter,
        }
    }
}

/// See [`omicron_common::disk::OmicronPhysicalDiskConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_omicron_sled_config_disk)]
pub struct InvOmicronSledConfigDisk {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_config_id: DbTypedUuid<OmicronSledConfigKind>,
    pub id: DbTypedUuid<omicron_uuid_kinds::PhysicalDiskKind>,

    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub pool_id: DbTypedUuid<ZpoolKind>,
}

impl InvOmicronSledConfigDisk {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_config_id: OmicronSledConfigUuid,
        disk_config: OmicronPhysicalDiskConfig,
    ) -> Self {
        Self {
            inv_collection_id: inv_collection_id.into(),
            sled_config_id: sled_config_id.into(),
            id: disk_config.id.into(),
            vendor: disk_config.identity.vendor,
            serial: disk_config.identity.serial,
            model: disk_config.identity.model,
            pool_id: disk_config.pool_id.into(),
        }
    }
}

impl From<InvOmicronSledConfigDisk> for OmicronPhysicalDiskConfig {
    fn from(disk: InvOmicronSledConfigDisk) -> Self {
        Self {
            identity: DiskIdentity {
                vendor: disk.vendor,
                serial: disk.serial,
                model: disk.model,
            },
            id: disk.id.into(),
            pool_id: disk.pool_id.into(),
        }
    }
}

/// See [`omicron_common::disk::DatasetConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_omicron_sled_config_dataset)]
pub struct InvOmicronSledConfigDataset {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_config_id: DbTypedUuid<OmicronSledConfigKind>,
    pub id: DbTypedUuid<omicron_uuid_kinds::DatasetKind>,

    pub pool_id: DbTypedUuid<ZpoolKind>,
    pub kind: crate::DatasetKind,
    zone_name: Option<String>,

    pub quota: Option<ByteCount>,
    pub reservation: Option<ByteCount>,
    pub compression: String,
}

impl InvOmicronSledConfigDataset {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_config_id: OmicronSledConfigUuid,
        dataset_config: &DatasetConfig,
    ) -> Self {
        Self {
            inv_collection_id: inv_collection_id.into(),
            sled_config_id: sled_config_id.into(),
            id: dataset_config.id.into(),
            pool_id: dataset_config.name.pool().id().into(),
            kind: dataset_config.name.kind().into(),
            zone_name: dataset_config.name.kind().zone_name().map(String::from),
            quota: dataset_config.inner.quota.map(|q| q.into()),
            reservation: dataset_config.inner.reservation.map(|r| r.into()),
            compression: dataset_config.inner.compression.to_string(),
        }
    }
}

impl TryFrom<InvOmicronSledConfigDataset> for DatasetConfig {
    type Error = anyhow::Error;

    fn try_from(
        dataset: InvOmicronSledConfigDataset,
    ) -> Result<Self, Self::Error> {
        let pool = omicron_common::zpool_name::ZpoolName::new_external(
            dataset.pool_id.into(),
        );
        let kind =
            crate::DatasetKind::try_into_api(dataset.kind, dataset.zone_name)?;

        Ok(Self {
            id: dataset.id.into(),
            name: DatasetName::new(pool, kind),
            inner: omicron_common::disk::SharedDatasetConfig {
                quota: dataset.quota.map(|b| b.into()),
                reservation: dataset.reservation.map(|b| b.into()),
                compression: dataset.compression.parse()?,
            },
        })
    }
}

impl_enum_type!(
    InvZoneImageSourceEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum InvZoneImageSource;

    // Enum values
    InstallDataset => b"install_dataset"
    Artifact => b"artifact"
);

/// See [`nexus_sled_agent_shared::inventory::OmicronZoneConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_omicron_sled_config_zone)]
pub struct InvOmicronSledConfigZone {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_config_id: DbTypedUuid<OmicronSledConfigKind>,
    pub id: DbTypedUuid<OmicronZoneKind>,
    pub zone_type: ZoneType,
    pub primary_service_ip: ipv6::Ipv6Addr,
    pub primary_service_port: SqlU16,
    pub second_service_ip: Option<IpNetwork>,
    pub second_service_port: Option<SqlU16>,
    pub dataset_zpool_name: Option<String>,
    pub nic_id: Option<Uuid>,
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
    pub filesystem_pool: Option<DbTypedUuid<ZpoolKind>>,
    pub image_source: InvZoneImageSource,
    pub image_artifact_sha256: Option<ArtifactHash>,
    pub nexus_debug_port: Option<SqlU16>,
}

impl InvOmicronSledConfigZone {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_config_id: OmicronSledConfigUuid,
        zone: &OmicronZoneConfig,
    ) -> Result<InvOmicronSledConfigZone, anyhow::Error> {
        let (image_source, image_artifact_sha256) = match &zone.image_source {
            OmicronZoneImageSource::InstallDataset => {
                (InvZoneImageSource::InstallDataset, None)
            }
            OmicronZoneImageSource::Artifact { hash } => {
                (InvZoneImageSource::Artifact, Some(ArtifactHash(*hash)))
            }
        };

        // Create a dummy record to start, then fill in the rest
        // according to the zone type
        let mut inv_omicron_zone = InvOmicronSledConfigZone {
            // Fill in the known fields that don't require inspecting
            // `zone.zone_type`
            inv_collection_id: inv_collection_id.into(),
            sled_config_id: sled_config_id.into(),
            id: zone.id.into(),
            filesystem_pool: zone
                .filesystem_pool
                .as_ref()
                .map(|pool| pool.id().into()),
            zone_type: zone.zone_type.kind().into(),

            // Set the remainder of the fields to a default
            primary_service_ip: "::1"
                .parse::<std::net::Ipv6Addr>()
                .unwrap()
                .into(),
            primary_service_port: 0.into(),
            second_service_ip: None,
            second_service_port: None,
            dataset_zpool_name: None,
            nic_id: None,
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
            image_source,
            image_artifact_sha256,
            nexus_debug_port: None,
        };

        match &zone.zone_type {
            OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);

                // Set the zone specific fields
                let (first_port, last_port) = snat_cfg.port_range_raw();
                inv_omicron_zone.ntp_ntp_servers = Some(ntp_servers.clone());
                inv_omicron_zone.ntp_dns_servers = Some(
                    dns_servers
                        .into_iter()
                        .cloned()
                        .map(IpNetwork::from)
                        .collect(),
                );
                inv_omicron_zone.ntp_domain.clone_from(domain);
                inv_omicron_zone.snat_ip = Some(IpNetwork::from(snat_cfg.ip));
                inv_omicron_zone.snat_first_port =
                    Some(SqlU16::from(first_port));
                inv_omicron_zone.snat_last_port = Some(SqlU16::from(last_port));
                inv_omicron_zone.nic_id = Some(nic.id);
            }
            OmicronZoneType::Clickhouse { address, dataset } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
                inv_omicron_zone.set_zpool_name(dataset);
            }
            OmicronZoneType::ClickhouseKeeper { address, dataset } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
                inv_omicron_zone.set_zpool_name(dataset);
            }
            OmicronZoneType::ClickhouseServer { address, dataset } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
                inv_omicron_zone.set_zpool_name(dataset);
            }
            OmicronZoneType::CockroachDb { address, dataset } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
                inv_omicron_zone.set_zpool_name(dataset);
            }
            OmicronZoneType::Crucible { address, dataset } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
                inv_omicron_zone.set_zpool_name(dataset);
            }
            OmicronZoneType::CruciblePantry { address } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
            }
            OmicronZoneType::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic,
            } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(http_address);
                inv_omicron_zone.set_zpool_name(dataset);

                // Set the zone specific fields
                inv_omicron_zone.nic_id = Some(nic.id);
                inv_omicron_zone.second_service_ip =
                    Some(IpNetwork::from(dns_address.ip()));
                inv_omicron_zone.second_service_port =
                    Some(SqlU16::from(dns_address.port()));
            }
            OmicronZoneType::InternalDns {
                dataset,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(http_address);
                inv_omicron_zone.set_zpool_name(dataset);

                // Set the zone specific fields
                inv_omicron_zone.second_service_ip =
                    Some(IpNetwork::from(IpAddr::V6(*dns_address.ip())));
                inv_omicron_zone.second_service_port =
                    Some(SqlU16::from(dns_address.port()));

                inv_omicron_zone.dns_gz_address =
                    Some(ipv6::Ipv6Addr::from(gz_address));
                inv_omicron_zone.dns_gz_address_index =
                    Some(SqlU32::from(*gz_address_index));
            }
            OmicronZoneType::InternalNtp { address } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
            }
            OmicronZoneType::Nexus {
                internal_address,
                debug_port,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => {
                // Set the common fields
                inv_omicron_zone
                    .set_primary_service_ip_and_port(internal_address);

                // Set the zone specific fields
                inv_omicron_zone.nic_id = Some(nic.id);
                inv_omicron_zone.second_service_ip =
                    Some(IpNetwork::from(*external_ip));
                inv_omicron_zone.nexus_external_tls = Some(*external_tls);
                inv_omicron_zone.nexus_external_dns_servers = Some(
                    external_dns_servers
                        .iter()
                        .cloned()
                        .map(IpNetwork::from)
                        .collect(),
                );
                inv_omicron_zone.nexus_debug_port =
                    Some(SqlU16::from(*debug_port));
            }
            OmicronZoneType::Oximeter { address } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
            }
        }

        Ok(inv_omicron_zone)
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

    pub fn into_omicron_zone_config(
        self,
        nic_row: Option<InvOmicronSledConfigZoneNic>,
    ) -> Result<OmicronZoneConfig, anyhow::Error> {
        // Build up a set of common fields for our `OmicronZoneType`s
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
            self.nic_id,
            nic_row.map(Into::into),
        )?;

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
                OmicronZoneType::BoundaryNtp {
                    address: primary_address,
                    ntp_servers: ntp_servers?,
                    dns_servers: ntp_dns_servers?,
                    domain: self.ntp_domain,
                    nic: nic?,
                    snat_cfg,
                }
            }
            ZoneType::Clickhouse => OmicronZoneType::Clickhouse {
                address: primary_address,
                dataset: dataset?,
            },
            ZoneType::ClickhouseKeeper => OmicronZoneType::ClickhouseKeeper {
                address: primary_address,
                dataset: dataset?,
            },
            ZoneType::ClickhouseServer => OmicronZoneType::ClickhouseServer {
                address: primary_address,
                dataset: dataset?,
            },
            ZoneType::CockroachDb => OmicronZoneType::CockroachDb {
                address: primary_address,
                dataset: dataset?,
            },
            ZoneType::Crucible => OmicronZoneType::Crucible {
                address: primary_address,
                dataset: dataset?,
            },
            ZoneType::CruciblePantry => {
                OmicronZoneType::CruciblePantry { address: primary_address }
            }
            ZoneType::ExternalDns => OmicronZoneType::ExternalDns {
                dataset: dataset?,
                http_address: primary_address,
                dns_address: dns_address?,
                nic: nic?,
            },
            ZoneType::InternalDns => OmicronZoneType::InternalDns {
                dataset: dataset?,
                http_address: primary_address,
                dns_address: omicron_zone_config::to_internal_dns_address(
                    dns_address?,
                )?,
                gz_address: self.dns_gz_address.map(Into::into).ok_or_else(
                    || anyhow!("expected dns_gz_address, found none"),
                )?,
                gz_address_index: *self.dns_gz_address_index.ok_or_else(
                    || anyhow!("expected dns_gz_address_index, found none"),
                )?,
            },
            ZoneType::InternalNtp => {
                OmicronZoneType::InternalNtp { address: primary_address }
            }
            ZoneType::Nexus => OmicronZoneType::Nexus {
                internal_address: primary_address,
                debug_port: *self
                    .nexus_debug_port
                    .ok_or_else(|| anyhow!("expected 'nexus_debug_port'"))?,
                external_ip: self
                    .second_service_ip
                    .ok_or_else(|| anyhow!("expected second service IP"))?
                    .ip(),
                nic: nic?,
                external_tls: self
                    .nexus_external_tls
                    .ok_or_else(|| anyhow!("expected 'external_tls'"))?,
                external_dns_servers: self
                    .nexus_external_dns_servers
                    .ok_or_else(|| anyhow!("expected 'external_dns_servers'"))?
                    .into_iter()
                    .map(|i| i.ip())
                    .collect(),
            },
            ZoneType::Oximeter => {
                OmicronZoneType::Oximeter { address: primary_address }
            }
        };

        let image_source = match (self.image_source, self.image_artifact_sha256)
        {
            (InvZoneImageSource::InstallDataset, None) => {
                OmicronZoneImageSource::InstallDataset
            }
            (InvZoneImageSource::Artifact, Some(ArtifactHash(hash))) => {
                OmicronZoneImageSource::Artifact { hash }
            }
            (InvZoneImageSource::InstallDataset, Some(_))
            | (InvZoneImageSource::Artifact, None) => {
                bail!(
                    "invalid image source column combination: {:?}, {:?}",
                    self.image_source,
                    self.image_artifact_sha256
                )
            }
        };

        Ok(OmicronZoneConfig {
            id: self.id.into(),
            filesystem_pool: self
                .filesystem_pool
                .map(|id| ZpoolName::new_external(id.into())),
            zone_type,
            image_source,
        })
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_omicron_sled_config_zone_nic)]
pub struct InvOmicronSledConfigZoneNic {
    inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_config_id: DbTypedUuid<OmicronSledConfigKind>,
    pub id: Uuid,
    name: Name,
    ip: IpNetwork,
    mac: MacAddr,
    subnet: IpNetwork,
    vni: SqlU32,
    is_primary: bool,
    slot: SqlU8,
}

impl From<InvOmicronSledConfigZoneNic> for OmicronZoneNic {
    fn from(value: InvOmicronSledConfigZoneNic) -> Self {
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

impl InvOmicronSledConfigZoneNic {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_config_id: OmicronSledConfigUuid,
        zone: &OmicronZoneConfig,
    ) -> Result<Option<InvOmicronSledConfigZoneNic>, anyhow::Error> {
        let Some(nic) = zone.zone_type.service_vnic() else {
            return Ok(None);
        };
        let nic = OmicronZoneNic::new(zone.id, nic)?;
        Ok(Some(Self {
            inv_collection_id: inv_collection_id.into(),
            sled_config_id: sled_config_id.into(),
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

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_clickhouse_keeper_membership)]
pub struct InvClickhouseKeeperMembership {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub queried_keeper_id: i64,
    pub leader_committed_log_index: i64,
    pub raft_config: Vec<i64>,
}

impl TryFrom<InvClickhouseKeeperMembership>
    for ClickhouseKeeperClusterMembership
{
    type Error = anyhow::Error;

    fn try_from(value: InvClickhouseKeeperMembership) -> anyhow::Result<Self> {
        let err_msg = "clickhouse keeper ID is negative";
        let mut raft_config = BTreeSet::new();
        // We are not worried about duplicates here, as each
        // `clickhouse-admin-keeper` reports about its local, unique keeper.
        // This uniqueness is guaranteed by the blueprint generation mechanism.
        for id in value.raft_config {
            raft_config.insert(KeeperId(id.try_into().context(err_msg)?));
        }
        Ok(ClickhouseKeeperClusterMembership {
            queried_keeper: KeeperId(
                value.queried_keeper_id.try_into().context(err_msg)?,
            ),
            leader_committed_log_index: value
                .leader_committed_log_index
                .try_into()
                .context("log index is negative")?,
            raft_config,
        })
    }
}

impl InvClickhouseKeeperMembership {
    pub fn new(
        inv_collection_id: CollectionUuid,
        membership: ClickhouseKeeperClusterMembership,
    ) -> anyhow::Result<InvClickhouseKeeperMembership> {
        let err_msg = "clickhouse keeper ID > 2^63";
        let mut raft_config = Vec::with_capacity(membership.raft_config.len());
        for id in membership.raft_config {
            raft_config.push(id.0.try_into().context(err_msg)?);
        }
        Ok(InvClickhouseKeeperMembership {
            inv_collection_id: inv_collection_id.into(),
            queried_keeper_id: membership
                .queried_keeper
                .0
                .try_into()
                .context(err_msg)?,
            leader_committed_log_index: membership
                .leader_committed_log_index
                .try_into()
                .context("log index > 2^63")?,
            raft_config,
        })
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_cockroachdb_status)]
pub struct InvCockroachStatus {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub node_id: String,
    pub ranges_underreplicated: Option<i64>,
    pub liveness_live_nodes: Option<i64>,
}

impl InvCockroachStatus {
    pub fn new(
        inv_collection_id: CollectionUuid,
        node_id: cockroach_admin_types::NodeId,
        status: &CockroachStatus,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            inv_collection_id: inv_collection_id.into(),
            node_id: node_id.0,
            ranges_underreplicated: status
                .ranges_underreplicated
                .map(|n| i64::try_from(n))
                .transpose()
                .with_context(
                    || "Converting ranges_underreplicated from u64 to i64",
                )?,
            liveness_live_nodes: status
                .liveness_live_nodes
                .map(|n| i64::try_from(n))
                .transpose()
                .with_context(
                    || "Converting liveness_live_nodes from u64 to i64",
                )?,
        })
    }
}

impl TryFrom<InvCockroachStatus> for CockroachStatus {
    type Error = anyhow::Error;

    fn try_from(value: InvCockroachStatus) -> anyhow::Result<Self> {
        Ok(Self {
            ranges_underreplicated: value
                .ranges_underreplicated
                .map(|n| {
                    u64::try_from(n).with_context(|| {
                        format!("Failed to convert ranges_underreplicated ({n}) to u64")
                    })
                })
                .transpose()?,
            liveness_live_nodes: value
                .liveness_live_nodes
                .map(|n| {
                    u64::try_from(n).with_context(|| {
                        format!("Failed to convert liveness_live_nodes ({n}) to u64")
                    })
                })
                .transpose()?,
        })
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_ntp_timesync)]
pub struct InvNtpTimesync {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub zone_id: DbTypedUuid<OmicronZoneKind>,
    pub synced: bool,
}

impl InvNtpTimesync {
    pub fn new(
        inv_collection_id: CollectionUuid,
        timesync: &TimeSync,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            inv_collection_id: inv_collection_id.into(),
            zone_id: timesync.zone_id.into(),
            synced: timesync.synced,
        })
    }
}

impl From<InvNtpTimesync> for nexus_types::inventory::TimeSync {
    fn from(value: InvNtpTimesync) -> Self {
        Self { zone_id: value.zone_id.into(), synced: value.synced }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_internal_dns)]
pub struct InvInternalDns {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub zone_id: DbTypedUuid<OmicronZoneKind>,
    pub generation: Generation,
}

impl InvInternalDns {
    pub fn new(
        inv_collection_id: CollectionUuid,
        status: &InternalDnsGenerationStatus,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            inv_collection_id: inv_collection_id.into(),
            zone_id: status.zone_id.into(),
            generation: Generation(status.generation),
        })
    }
}

impl From<InvInternalDns> for InternalDnsGenerationStatus {
    fn from(value: InvInternalDns) -> Self {
        Self {
            zone_id: value.zone_id.into(),
            generation: value.generation.into(),
        }
    }
}

#[cfg(test)]
mod test {
    use nexus_types::inventory::NvmeFirmware;
    use omicron_uuid_kinds::{CollectionKind, SledUuid, TypedUuid};

    use crate::{InvNvmeDiskFirmware, InvNvmeDiskFirmwareError, typed_uuid};

    #[test]
    fn test_inv_nvme_disk_firmware() {
        let inv_collection_id: TypedUuid<CollectionKind> =
            typed_uuid::DbTypedUuid(TypedUuid::new_v4()).into();
        let sled_id: SledUuid = TypedUuid::new_v4();
        let slot = 1;

        // NB: We are testing these error cases with only one value that
        // is out of spec so that we don't have to worry about what order
        // `InvNvmeDiskFirmware::new` is validating fields in.  The only test
        // dependent on position is the number of slots test, but that is always
        // processed first in the implementation

        // Invalid active slot
        for i in [0u8, 8] {
            let firmware = &NvmeFirmware {
                active_slot: i,
                next_active_slot: None,
                number_of_slots: 1,
                slot1_is_read_only: true,
                slot_firmware_versions: vec![Some("firmware".to_string())],
            };
            let err = InvNvmeDiskFirmware::new(
                inv_collection_id,
                sled_id,
                slot,
                firmware,
            )
            .unwrap_err();
            assert!(matches!(
                err,
                InvNvmeDiskFirmwareError::InvalidActiveSlot(_)
            ));
        }

        // Invalid next active slot
        for i in [0u8, 8] {
            let firmware = &NvmeFirmware {
                active_slot: 1,
                next_active_slot: Some(i),
                number_of_slots: 2,
                slot1_is_read_only: true,
                slot_firmware_versions: vec![
                    Some("firmware".to_string()),
                    Some("firmware".to_string()),
                ],
            };
            let err = InvNvmeDiskFirmware::new(
                inv_collection_id,
                sled_id,
                slot,
                firmware,
            )
            .unwrap_err();
            assert!(matches!(
                err,
                InvNvmeDiskFirmwareError::InvalidNextActiveSlot(_)
            ));
        }

        // Invalid number of slots
        for i in [0u8, 8] {
            let firmware = &NvmeFirmware {
                active_slot: i,
                next_active_slot: None,
                number_of_slots: i,
                slot1_is_read_only: true,
                slot_firmware_versions: vec![
                    Some("firmware".to_string());
                    i as usize
                ],
            };
            let err = InvNvmeDiskFirmware::new(
                inv_collection_id,
                sled_id,
                slot,
                firmware,
            )
            .unwrap_err();
            assert!(matches!(
                err,
                InvNvmeDiskFirmwareError::InvalidNumberOfSlots(_)
            ));
        }

        // Mismatch between number of slots and firmware versions
        let firmware = &NvmeFirmware {
            active_slot: 1,
            next_active_slot: None,
            number_of_slots: 2,
            slot1_is_read_only: true,
            slot_firmware_versions: vec![Some("firmware".to_string())],
        };
        let err = InvNvmeDiskFirmware::new(
            inv_collection_id,
            sled_id,
            slot,
            firmware,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            InvNvmeDiskFirmwareError::SlotFirmwareVersionsLengthMismatch { .. }
        ));

        // Firmware version string contains non ascii characters
        let firmware = &NvmeFirmware {
            active_slot: 1,
            next_active_slot: None,
            number_of_slots: 1,
            slot1_is_read_only: true,
            slot_firmware_versions: vec![Some("💾".to_string())],
        };
        let err = InvNvmeDiskFirmware::new(
            inv_collection_id,
            sled_id,
            slot,
            firmware,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            InvNvmeDiskFirmwareError::FirmwareVersionNotAscii(_)
        ));

        // Firmware version string is too long
        let firmware = &NvmeFirmware {
            active_slot: 1,
            next_active_slot: None,
            number_of_slots: 1,
            slot1_is_read_only: true,
            slot_firmware_versions: vec![Some(
                "somereallylongstring".to_string(),
            )],
        };
        let err = InvNvmeDiskFirmware::new(
            inv_collection_id,
            sled_id,
            slot,
            firmware,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            InvNvmeDiskFirmwareError::FirmwareVersionTooLong(_)
        ));

        // Active firmware version slot is in the vec but is empty
        let firmware = &NvmeFirmware {
            active_slot: 1,
            next_active_slot: None,
            number_of_slots: 1,
            slot1_is_read_only: true,
            slot_firmware_versions: vec![None],
        };
        let err = InvNvmeDiskFirmware::new(
            inv_collection_id,
            sled_id,
            slot,
            firmware,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            InvNvmeDiskFirmwareError::InvalidActiveSlotFirmware(_)
        ));

        // Next active firmware version slot is in the vec but is empty
        let firmware = &NvmeFirmware {
            active_slot: 1,
            next_active_slot: Some(2),
            number_of_slots: 2,
            slot1_is_read_only: true,
            slot_firmware_versions: vec![Some("1234".to_string()), None],
        };
        let err = InvNvmeDiskFirmware::new(
            inv_collection_id,
            sled_id,
            slot,
            firmware,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            InvNvmeDiskFirmwareError::InvalidNextActiveSlotFirmware(_)
        ));

        // Actually construct a valid value
        let firmware = &NvmeFirmware {
            active_slot: 1,
            next_active_slot: Some(2),
            number_of_slots: 2,
            slot1_is_read_only: true,
            slot_firmware_versions: vec![
                Some("1234".to_string()),
                Some("4567".to_string()),
            ],
        };
        assert!(
            InvNvmeDiskFirmware::new(
                inv_collection_id,
                sled_id,
                slot,
                firmware,
            )
            .is_ok()
        )
    }
}
