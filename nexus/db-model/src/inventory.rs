// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for representing the hardware/software inventory in the database

use crate::omicron_zone_config::{self, OmicronZoneNic};
use crate::schema::{
    hw_baseboard_id, inv_caboose, inv_collection, inv_collection_error,
    inv_omicron_zone, inv_omicron_zone_nic, inv_physical_disk,
    inv_root_of_trust, inv_root_of_trust_page, inv_service_processor,
    inv_sled_agent, inv_sled_omicron_zones, inv_zpool, sw_caboose,
    sw_root_of_trust_page,
};
use crate::typed_uuid::DbTypedUuid;
use crate::PhysicalDiskKind;
use crate::{
    impl_enum_type, ipv6, ByteCount, Generation, MacAddr, Name, ServiceKind,
    SqlU16, SqlU32, SqlU8,
};
use anyhow::{anyhow, bail, Context, Result};
use chrono::DateTime;
use chrono::Utc;
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::expression::AsExpression;
use diesel::pg::Pg;
use diesel::serialize::ToSql;
use diesel::{serialize, sql_types};
use ipnetwork::IpNetwork;
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_sled_agent_shared::inventory::{
    OmicronZoneConfig, OmicronZoneType, OmicronZonesConfig,
};
use nexus_types::inventory::{
    BaseboardId, Caboose, Collection, PowerState, RotPage, RotSlot,
};
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::CollectionKind;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolKind;
use omicron_uuid_kinds::ZpoolUuid;
use std::net::{IpAddr, SocketAddrV6};
use uuid::Uuid;

// See [`nexus_types::inventory::PowerState`].
impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "hw_power_state", schema = "public"))]
    pub struct HwPowerStateEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = HwPowerStateEnum)]
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
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "hw_rot_slot", schema = "public"))]
    pub struct HwRotSlotEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = HwRotSlotEnum)]
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

// See [`nexus_types::inventory::CabooseWhich`].
impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "caboose_which", schema = "public"))]
    pub struct CabooseWhichEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = CabooseWhichEnum)]
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
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "root_of_trust_page_which", schema = "public"))]
    pub struct RotPageWhichEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = RotPageWhichEnum)]
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
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "rot_image_error", schema = "public"))]
    pub struct RotImageErrorEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = RotImageErrorEnum)]
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
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "sp_type", schema = "public"))]
    pub struct SpTypeEnum;

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
    #[diesel(sql_type = SpTypeEnum)]
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
}

impl From<Caboose> for SwCaboose {
    fn from(c: Caboose) -> Self {
        SwCaboose {
            id: Uuid::new_v4(),
            board: c.board,
            git_commit: c.git_commit,
            name: c.name,
            version: c.version,
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
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "sled_role"))]
    pub struct SledRoleEnum;

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
    #[diesel(sql_type = SledRoleEnum)]
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
    pub reservoir_size: ByteCount,
}

impl InvSledAgent {
    pub fn new_without_baseboard(
        collection_id: CollectionUuid,
        sled_agent: &nexus_types::inventory::SledAgent,
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
                reservoir_size: ByteCount::from(sled_agent.reservoir_size),
            })
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

impl From<InvPhysicalDisk> for nexus_types::inventory::PhysicalDisk {
    fn from(disk: InvPhysicalDisk) -> Self {
        Self {
            identity: omicron_common::disk::DiskIdentity {
                vendor: disk.vendor,
                serial: disk.serial,
                model: disk.model,
            },
            variant: disk.variant.into(),
            slot: disk.slot,
        }
    }
}

/// See [`nexus_types::inventory::Zpool`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_zpool)]
pub struct InvZpool {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub time_collected: DateTime<Utc>,
    pub id: Uuid,
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
            id: zpool.id.into_untyped_uuid(),
            sled_id: sled_id.into(),
            total_size: zpool.total_size.into(),
        }
    }
}

impl From<InvZpool> for nexus_types::inventory::Zpool {
    fn from(pool: InvZpool) -> Self {
        Self {
            time_collected: pool.time_collected,
            id: ZpoolUuid::from_untyped_uuid(pool.id),
            total_size: *pool.total_size,
        }
    }
}

/// See [`nexus_types::inventory::OmicronZonesFound`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_sled_omicron_zones)]
pub struct InvSledOmicronZones {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub time_collected: DateTime<Utc>,
    pub source: String,
    pub sled_id: DbTypedUuid<SledKind>,
    pub generation: Generation,
}

impl InvSledOmicronZones {
    pub fn new(
        inv_collection_id: CollectionUuid,
        zones_found: &nexus_types::inventory::OmicronZonesFound,
    ) -> InvSledOmicronZones {
        InvSledOmicronZones {
            inv_collection_id: inv_collection_id.into(),
            time_collected: zones_found.time_collected,
            source: zones_found.source.clone(),
            sled_id: zones_found.sled_id.into(),
            generation: Generation(zones_found.zones.generation),
        }
    }

    pub fn into_uninit_zones_found(
        self,
    ) -> nexus_types::inventory::OmicronZonesFound {
        nexus_types::inventory::OmicronZonesFound {
            time_collected: self.time_collected,
            source: self.source,
            sled_id: self.sled_id.into(),
            zones: OmicronZonesConfig {
                generation: *self.generation,
                zones: Vec::new(),
            },
        }
    }
}

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "zone_type"))]
    pub struct ZoneTypeEnum;

    #[derive(Clone, Copy, Debug, Eq, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = ZoneTypeEnum)]
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

/// See [`nexus_sled_agent_shared::inventory::OmicronZoneConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_omicron_zone)]
pub struct InvOmicronZone {
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub id: Uuid,
    pub underlay_address: ipv6::Ipv6Addr,
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
}

impl InvOmicronZone {
    pub fn new(
        inv_collection_id: CollectionUuid,
        sled_id: SledUuid,
        zone: &OmicronZoneConfig,
    ) -> Result<InvOmicronZone, anyhow::Error> {
        // Create a dummy record to start, then fill in the rest
        // according to the zone type
        let mut inv_omicron_zone = InvOmicronZone {
            // Fill in the known fields that don't require inspecting
            // `zone.zone_type`
            inv_collection_id: inv_collection_id.into(),
            sled_id: sled_id.into(),
            id: zone.id,
            underlay_address: zone.underlay_address.into(),
            filesystem_pool: zone
                .filesystem_pool
                .as_ref()
                .map(|pool| pool.id().into()),

            // Set the remainder of the fields to a default
            zone_type: ZoneType::BoundaryNtp,
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
                inv_omicron_zone.zone_type = ZoneType::BoundaryNtp;

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
                inv_omicron_zone.zone_type = ZoneType::Clickhouse;
            }
            OmicronZoneType::ClickhouseKeeper { address, dataset } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
                inv_omicron_zone.set_zpool_name(dataset);
                inv_omicron_zone.zone_type = ZoneType::ClickhouseKeeper;
            }
            OmicronZoneType::ClickhouseServer { address, dataset } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
                inv_omicron_zone.set_zpool_name(dataset);
                inv_omicron_zone.zone_type = ZoneType::ClickhouseServer;
            }
            OmicronZoneType::CockroachDb { address, dataset } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
                inv_omicron_zone.set_zpool_name(dataset);
                inv_omicron_zone.zone_type = ZoneType::CockroachDb;
            }
            OmicronZoneType::Crucible { address, dataset } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
                inv_omicron_zone.set_zpool_name(dataset);
                inv_omicron_zone.zone_type = ZoneType::Crucible;
            }
            OmicronZoneType::CruciblePantry { address } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
                inv_omicron_zone.zone_type = ZoneType::CruciblePantry;
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
                inv_omicron_zone.zone_type = ZoneType::ExternalDns;

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
                inv_omicron_zone.zone_type = ZoneType::InternalDns;

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
            OmicronZoneType::InternalNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
            } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
                inv_omicron_zone.zone_type = ZoneType::InternalNtp;

                // Set the zone specific fields
                inv_omicron_zone.ntp_ntp_servers = Some(ntp_servers.clone());
                inv_omicron_zone.ntp_dns_servers = Some(
                    dns_servers.iter().cloned().map(IpNetwork::from).collect(),
                );
                inv_omicron_zone.ntp_domain.clone_from(domain);
            }
            OmicronZoneType::Nexus {
                internal_address,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => {
                // Set the common fields
                inv_omicron_zone
                    .set_primary_service_ip_and_port(internal_address);
                inv_omicron_zone.zone_type = ZoneType::Nexus;

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
            }
            OmicronZoneType::Oximeter { address } => {
                // Set the common fields
                inv_omicron_zone.set_primary_service_ip_and_port(address);
                inv_omicron_zone.zone_type = ZoneType::Oximeter;
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
        nic_row: Option<InvOmicronZoneNic>,
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
            self.id,
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
            ZoneType::InternalNtp => OmicronZoneType::InternalNtp {
                address: primary_address,
                ntp_servers: ntp_servers?,
                dns_servers: ntp_dns_servers?,
                domain: self.ntp_domain,
            },
            ZoneType::Nexus => OmicronZoneType::Nexus {
                internal_address: primary_address,
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

        Ok(OmicronZoneConfig {
            id: self.id,
            underlay_address: self.underlay_address.into(),
            filesystem_pool: self
                .filesystem_pool
                .map(|id| ZpoolName::new_external(id.into())),
            zone_type,
        })
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = inv_omicron_zone_nic)]
pub struct InvOmicronZoneNic {
    inv_collection_id: DbTypedUuid<CollectionKind>,
    pub id: Uuid,
    name: Name,
    ip: IpNetwork,
    mac: MacAddr,
    subnet: IpNetwork,
    vni: SqlU32,
    is_primary: bool,
    slot: SqlU8,
}

impl From<InvOmicronZoneNic> for OmicronZoneNic {
    fn from(value: InvOmicronZoneNic) -> Self {
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

impl InvOmicronZoneNic {
    pub fn new(
        inv_collection_id: CollectionUuid,
        zone: &OmicronZoneConfig,
    ) -> Result<Option<InvOmicronZoneNic>, anyhow::Error> {
        let Some(nic) = zone.zone_type.service_vnic() else {
            return Ok(None);
        };
        let nic = OmicronZoneNic::new(zone.id, nic)?;
        Ok(Some(Self {
            inv_collection_id: inv_collection_id.into(),
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
