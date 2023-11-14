// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for representing the hardware/software inventory in the database

use crate::schema::{
    hw_baseboard_id, inv_caboose, inv_collection, inv_collection_error,
    inv_root_of_trust, inv_root_of_trust_page, inv_service_processor,
    sw_caboose, sw_root_of_trust_page,
};
use crate::{impl_enum_type, SqlU16, SqlU32};
use chrono::DateTime;
use chrono::Utc;
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::expression::AsExpression;
use diesel::pg::Pg;
use diesel::serialize::ToSql;
use diesel::{serialize, sql_types};
use nexus_types::inventory::{
    BaseboardId, Caboose, Collection, PowerState, RotPage, RotSlot,
};
use uuid::Uuid;

// See [`nexus_types::inventory::PowerState`].
impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "hw_power_state"))]
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
    #[diesel(postgres_type(name = "hw_rot_slot"))]
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
    #[diesel(postgres_type(name = "caboose_which"))]
    pub struct CabooseWhichEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = CabooseWhichEnum)]
    pub enum CabooseWhich;

    // Enum values
    SpSlot0 => b"sp_slot_0"
    SpSlot1 => b"sp_slot_1"
    RotSlotA => b"rot_slot_A"
    RotSlotB => b"rot_slot_B"
);

impl From<nexus_types::inventory::CabooseWhich> for CabooseWhich {
    fn from(c: nexus_types::inventory::CabooseWhich) -> Self {
        use nexus_types::inventory as nexus_inventory;
        match c {
            nexus_inventory::CabooseWhich::SpSlot0 => CabooseWhich::SpSlot0,
            nexus_inventory::CabooseWhich::SpSlot1 => CabooseWhich::SpSlot1,
            nexus_inventory::CabooseWhich::RotSlotA => CabooseWhich::RotSlotA,
            nexus_inventory::CabooseWhich::RotSlotB => CabooseWhich::RotSlotB,
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
        }
    }
}

// See [`nexus_types::inventory::RotPageWhich`].
impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "root_of_trust_page_which"))]
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

// See [`nexus_types::inventory::SpType`].
impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "sp_type"))]
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
    pub id: Uuid,
    pub time_started: DateTime<Utc>,
    pub time_done: DateTime<Utc>,
    pub collector: String,
}

impl<'a> From<&'a Collection> for InvCollection {
    fn from(c: &'a Collection) -> Self {
        InvCollection {
            id: c.id,
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
    pub inv_collection_id: Uuid,
    pub idx: SqlU16,
    pub message: String,
}

impl InvCollectionError {
    pub fn new(inv_collection_id: Uuid, idx: u16, message: String) -> Self {
        InvCollectionError {
            inv_collection_id,
            idx: SqlU16::from(idx),
            message,
        }
    }
}

/// See [`nexus_types::inventory::ServiceProcessor`].
#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_service_processor)]
pub struct InvServiceProcessor {
    pub inv_collection_id: Uuid,
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
