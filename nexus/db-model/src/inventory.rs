// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::impl_enum_type;
use crate::schema::{
    hw_baseboard_id, inv_caboose, inv_collection, inv_collection_error,
    inv_root_of_trust, inv_service_processor, sw_caboose,
};
use chrono::DateTime;
use chrono::Utc;
use diesel::expression::AsExpression;
use nexus_types::inventory::{
    BaseboardId, Caboose, Collection, PowerState, RotSlot,
};
use uuid::Uuid;

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
        match c {
            nexus_types::inventory::CabooseWhich::SpSlot0 => {
                CabooseWhich::SpSlot0
            }
            nexus_types::inventory::CabooseWhich::SpSlot1 => {
                CabooseWhich::SpSlot1
            }
            nexus_types::inventory::CabooseWhich::RotSlotA => {
                CabooseWhich::RotSlotA
            }
            nexus_types::inventory::CabooseWhich::RotSlotB => {
                CabooseWhich::RotSlotB
            }
        }
    }
}

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

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_collection)]
pub struct InvCollection {
    pub id: Uuid,
    pub time_started: DateTime<Utc>,
    pub time_done: DateTime<Utc>,
    pub collector: String,
    pub comment: String,
}

impl<'a> From<&'a Collection> for InvCollection {
    fn from(c: &'a Collection) -> Self {
        InvCollection {
            id: c.id,
            time_started: c.time_started,
            time_done: c.time_done,
            collector: c.collector.clone(),
            comment: c.comment.clone(),
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = hw_baseboard_id)]
pub struct HwBaseboardId {
    pub id: Uuid,
    pub part_number: String,
    pub serial_number: String,
}

impl<'a> From<&'a BaseboardId> for HwBaseboardId {
    fn from(c: &'a BaseboardId) -> Self {
        HwBaseboardId {
            id: Uuid::new_v4(),
            part_number: c.part_number.clone(),
            serial_number: c.serial_number.clone(),
        }
    }
}

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

impl<'a> From<&'a Caboose> for SwCaboose {
    fn from(c: &'a Caboose) -> Self {
        SwCaboose {
            id: Uuid::new_v4(),
            board: c.board.clone(),
            git_commit: c.git_commit.clone(),
            name: c.name.clone(),
            version: c.version.clone(),
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_collection_error)]
pub struct InvCollectionError {
    pub inv_collection_id: Uuid,
    pub idx: i32,
    pub message: String,
}

impl InvCollectionError {
    pub fn new(inv_collection_id: Uuid, idx: i32, message: String) -> Self {
        InvCollectionError { inv_collection_id, idx, message }
    }
}

#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_service_processor)]
pub struct InvServiceProcessor {
    pub inv_collection_id: Uuid,
    pub hw_baseboard_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub sp_type: SpType,
    // XXX-dap newtype all around
    pub sp_slot: i32,

    // XXX-dap newtype all around
    // XXX-dap numeric types?
    pub baseboard_revision: i64,
    pub hubris_archive_id: String,
    pub power_state: HwPowerState,
}

#[derive(Queryable, Clone, Debug, Selectable)]
#[diesel(table_name = inv_root_of_trust)]
pub struct InvRootOfTrust {
    pub inv_collection_id: Uuid,
    pub hw_baseboard_id: Uuid,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub rot_slot_active: HwRotSlot,
    pub rot_slot_boot_pref_transient: Option<HwRotSlot>,
    pub rot_slot_boot_pref_persistent: HwRotSlot,
    pub rot_slot_boot_pref_persistent_pending: Option<HwRotSlot>,
    pub rot_slot_a_sha3_256: Option<String>,
    pub rot_slot_b_sha3_256: Option<String>,
}

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
