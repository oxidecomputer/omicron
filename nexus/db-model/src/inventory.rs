// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::impl_enum_type;
use crate::schema::{
    hw_baseboard_id, inv_caboose, inv_collection, inv_root_of_trust,
    inv_service_processor, sw_caboose,
};
use chrono::DateTime;
use chrono::Utc;
use db_macros::Asset;
use diesel::expression::AsExpression;
use nexus_types::identity::Asset;
use nexus_types::inventory::{BaseboardId, Collection, PowerState};
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "hw_power_state"))]
    pub struct HwPowerStateEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = HwRotSlotEnum)]
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
            id: Uuid::new_v4(),
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
