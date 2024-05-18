// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::ipv6;
use crate::schema::region_replacement_step;
use crate::SqlU16;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::net::SocketAddrV6;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "region_replacement_step_type", schema = "public"))]
    pub struct RegionReplacementStepTypeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = RegionReplacementStepTypeEnum)]
    pub enum RegionReplacementStepType;

    // What is driving the repair forward?
    Propolis => b"propolis"
    Pantry => b"pantry"
);

/// Database representation of a Region replacement repair step
#[derive(
    Queryable,
    Insertable,
    Debug,
    Clone,
    Selectable,
    Serialize,
    Deserialize,
    PartialEq,
)]
#[diesel(table_name = region_replacement_step)]
pub struct RegionReplacementStep {
    pub replacement_id: Uuid,

    pub step_time: DateTime<Utc>,

    pub step_type: RegionReplacementStepType,

    pub step_associated_instance_id: Option<Uuid>,
    pub step_associated_vmm_id: Option<Uuid>,

    pub step_associated_pantry_ip: Option<ipv6::Ipv6Addr>,
    pub step_associated_pantry_port: Option<SqlU16>,
    pub step_associated_pantry_job_id: Option<Uuid>,
}

impl RegionReplacementStep {
    pub fn instance_and_vmm_ids(&self) -> Option<(Uuid, Uuid)> {
        if self.step_type != RegionReplacementStepType::Propolis {
            return None;
        }

        let instance_id = self.step_associated_instance_id?;
        let vmm_id = self.step_associated_vmm_id?;

        Some((instance_id, vmm_id))
    }

    pub fn pantry_address(&self) -> Option<SocketAddrV6> {
        if self.step_type != RegionReplacementStepType::Pantry {
            return None;
        }

        let ip = self.step_associated_pantry_ip?;
        let port = self.step_associated_pantry_port?;

        Some(SocketAddrV6::new(*ip, *port, 0, 0))
    }
}
