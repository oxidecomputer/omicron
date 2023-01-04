// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    impl_enum_type,
    schema::{component_update, system_update, system_update_component_update},
    SemverVersion,
};
use db_macros::Asset;
use nexus_types::{external_api::views, identity::Asset};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Asset,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = system_update)]
pub struct SystemUpdate {
    #[diesel(embed)]
    identity: SystemUpdateIdentity,
    pub version: SemverVersion,
}

impl From<SystemUpdate> for views::SystemUpdate {
    fn from(system_update: SystemUpdate) -> Self {
        Self {
            identity: system_update.identity(),
            version: system_update.version.into(),
        }
    }
}

// TODO: more specific name than device_type, maybe update_device_type

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "device_type"))]
    pub struct DeviceTypeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = DeviceTypeEnum)]
    pub enum DeviceType;

    // Enum values
    Disk => b"disk"
);

impl From<DeviceType> for views::DeviceType {
    fn from(device_type: DeviceType) -> Self {
        match device_type {
            DeviceType::Disk => views::DeviceType::Disk,
        }
    }
}

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Asset,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = component_update)]
pub struct ComponentUpdate {
    #[diesel(embed)]
    identity: ComponentUpdateIdentity,
    pub version: SemverVersion,
    pub device_type: DeviceType,
    pub parent_id: Option<Uuid>,
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = system_update_component_update)]
pub struct SystemUpdateComponentUpdate {
    pub component_update_id: Uuid,
    pub system_update_id: Uuid,
}

impl From<ComponentUpdate> for views::ComponentUpdate {
    fn from(component_update: ComponentUpdate) -> Self {
        Self {
            identity: component_update.identity(),
            version: component_update.version.into(),
            device_type: component_update.device_type.into(),
            parent_id: component_update.parent_id,
        }
    }
}
