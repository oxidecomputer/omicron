// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    impl_enum_type,
    schema::{
        component_update, system_update, system_update_component_update,
        updateable_component,
    },
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
    pub identity: SystemUpdateIdentity,
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

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "updateable_component_type"))]
    pub struct UpdateableComponentTypeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = UpdateableComponentTypeEnum)]
    pub enum UpdateableComponentType;

    BootloaderForRot => b"bootloader_for_rot"
    BootloaderForSp => b"bootloader_for_sp"
    BootloaderForHostProc => b"bootloader_for_host_proc"
    HubrisForPscRot => b"hubris_for_psc_rot"
    HubrisForPscSp => b"hubris_for_psc_sp"
    HubrisForSidecarRot => b"hubris_for_sidecar_rot"
    HubrisForSidecarSp => b"hubris_for_sidecar_sp"
    HubrisForGimletRot => b"hubris_for_gimlet_rot"
    HubrisForGimletSp => b"hubris_for_gimlet_sp"
    HeliosHostPhase1 => b"helios_host_phase_1"
    HeliosHostPhase2 => b"helios_host_phase_2"
    HostOmicron => b"host_omicron"
);

impl From<UpdateableComponentType> for views::UpdateableComponentType {
    fn from(component_type: UpdateableComponentType) -> Self {
        match component_type {
            UpdateableComponentType::BootloaderForRot => Self::BootloaderForRot,
            UpdateableComponentType::BootloaderForSp => Self::BootloaderForSp,
            UpdateableComponentType::BootloaderForHostProc => {
                Self::BootloaderForHostProc
            }
            UpdateableComponentType::HubrisForPscRot => Self::HubrisForPscRot,
            UpdateableComponentType::HubrisForPscSp => Self::HubrisForPscSp,
            UpdateableComponentType::HubrisForSidecarRot => {
                Self::HubrisForSidecarRot
            }
            UpdateableComponentType::HubrisForSidecarSp => {
                Self::HubrisForSidecarSp
            }
            UpdateableComponentType::HubrisForGimletRot => {
                Self::HubrisForGimletRot
            }
            UpdateableComponentType::HubrisForGimletSp => {
                Self::HubrisForGimletSp
            }
            UpdateableComponentType::HeliosHostPhase1 => Self::HeliosHostPhase1,
            UpdateableComponentType::HeliosHostPhase2 => Self::HeliosHostPhase2,
            UpdateableComponentType::HostOmicron => Self::HostOmicron,
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
    pub component_type: UpdateableComponentType,
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
            component_type: component_update.component_type.into(),
            parent_id: component_update.parent_id,
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
#[diesel(table_name = updateable_component)]
pub struct UpdateableComponent {
    #[diesel(embed)]
    identity: UpdateableComponentIdentity,
    pub device_id: String,
    pub component_type: UpdateableComponentType,
    pub version: SemverVersion,
    // pub status: VersionStatus,
    /// ID of the parent component, e.g., the sled a disk belongs to. Value will
    /// be `None` for top-level components whose "parent" is the rack.
    pub parent_id: Option<Uuid>,
}

impl From<UpdateableComponent> for views::UpdateableComponent {
    fn from(component: UpdateableComponent) -> Self {
        Self {
            identity: component.identity(),
            device_id: component.device_id,
            component_type: component.component_type.into(),
            version: component.version.into(),
            parent_id: component.parent_id,
        }
    }
}
