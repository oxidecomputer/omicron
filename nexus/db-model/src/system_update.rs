// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    impl_enum_type,
    schema::{
        component_update, system_update, system_update_component_update,
        update_deployment, updateable_component,
    },
    SemverVersion,
};
use db_macros::Asset;
use nexus_types::{
    external_api::{params, shared, views},
    identity::Asset,
};
use omicron_common::api::external;
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
    /// Semver version string with 0-padding on the numeric parts to make it
    /// DB-sortable. See `to_sortable_string` on `SemverVersion`
    pub version_sort: String,
}

impl SystemUpdate {
    /// Can fail if version numbers are too high.
    pub fn new(
        version: external::SemverVersion,
    ) -> Result<Self, external::Error> {
        let db_version = SemverVersion(version);
        Ok(Self {
            identity: SystemUpdateIdentity::new(Uuid::new_v4()),
            version: db_version.clone(),
            version_sort: db_version.to_sortable_string()?,
        })
    }
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
    #[diesel(postgres_type(name = "update_status"))]
    pub struct UpdateStatusEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = UpdateStatusEnum)]
    pub enum UpdateStatus;

    Updating => b"updating"
    Steady => b"steady"
);

impl From<UpdateStatus> for views::UpdateStatus {
    fn from(status: UpdateStatus) -> Self {
        match status {
            UpdateStatus::Updating => Self::Updating,
            UpdateStatus::Steady => Self::Steady,
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

impl From<shared::UpdateableComponentType> for UpdateableComponentType {
    fn from(component_type: shared::UpdateableComponentType) -> Self {
        match component_type {
            shared::UpdateableComponentType::BootloaderForRot => {
                UpdateableComponentType::BootloaderForRot
            }
            shared::UpdateableComponentType::BootloaderForSp => {
                UpdateableComponentType::BootloaderForSp
            }
            shared::UpdateableComponentType::BootloaderForHostProc => {
                UpdateableComponentType::BootloaderForHostProc
            }
            shared::UpdateableComponentType::HubrisForPscRot => {
                UpdateableComponentType::HubrisForPscRot
            }
            shared::UpdateableComponentType::HubrisForPscSp => {
                UpdateableComponentType::HubrisForPscSp
            }
            shared::UpdateableComponentType::HubrisForSidecarRot => {
                UpdateableComponentType::HubrisForSidecarRot
            }
            shared::UpdateableComponentType::HubrisForSidecarSp => {
                UpdateableComponentType::HubrisForSidecarSp
            }
            shared::UpdateableComponentType::HubrisForGimletRot => {
                UpdateableComponentType::HubrisForGimletRot
            }
            shared::UpdateableComponentType::HubrisForGimletSp => {
                UpdateableComponentType::HubrisForGimletSp
            }
            shared::UpdateableComponentType::HeliosHostPhase1 => {
                UpdateableComponentType::HeliosHostPhase1
            }
            shared::UpdateableComponentType::HeliosHostPhase2 => {
                UpdateableComponentType::HeliosHostPhase2
            }
            shared::UpdateableComponentType::HostOmicron => {
                UpdateableComponentType::HostOmicron
            }
        }
    }
}

impl Into<shared::UpdateableComponentType> for UpdateableComponentType {
    fn into(self) -> shared::UpdateableComponentType {
        match self {
            UpdateableComponentType::BootloaderForRot => {
                shared::UpdateableComponentType::BootloaderForRot
            }
            UpdateableComponentType::BootloaderForSp => {
                shared::UpdateableComponentType::BootloaderForSp
            }
            UpdateableComponentType::BootloaderForHostProc => {
                shared::UpdateableComponentType::BootloaderForHostProc
            }
            UpdateableComponentType::HubrisForPscRot => {
                shared::UpdateableComponentType::HubrisForPscRot
            }
            UpdateableComponentType::HubrisForPscSp => {
                shared::UpdateableComponentType::HubrisForPscSp
            }
            UpdateableComponentType::HubrisForSidecarRot => {
                shared::UpdateableComponentType::HubrisForSidecarRot
            }
            UpdateableComponentType::HubrisForSidecarSp => {
                shared::UpdateableComponentType::HubrisForSidecarSp
            }
            UpdateableComponentType::HubrisForGimletRot => {
                shared::UpdateableComponentType::HubrisForGimletRot
            }
            UpdateableComponentType::HubrisForGimletSp => {
                shared::UpdateableComponentType::HubrisForGimletSp
            }
            UpdateableComponentType::HeliosHostPhase1 => {
                shared::UpdateableComponentType::HeliosHostPhase1
            }
            UpdateableComponentType::HeliosHostPhase2 => {
                shared::UpdateableComponentType::HeliosHostPhase2
            }
            UpdateableComponentType::HostOmicron => {
                shared::UpdateableComponentType::HostOmicron
            }
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
    pub identity: ComponentUpdateIdentity,
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

// TODO: As mentioned in the "open questions" section of the PR description,
// I've determined this model is insufficient because it does not allow me to
// represent nodes like PSC or Host below (example tree taken from RFD 334),
// which are parents of updateable components but which are not themselves
// directly associated with any update artifacts. Displaying this tree in full
// seems pretty important for the UI, so I'm going to rethink this model a bit.
// It's probably not a drastic change. Might be as simple as making the artifact
// field nullable.

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
    pub identity: UpdateableComponentIdentity,
    pub device_id: String,
    pub component_type: UpdateableComponentType,
    pub version: SemverVersion,
    /// Semver version string with 0-padding on the numeric parts to make it
    /// DB-sortable. See `to_sortable_string` on `SemverVersion`
    pub version_sort: String,
    pub status: UpdateStatus,
    /// ID of the parent component, e.g., the sled a disk belongs to. Value will
    /// be `None` for top-level components whose "parent" is the rack.
    pub parent_id: Option<Uuid>,
    // TODO: point to the actual update artifact
}

impl TryFrom<params::UpdateableComponentCreate> for UpdateableComponent {
    type Error = external::Error;

    fn try_from(
        create: params::UpdateableComponentCreate,
    ) -> Result<Self, Self::Error> {
        let version = SemverVersion(create.version);
        Ok(Self {
            identity: UpdateableComponentIdentity::new(Uuid::new_v4()),
            version: version.clone(),
            version_sort: version.to_sortable_string()?,
            component_type: create.component_type.into(),
            parent_id: create.parent_id,
            device_id: create.device_id,
            status: UpdateStatus::Steady,
        })
    }
}

impl From<UpdateableComponent> for views::UpdateableComponent {
    fn from(component: UpdateableComponent) -> Self {
        Self {
            identity: component.identity(),
            device_id: component.device_id,
            component_type: component.component_type.into(),
            version: component.version.into(),
            parent_id: component.parent_id,
            status: component.status.into(),
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
#[diesel(table_name = update_deployment)]
pub struct UpdateDeployment {
    #[diesel(embed)]
    pub identity: UpdateDeploymentIdentity,
    pub version: SemverVersion,
    pub status: UpdateStatus,
}

impl From<UpdateDeployment> for views::UpdateDeployment {
    fn from(deployment: UpdateDeployment) -> Self {
        Self {
            identity: deployment.identity(),
            version: deployment.version.into(),
            status: deployment.status.into(),
        }
    }
}
