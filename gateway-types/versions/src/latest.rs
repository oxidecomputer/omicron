// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest versions of all published types.

pub mod caboose {
    pub use crate::v1::caboose::ComponentCabooseSlot;
    pub use crate::v1::caboose::SpComponentCaboose;
}

pub mod component {
    pub use crate::v1::component::PathSp;
    pub use crate::v1::component::PathSpComponent;
    pub use crate::v1::component::PathSpComponentFirmwareSlot;
    pub use crate::v1::component::PowerState;
    pub use crate::v1::component::SetComponentActiveSlotParams;
    pub use crate::v1::component::SpComponentFirmwareSlot;
    pub use crate::v1::component::SpComponentInfo;
    pub use crate::v1::component::SpComponentList;
    pub use crate::v1::component::SpComponentPresence;
    pub use crate::v1::component::SpIdentifier;
    pub use crate::v1::component::SpState;
    pub use crate::v1::component::SpType;
}

pub mod component_details {
    pub use crate::v1::component_details::LinkStatus;
    pub use crate::v1::component_details::Measurement;
    pub use crate::v1::component_details::MeasurementError;
    pub use crate::v1::component_details::MeasurementErrorCode;
    pub use crate::v1::component_details::MeasurementKind;
    pub use crate::v1::component_details::PacketCount;
    pub use crate::v1::component_details::PhyStatus;
    pub use crate::v1::component_details::PhyType;
    pub use crate::v1::component_details::PortConfig;
    pub use crate::v1::component_details::PortCounters;
    pub use crate::v1::component_details::PortDev;
    pub use crate::v1::component_details::PortMode;
    pub use crate::v1::component_details::PortSerdes;
    pub use crate::v1::component_details::PortStatus;
    pub use crate::v1::component_details::PortStatusError;
    pub use crate::v1::component_details::PortStatusErrorCode;
    pub use crate::v1::component_details::SpComponentDetails;
    pub use crate::v1::component_details::Speed;
    pub use crate::v1::component_details::UnsupportedComponentDetails;
}

pub mod host {
    pub use crate::v1::host::ComponentFirmwareHashStatus;
    pub use crate::v1::host::HostStartupOptions;
}

pub mod ignition {
    pub use crate::v1::ignition::IgnitionCommand;
    pub use crate::v1::ignition::PathSpIgnitionCommand;

    pub use crate::v2::ignition::SpIgnition;
    pub use crate::v2::ignition::SpIgnitionInfo;
    pub use crate::v2::ignition::SpIgnitionSystemType;
}

pub mod rot {
    pub use crate::v1::rot::GetCfpaParams;
    pub use crate::v1::rot::GetRotBootInfoParams;
    pub use crate::v1::rot::ImageVersion;
    pub use crate::v1::rot::RotCfpa;
    pub use crate::v1::rot::RotCfpaSlot;
    pub use crate::v1::rot::RotCmpa;
    pub use crate::v1::rot::RotImageDetails;
    pub use crate::v1::rot::RotImageError;
    pub use crate::v1::rot::RotSlot;
    pub use crate::v1::rot::RotState;
}

pub mod sensor {
    pub use crate::v1::sensor::PathSpSensorId;
    pub use crate::v1::sensor::SpSensorReading;
    pub use crate::v1::sensor::SpSensorReadingResult;
}

pub mod task_dump {
    pub use crate::v1::task_dump::PathSpTaskDumpIndex;
    pub use crate::v1::task_dump::TaskDump;
}

pub mod update {
    pub use crate::v1::update::ComponentUpdateIdSlot;
    pub use crate::v1::update::HostPhase2Progress;
    pub use crate::v1::update::HostPhase2RecoveryImageId;
    pub use crate::v1::update::InstallinatorImageId;
    pub use crate::v1::update::SpComponentResetError;
    pub use crate::v1::update::SpUpdateStatus;
    pub use crate::v1::update::UpdateAbortBody;
    pub use crate::v1::update::UpdatePreparationProgress;
}
