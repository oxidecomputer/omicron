// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::Deserialize;
use uuid::Uuid;

use crate::v1::component::SpIdentifier;
use crate::v1::ignition::IgnitionCommand;
use crate::v1::rot::RotCfpaSlot;

#[derive(Deserialize, JsonSchema)]
pub struct PathSp {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpSensorId {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// ID for the sensor on the SP.
    pub sensor_id: u32,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpComponent {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// ID for the component of the SP; this is the internal identifier used by
    /// the SP itself to identify its components.
    pub component: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpComponentFirmwareSlot {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// ID for the component of the SP; this is the internal identifier used by
    /// the SP itself to identify its components.
    pub component: String,
    /// Firmware slot of the component.
    pub firmware_slot: u16,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpTaskDumpIndex {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// The index of the task dump to be read.
    pub task_dump_index: u32,
}

#[derive(Deserialize, JsonSchema)]
pub struct ComponentCabooseSlot {
    /// The firmware slot to for which we want to request caboose information.
    pub firmware_slot: u16,
}

#[derive(Deserialize, JsonSchema)]
pub struct SetComponentActiveSlotParams {
    /// Persist this choice of active slot.
    pub persist: bool,
}

#[derive(Deserialize, JsonSchema)]
pub struct ComponentUpdateIdSlot {
    /// An identifier for this update.
    ///
    /// This ID applies to this single instance of the API call; it is not an
    /// ID of `image` itself. Multiple API calls with the same `image` should
    /// use different IDs.
    pub id: Uuid,
    /// The update slot to apply this image to. Supply 0 if the component only
    /// has one update slot.
    pub firmware_slot: u16,
}

#[derive(Deserialize, JsonSchema)]
pub struct UpdateAbortBody {
    /// The ID of the update to abort.
    ///
    /// If the SP is currently receiving an update with this ID, it will be
    /// aborted.
    ///
    /// If the SP is currently receiving an update with a different ID, the
    /// abort request will fail.
    ///
    /// If the SP is not currently receiving any update, the request to abort
    /// should succeed but will not have actually done anything.
    pub id: Uuid,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, JsonSchema,
)]
pub struct GetCfpaParams {
    pub slot: RotCfpaSlot,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, JsonSchema,
)]
pub struct GetRotBootInfoParams {
    pub version: u8,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpIgnitionCommand {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// Ignition command to perform on the targeted SP.
    pub command: IgnitionCommand,
}
