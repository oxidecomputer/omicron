// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Conversions between externally-defined types and HTTP / JsonSchema types.

use super::HostStartupOptions;
use super::IgnitionCommand;
use super::ImageVersion;
use super::InstallinatorImageId;
use super::PowerState;
use super::RotImageDetails;
use super::RotSlot;
use super::RotState;
use super::SpComponentInfo;
use super::SpComponentList;
use super::SpComponentPresence;
use super::SpIdentifier;
use super::SpIgnition;
use super::SpIgnitionSystemType;
use super::SpSensorReading;
use super::SpSensorReadingResult;
use super::SpState;
use super::SpType;
use super::SpUpdateStatus;
use super::UpdatePreparationProgress;
use dropshot::HttpError;
use gateway_messages::SpComponent;
use gateway_messages::StartupOptions;
use gateway_messages::UpdateStatus;
use std::str;

// wrap `SpComponent::try_from(&str)` into a usable form for dropshot endpoints
pub(super) fn component_from_str(s: &str) -> Result<SpComponent, HttpError> {
    SpComponent::try_from(s).map_err(|_| {
        HttpError::for_bad_request(
            Some("InvalidSpComponent".to_string()),
            "invalid SP component name".to_string(),
        )
    })
}

impl From<gateway_messages::SensorReading> for SpSensorReading {
    fn from(value: gateway_messages::SensorReading) -> Self {
        Self {
            timestamp: value.timestamp,
            result: match value.value {
                Ok(value) => SpSensorReadingResult::Success { value },
                Err(err) => err.into(),
            },
        }
    }
}

impl From<gateway_messages::SensorDataMissing> for SpSensorReadingResult {
    fn from(value: gateway_messages::SensorDataMissing) -> Self {
        use gateway_messages::SensorDataMissing;
        match value {
            SensorDataMissing::DeviceOff => Self::DeviceOff,
            SensorDataMissing::DeviceError => Self::DeviceError,
            SensorDataMissing::DeviceNotPresent => Self::DeviceNotPresent,
            SensorDataMissing::DeviceUnavailable => Self::DeviceUnavailable,
            SensorDataMissing::DeviceTimeout => Self::DeviceTimeout,
        }
    }
}

impl From<UpdateStatus> for SpUpdateStatus {
    fn from(status: UpdateStatus) -> Self {
        match status {
            UpdateStatus::None => Self::None,
            UpdateStatus::Preparing(status) => Self::Preparing {
                id: status.id.into(),
                progress: status.progress.map(Into::into),
            },
            UpdateStatus::SpUpdateAuxFlashChckScan {
                id, total_size, ..
            } => Self::InProgress {
                id: id.into(),
                bytes_received: 0,
                total_bytes: total_size,
            },
            UpdateStatus::InProgress(status) => Self::InProgress {
                id: status.id.into(),
                bytes_received: status.bytes_received,
                total_bytes: status.total_size,
            },
            UpdateStatus::Complete(id) => Self::Complete { id: id.into() },
            UpdateStatus::Aborted(id) => Self::Aborted { id: id.into() },
            UpdateStatus::Failed { id, code } => {
                Self::Failed { id: id.into(), code }
            }
            UpdateStatus::RotError { id, error } => {
                Self::RotError { id: id.into(), message: format!("{error:?}") }
            }
        }
    }
}

impl From<gateway_messages::UpdatePreparationProgress>
    for UpdatePreparationProgress
{
    fn from(progress: gateway_messages::UpdatePreparationProgress) -> Self {
        Self { current: progress.current, total: progress.total }
    }
}

impl From<gateway_messages::PowerState> for PowerState {
    fn from(power_state: gateway_messages::PowerState) -> Self {
        match power_state {
            gateway_messages::PowerState::A0 => Self::A0,
            gateway_messages::PowerState::A1 => Self::A1,
            gateway_messages::PowerState::A2 => Self::A2,
        }
    }
}

impl From<PowerState> for gateway_messages::PowerState {
    fn from(power_state: PowerState) -> gateway_messages::PowerState {
        match power_state {
            PowerState::A0 => gateway_messages::PowerState::A0,
            PowerState::A1 => gateway_messages::PowerState::A1,
            PowerState::A2 => gateway_messages::PowerState::A2,
        }
    }
}

impl From<gateway_messages::ImageVersion> for ImageVersion {
    fn from(v: gateway_messages::ImageVersion) -> Self {
        Self { epoch: v.epoch, version: v.version }
    }
}

// We expect serial and model numbers to be ASCII and 0-padded: find the first 0
// byte and convert to a string. If that fails, hexlify the entire slice.
fn stringify_byte_string(bytes: &[u8]) -> String {
    let first_zero = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());

    str::from_utf8(&bytes[..first_zero])
        .map(|s| s.to_string())
        .unwrap_or_else(|_err| hex::encode(bytes))
}

impl From<gateway_messages::SpStateV1> for SpState {
    fn from(state: gateway_messages::SpStateV1) -> Self {
        Self {
            serial_number: stringify_byte_string(&state.serial_number),
            model: stringify_byte_string(&state.model),
            revision: state.revision,
            hubris_archive_id: hex::encode(state.hubris_archive_id),
            base_mac_address: state.base_mac_address,
            power_state: PowerState::from(state.power_state),
            rot: RotState::from(state.rot),
        }
    }
}

impl From<gateway_messages::SpStateV2> for SpState {
    fn from(state: gateway_messages::SpStateV2) -> Self {
        Self {
            serial_number: stringify_byte_string(&state.serial_number),
            model: stringify_byte_string(&state.model),
            revision: state.revision,
            hubris_archive_id: hex::encode(state.hubris_archive_id),
            base_mac_address: state.base_mac_address,
            power_state: PowerState::from(state.power_state),
            rot: RotState::from(state.rot),
        }
    }
}

impl From<gateway_sp_comms::VersionedSpState> for SpState {
    fn from(value: gateway_sp_comms::VersionedSpState) -> Self {
        match value {
            gateway_sp_comms::VersionedSpState::V1(s) => Self::from(s),
            gateway_sp_comms::VersionedSpState::V2(s) => Self::from(s),
        }
    }
}

impl From<Result<gateway_messages::RotState, gateway_messages::RotError>>
    for RotState
{
    fn from(
        result: Result<gateway_messages::RotState, gateway_messages::RotError>,
    ) -> Self {
        match result {
            Ok(state) => {
                let boot_state = state.rot_updates.boot_state;
                Self::Enabled {
                    active: boot_state.active.into(),
                    slot_a_sha3_256_digest: boot_state
                        .slot_a
                        .map(|details| hex::encode(details.digest)),
                    slot_b_sha3_256_digest: boot_state
                        .slot_b
                        .map(|details| hex::encode(details.digest)),
                    // RotState(V1) didn't have the following fields, so we make
                    // it up as best we can. This RoT version is pre-shipping
                    // and should only exist on (not updated recently) test
                    // systems.
                    persistent_boot_preference: boot_state.active.into(),
                    pending_persistent_boot_preference: None,
                    transient_boot_preference: None,
                }
            }
            Err(err) => Self::CommunicationFailed { message: err.to_string() },
        }
    }
}

impl From<Result<gateway_messages::RotStateV2, gateway_messages::RotError>>
    for RotState
{
    fn from(
        result: Result<
            gateway_messages::RotStateV2,
            gateway_messages::RotError,
        >,
    ) -> Self {
        match result {
            Ok(state) => Self::Enabled {
                active: state.active.into(),
                persistent_boot_preference: state
                    .persistent_boot_preference
                    .into(),
                pending_persistent_boot_preference: state
                    .pending_persistent_boot_preference
                    .map(Into::into),
                transient_boot_preference: state
                    .transient_boot_preference
                    .map(Into::into),
                slot_a_sha3_256_digest: state
                    .slot_a_sha3_256_digest
                    .map(hex::encode),
                slot_b_sha3_256_digest: state
                    .slot_b_sha3_256_digest
                    .map(hex::encode),
            },
            Err(err) => Self::CommunicationFailed { message: err.to_string() },
        }
    }
}

impl From<gateway_messages::RotSlotId> for RotSlot {
    fn from(slot: gateway_messages::RotSlotId) -> Self {
        match slot {
            gateway_messages::RotSlotId::A => Self::A,
            gateway_messages::RotSlotId::B => Self::B,
        }
    }
}

impl From<gateway_messages::RotImageDetails> for RotImageDetails {
    fn from(details: gateway_messages::RotImageDetails) -> Self {
        Self {
            digest: hex::encode(details.digest),
            version: details.version.into(),
        }
    }
}

impl From<gateway_messages::IgnitionState> for SpIgnition {
    fn from(state: gateway_messages::IgnitionState) -> Self {
        use gateway_messages::ignition::SystemPowerState;

        if let Some(target_state) = state.target {
            Self::Present {
                id: target_state.system_type.into(),
                power: matches!(
                    target_state.power_state,
                    SystemPowerState::On | SystemPowerState::PoweringOn
                ),
                ctrl_detect_0: target_state.controller0_present,
                ctrl_detect_1: target_state.controller1_present,
                flt_a3: target_state.faults.power_a3,
                flt_a2: target_state.faults.power_a2,
                flt_rot: target_state.faults.rot,
                flt_sp: target_state.faults.sp,
            }
        } else {
            Self::Absent
        }
    }
}

impl From<gateway_messages::ignition::SystemType> for SpIgnitionSystemType {
    fn from(st: gateway_messages::ignition::SystemType) -> Self {
        use gateway_messages::ignition::SystemType;
        match st {
            SystemType::Gimlet => Self::Gimlet,
            SystemType::Sidecar => Self::Sidecar,
            SystemType::Psc => Self::Psc,
            SystemType::Unknown(id) => Self::Unknown { id },
        }
    }
}

impl From<IgnitionCommand> for gateway_messages::IgnitionCommand {
    fn from(cmd: IgnitionCommand) -> Self {
        match cmd {
            IgnitionCommand::PowerOn => {
                gateway_messages::IgnitionCommand::PowerOn
            }
            IgnitionCommand::PowerOff => {
                gateway_messages::IgnitionCommand::PowerOff
            }
            IgnitionCommand::PowerReset => {
                gateway_messages::IgnitionCommand::PowerReset
            }
        }
    }
}

impl From<SpType> for crate::management_switch::SpType {
    fn from(typ: SpType) -> Self {
        match typ {
            SpType::Sled => Self::Sled,
            SpType::Power => Self::Power,
            SpType::Switch => Self::Switch,
        }
    }
}

impl From<crate::management_switch::SpType> for SpType {
    fn from(typ: crate::management_switch::SpType) -> Self {
        match typ {
            crate::management_switch::SpType::Sled => Self::Sled,
            crate::management_switch::SpType::Power => Self::Power,
            crate::management_switch::SpType::Switch => Self::Switch,
        }
    }
}

impl From<SpIdentifier> for crate::management_switch::SpIdentifier {
    fn from(id: SpIdentifier) -> Self {
        Self {
            typ: id.typ.into(),
            // id.slot may come from an untrusted source, but usize >= 32 bits
            // on any platform that will run this code, so unwrap is fine
            slot: usize::try_from(id.slot).unwrap(),
        }
    }
}

impl From<crate::management_switch::SpIdentifier> for SpIdentifier {
    fn from(id: crate::management_switch::SpIdentifier) -> Self {
        Self {
            typ: id.typ.into(),
            // id.slot comes from a trusted source (crate::management_switch)
            // and will not exceed u32::MAX
            slot: u32::try_from(id.slot).unwrap(),
        }
    }
}

impl From<gateway_messages::DevicePresence> for SpComponentPresence {
    fn from(p: gateway_messages::DevicePresence) -> Self {
        match p {
            gateway_messages::DevicePresence::Present => Self::Present,
            gateway_messages::DevicePresence::NotPresent => Self::NotPresent,
            gateway_messages::DevicePresence::Failed => Self::Failed,
            gateway_messages::DevicePresence::Unavailable => Self::Unavailable,
            gateway_messages::DevicePresence::Timeout => Self::Timeout,
            gateway_messages::DevicePresence::Error => Self::Error,
        }
    }
}

impl From<gateway_sp_comms::SpDevice> for SpComponentInfo {
    fn from(d: gateway_sp_comms::SpDevice) -> Self {
        Self {
            component: d.component.as_str().unwrap_or("???").to_string(),
            device: d.device,
            serial_number: None, // TODO populate when SP provides it
            description: d.description,
            capabilities: d.capabilities.bits(),
            presence: d.presence.into(),
        }
    }
}

impl From<gateway_sp_comms::SpInventory> for SpComponentList {
    fn from(inv: gateway_sp_comms::SpInventory) -> Self {
        Self { components: inv.devices.into_iter().map(Into::into).collect() }
    }
}

impl From<HostStartupOptions> for StartupOptions {
    fn from(mgs_opt: HostStartupOptions) -> Self {
        let mut opt = StartupOptions::empty();
        opt.set(
            StartupOptions::PHASE2_RECOVERY_MODE,
            mgs_opt.phase2_recovery_mode,
        );
        opt.set(StartupOptions::STARTUP_KBM, mgs_opt.kbm);
        opt.set(StartupOptions::STARTUP_BOOTRD, mgs_opt.bootrd);
        opt.set(StartupOptions::STARTUP_PROM, mgs_opt.prom);
        opt.set(StartupOptions::STARTUP_KMDB, mgs_opt.kmdb);
        opt.set(StartupOptions::STARTUP_KMDB_BOOT, mgs_opt.kmdb_boot);
        opt.set(StartupOptions::STARTUP_BOOT_RAMDISK, mgs_opt.boot_ramdisk);
        opt.set(StartupOptions::STARTUP_BOOT_NET, mgs_opt.boot_net);
        opt.set(StartupOptions::STARTUP_VERBOSE, mgs_opt.verbose);
        opt
    }
}

impl From<StartupOptions> for HostStartupOptions {
    fn from(opt: StartupOptions) -> Self {
        Self {
            phase2_recovery_mode: opt
                .contains(StartupOptions::PHASE2_RECOVERY_MODE),
            kbm: opt.contains(StartupOptions::STARTUP_KBM),
            bootrd: opt.contains(StartupOptions::STARTUP_BOOTRD),
            prom: opt.contains(StartupOptions::STARTUP_PROM),
            kmdb: opt.contains(StartupOptions::STARTUP_KMDB),
            kmdb_boot: opt.contains(StartupOptions::STARTUP_KMDB_BOOT),
            boot_ramdisk: opt.contains(StartupOptions::STARTUP_BOOT_RAMDISK),
            boot_net: opt.contains(StartupOptions::STARTUP_BOOT_NET),
            verbose: opt.contains(StartupOptions::STARTUP_VERBOSE),
        }
    }
}

impl From<InstallinatorImageId> for ipcc::InstallinatorImageId {
    fn from(id: InstallinatorImageId) -> Self {
        Self {
            update_id: id.update_id,
            host_phase_2: id.host_phase_2,
            control_plane: id.control_plane,
        }
    }
}
