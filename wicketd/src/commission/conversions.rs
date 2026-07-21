// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Conversions between internal and commission types.

use bootstrap_agent_lockstep_types as bootstrap;
use wicket_common::inventory::{
    RotInventory, SpComponentCaboose, SpIgnition, SpInventory,
};
use wicketd_commission_types::inventory as ct_inv;
use wicketd_commission_types::rack_setup::{
    NewPasswordHash, RackOperationStatus, RssStepInfo,
};
use wicketd_commission_types::update::StartUpdateOptions;

fn caboose_to_ct(caboose: SpComponentCaboose) -> ct_inv::Caboose {
    let SpComponentCaboose {
        version,
        board,
        // The remaining fields are not projected by the commission API.
        git_commit: _,
        name: _,
        sign: _,
        epoch: _,
    } = caboose;
    ct_inv::Caboose { version, board }
}

fn slot_caboose_to_ct(
    caboose: Option<SpComponentCaboose>,
) -> ct_inv::SlotCaboose {
    match caboose {
        None => ct_inv::SlotCaboose::NotRead,
        Some(caboose) => {
            ct_inv::SlotCaboose::Read { caboose: caboose_to_ct(caboose) }
        }
    }
}

fn stage0_caboose_to_ct(
    caboose: Option<Option<SpComponentCaboose>>,
) -> ct_inv::Stage0Caboose {
    match caboose {
        None => ct_inv::Stage0Caboose::Unsupported,
        Some(None) => ct_inv::Stage0Caboose::NotRead,
        Some(Some(caboose)) => {
            ct_inv::Stage0Caboose::Read { caboose: caboose_to_ct(caboose) }
        }
    }
}

fn rot_info_to_ct(rot: RotInventory) -> ct_inv::RotInfo {
    let RotInventory {
        active,
        caboose_a,
        caboose_b,
        caboose_stage0,
        caboose_stage0next,
    } = rot;
    ct_inv::RotInfo {
        active,
        caboose_a: slot_caboose_to_ct(caboose_a),
        caboose_b: slot_caboose_to_ct(caboose_b),
        caboose_stage0: stage0_caboose_to_ct(caboose_stage0),
        caboose_stage0next: stage0_caboose_to_ct(caboose_stage0next),
    }
}

fn ignition_to_ct(ignition: SpIgnition) -> ct_inv::SpIgnitionInfo {
    match ignition {
        SpIgnition::Absent => ct_inv::SpIgnitionInfo::Absent,
        SpIgnition::Present {
            power,
            flt_a3,
            flt_a2,
            flt_rot,
            flt_sp,
            // The remaining fields are not projected by the commission API.
            id: _,
            ctrl_detect_0: _,
            ctrl_detect_1: _,
        } => ct_inv::SpIgnitionInfo::Present {
            power,
            faults: ct_inv::IgnitionFaults {
                a3: flt_a3,
                a2: flt_a2,
                rot: flt_rot,
                sp: flt_sp,
            },
        },
    }
}

pub(crate) fn sp_info_to_ct(sp: SpInventory) -> ct_inv::SpInfo {
    let SpInventory {
        id,
        ignition,
        state,
        caboose_active,
        caboose_inactive,
        rot,
        // The raw MGS component list is not projected by the commission API.
        components: _,
    } = sp;
    ct_inv::SpInfo {
        id,
        state: state.map(|s| ct_inv::SpStateInfo {
            serial_number: s.serial_number,
            power_state: s.power_state,
        }),
        ignition: ignition.map(ignition_to_ct),
        caboose_active: slot_caboose_to_ct(caboose_active),
        caboose_inactive: slot_caboose_to_ct(caboose_inactive),
        rot: rot.map(rot_info_to_ct),
    }
}

fn rss_step_to_ct(step: bootstrap::RssStep) -> RssStepInfo {
    RssStepInfo {
        // index() is 0-based, so add 1 to get the 1-based step index.
        step: step.index() as u32 + 1,
        total_steps: step.max_step() as u32,
        description: step.description().to_string(),
    }
}

pub(crate) fn rack_operation_status_to_ct(
    status: bootstrap::RackOperationStatus,
) -> RackOperationStatus {
    use bootstrap::RackOperationStatus as B;
    match status {
        B::Initializing { id, step } => {
            RackOperationStatus::Initializing { id, step: rss_step_to_ct(step) }
        }
        B::Initialized { id } => RackOperationStatus::Initialized { id },
        B::InitializationFailed { id, message } => {
            RackOperationStatus::InitializationFailed { id, message }
        }
        B::InitializationPanicked { id } => {
            RackOperationStatus::InitializationPanicked { id }
        }
        B::Resetting { id } => RackOperationStatus::Resetting { id },
        B::Uninitialized { reset_id } => {
            RackOperationStatus::Uninitialized { reset_id }
        }
        B::ResetFailed { id, message } => {
            RackOperationStatus::ResetFailed { id, message }
        }
        B::ResetPanicked { id } => RackOperationStatus::ResetPanicked { id },
    }
}

pub(crate) fn start_update_options_to_internal(
    options: StartUpdateOptions,
) -> wicket_common::rack_update::StartUpdateOptions {
    let StartUpdateOptions {
        skip_rot_bootloader_version_check,
        skip_rot_version_check,
        skip_sp_version_check,
    } = options;
    // The commission API deliberately does not expose the test-only knobs
    // provided by the internal update API.
    wicket_common::rack_update::StartUpdateOptions {
        test_error: None,
        test_step_seconds: None,
        test_simulate_rot_bootloader_result: None,
        test_simulate_rot_result: None,
        test_simulate_sp_result: None,
        skip_rot_bootloader_version_check,
        skip_rot_version_check,
        skip_sp_version_check,
    }
}

pub(crate) fn password_hash_to_internal(
    hash: NewPasswordHash,
) -> Result<omicron_passwords::NewPasswordHash, String> {
    hash.0.parse::<omicron_passwords::NewPasswordHash>().map_err(|err| {
        format!("invalid recovery password hash (PHC string): {err}")
    })
}
