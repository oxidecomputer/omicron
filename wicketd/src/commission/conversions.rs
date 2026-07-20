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
    ct_inv::Caboose { version: caboose.version, board: caboose.board }
}

fn rot_info_to_ct(rot: RotInventory) -> ct_inv::RotInfo {
    ct_inv::RotInfo {
        active: rot.active,
        caboose_a: rot.caboose_a.map(caboose_to_ct),
        caboose_b: rot.caboose_b.map(caboose_to_ct),
        caboose_stage0: rot.caboose_stage0.flatten().map(caboose_to_ct),
        caboose_stage0next: rot.caboose_stage0next.flatten().map(caboose_to_ct),
    }
}

pub(crate) fn sp_info_to_ct(sp: SpInventory) -> ct_inv::SpInfo {
    let state = sp.state.as_ref();
    ct_inv::SpInfo {
        id: sp.id,
        serial_number: state.map(|s| s.serial_number.clone()),
        power_state: state.map(|s| s.power_state),
        ignition_present: sp.ignition.as_ref().map(SpIgnition::is_present),
        caboose_active: sp.caboose_active.map(caboose_to_ct),
        caboose_inactive: sp.caboose_inactive.map(caboose_to_ct),
        rot: sp.rot.map(rot_info_to_ct),
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
