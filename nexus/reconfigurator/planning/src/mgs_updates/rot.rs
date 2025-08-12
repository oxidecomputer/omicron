// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for making choices about RoT updates

use super::MgsUpdateStatus;
use super::mgs_update_status_inactive_versions;

use gateway_types::rot::RotSlot;
use nexus_types::deployment::ExpectedActiveRotSlot;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::Collection;
use omicron_common::api::external::TufRepoDescription;
use slog::{debug, warn};
use std::sync::Arc;
use tufaceous_artifact::ArtifactKind;
use tufaceous_artifact::ArtifactVersion;

/// RoT state that gets checked against preconditions in a `PendingMgsUpdate`
pub struct RotUpdateState {
    pub active_slot: ExpectedActiveRotSlot,
    pub persistent_boot_preference: RotSlot,
    pub pending_persistent_boot_preference: Option<RotSlot>,
    pub transient_boot_preference: Option<RotSlot>,
}

pub fn mgs_update_status_rot(
    desired_version: &ArtifactVersion,
    expected: RotUpdateState,
    found: RotUpdateState,
    expected_inactive_version: &ExpectedVersion,
    found_inactive_version: Option<&str>,
) -> MgsUpdateStatus {
    let RotUpdateState {
        active_slot: found_active_slot,
        persistent_boot_preference: found_persistent_boot_preference,
        pending_persistent_boot_preference:
            found_pending_persistent_boot_preference,
        transient_boot_preference: found_transient_boot_preference,
    } = found;

    let RotUpdateState {
        active_slot: expected_active_slot,
        persistent_boot_preference: expected_persistent_boot_preference,
        pending_persistent_boot_preference:
            expected_pending_persistent_boot_preference,
        transient_boot_preference: expected_transient_boot_preference,
    } = expected;

    if &found_active_slot.version() == desired_version {
        // If we find the desired version in the active slot, we're done.
        return MgsUpdateStatus::Done;
    }

    // The update hasn't completed.
    //
    // Check to make sure the contents of the active slot, persistent boot
    // preference, pending persistent boot preference, and transient boot
    // preference are still what they were when we configured this update.
    // If not, then this update cannot proceed as currently configured.
    if found_active_slot.version() != expected_active_slot.version()
        || found_persistent_boot_preference
            != expected_persistent_boot_preference
        || found_pending_persistent_boot_preference
            != expected_pending_persistent_boot_preference
        || found_transient_boot_preference != expected_transient_boot_preference
    {
        return MgsUpdateStatus::Impossible;
    }

    // If there is a mismatch between the found persistent boot preference
    // and the found active slot then this means a failed update. We cannot
    // proceed.
    // It will fail its precondition check.
    // Transient boot preference is not in use yet, if we find that it is set we
    // should not proceed. Once https://github.com/oxidecomputer/hubris/pull/2050
    // is implemented, we should revist this check
    if found_persistent_boot_preference != found_active_slot.slot
        || found_transient_boot_preference.is_some()
        || expected_transient_boot_preference.is_some()
    {
        return MgsUpdateStatus::Impossible;
    }

    // Similarly, check the contents of the inactive slot to determine if it
    // still matches what we saw when we configured this update.  If not, then
    // this update cannot proceed as currently configured.  It will fail its
    // precondition check.
    mgs_update_status_inactive_versions(
        found_inactive_version,
        expected_inactive_version,
    )
}

/// Determine if the given baseboard needs an RoT update and, if so, returns it.
pub fn try_make_update_rot(
    log: &slog::Logger,
    baseboard_id: &Arc<BaseboardId>,
    inventory: &Collection,
    current_artifacts: &TufRepoDescription,
) -> Option<PendingMgsUpdate> {
    let Some(sp_info) = inventory.sps.get(baseboard_id) else {
        warn!(
            log,
            "cannot configure RoT update for board \
             (missing SP info from inventory)";
            baseboard_id
        );
        return None;
    };

    let Some(rot_state) = inventory.rots.get(baseboard_id) else {
        warn!(
            log,
            "cannot configure RoT update for board \
             (missing RoT state from inventory)";
            baseboard_id
        );
        return None;
    };

    let active_slot = rot_state.active_slot;

    let Some(active_caboose) = inventory
        .caboose_for(CabooseWhich::from_rot_slot(active_slot), baseboard_id)
    else {
        warn!(
            log,
            "cannot configure RoT update for board \
             (missing active slot {active_slot} caboose from inventory)";
            baseboard_id,
        );
        return None;
    };

    let Ok(expected_active_version) = active_caboose.caboose.version.parse()
    else {
        warn!(
            log,
            "cannot configure RoT update for board \
             (cannot parse current active version as an ArtifactVersion)";
            baseboard_id,
            "found_version" => &active_caboose.caboose.version,
        );
        return None;
    };

    let board = &active_caboose.caboose.board;
    let Some(rkth) = &active_caboose.caboose.sign else {
        warn!(
            log,
            "cannot configure RoT update for board \
             (missing sign in caboose from inventory)";
            baseboard_id
        );
        return None;
    };

    let matching_artifacts: Vec<_> = current_artifacts
        .artifacts
        .iter()
        .filter(|a| {
            // A matching RoT artifact will have:
            //
            // - "name" matching the board name (found above from caboose)
            // - "kind" matching one of the known RoT kinds
            // - "sign" matching the rkth (found above from caboose)

            if a.id.name != *board {
                return false;
            }

            let Some(artifact_sign) = &a.sign else {
                return false;
            };
            let Ok(artifact_sign) = String::from_utf8(artifact_sign.to_vec())
            else {
                return false;
            };
            if artifact_sign != *rkth {
                return false;
            }

            // We'll be updating the inactive slot, so we choose the artifact
            // based on the inactive slot's kind.
            match active_slot.toggled() {
                RotSlot::A => {
                    let slot_a_artifacts = [
                        ArtifactKind::GIMLET_ROT_IMAGE_A,
                        ArtifactKind::PSC_ROT_IMAGE_A,
                        ArtifactKind::SWITCH_ROT_IMAGE_A,
                    ];

                    if slot_a_artifacts.contains(&a.id.kind) {
                        return true;
                    }
                }
                RotSlot::B => {
                    let slot_b_artifacts = [
                        ArtifactKind::GIMLET_ROT_IMAGE_B,
                        ArtifactKind::PSC_ROT_IMAGE_B,
                        ArtifactKind::SWITCH_ROT_IMAGE_B,
                    ];

                    if slot_b_artifacts.contains(&a.id.kind) {
                        return true;
                    }
                }
            }

            false
        })
        .collect();
    if matching_artifacts.is_empty() {
        warn!(
            log,
            "cannot configure RoT update for board (no matching artifact)";
            baseboard_id,
        );
        return None;
    }

    if matching_artifacts.len() > 1 {
        // This should be impossible unless we shipped a TUF repo with more than
        // 1 artifact for the same board and root key table hash (RKTH) as
        // `SIGN`. But it doesn't prevent us from picking one and proceeding.
        // Make a note and proceed.
        warn!(log, "found more than one matching artifact for RoT update");
    }

    let artifact = matching_artifacts[0];

    // If the artifact's version matches what's deployed, then no update is
    // needed.
    if artifact.id.version == expected_active_version {
        debug!(log, "no RoT update needed for board"; baseboard_id);
        return None;
    }

    let expected_active_slot = ExpectedActiveRotSlot {
        slot: active_slot,
        version: expected_active_version,
    };

    // Begin configuring an update.
    let expected_inactive_version = match inventory
        .caboose_for(
            CabooseWhich::from_rot_slot(active_slot.toggled()),
            baseboard_id,
        )
        .map(|c| c.caboose.version.parse::<ArtifactVersion>())
        .transpose()
    {
        Ok(None) => ExpectedVersion::NoValidVersion,
        Ok(Some(v)) => ExpectedVersion::Version(v),
        Err(_) => {
            warn!(
                log,
                "cannot configure RoT update for board \
                 (found inactive slot contents but version was not valid)";
                baseboard_id
            );
            return None;
        }
    };

    Some(PendingMgsUpdate {
        baseboard_id: baseboard_id.clone(),
        sp_type: sp_info.sp_type,
        slot_id: sp_info.sp_slot,
        details: PendingMgsUpdateDetails::Rot {
            expected_active_slot,
            expected_inactive_version,
            expected_persistent_boot_preference: rot_state
                .persistent_boot_preference,
            expected_pending_persistent_boot_preference: rot_state
                .pending_persistent_boot_preference,
            expected_transient_boot_preference: rot_state
                .transient_boot_preference,
        },
        artifact_hash: artifact.hash,
        artifact_version: artifact.id.version.clone(),
    })
}
