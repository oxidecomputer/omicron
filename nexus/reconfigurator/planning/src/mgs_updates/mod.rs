// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for making choices about MGS-managed updates

mod rot;

use crate::mgs_updates::rot::RotUpdateState;
use crate::mgs_updates::rot::mgs_update_status_rot;
use crate::mgs_updates::rot::try_make_update_rot;

use gateway_types::rot::RotSlot;
use nexus_types::deployment::ExpectedActiveRotSlot;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::deployment::PendingMgsUpdates;
use nexus_types::deployment::TargetReleaseDescription;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::Collection;
use omicron_common::api::external::TufRepoDescription;
use slog::{debug, error, info, warn};
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::sync::Arc;
use thiserror::Error;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::ArtifactVersionError;
use tufaceous_artifact::KnownArtifactKind;

/// How to handle an MGS-driven update that has become impossible due to
/// unsatisfied preconditions.
#[derive(Debug, Clone, Copy, strum::EnumIter)]
pub enum ImpossibleUpdatePolicy {
    /// Keep the update in the subsequent blueprint (e.g., because we believe it
    /// may become possible again).
    Keep,
    /// Remove the impossible update and attempt to replan, which will typically
    /// replace the impossible update with a new update for the same target with
    /// different preconditions.
    Reevaluate,
}

/// Generates a new set of `PendingMgsUpdates` based on:
///
/// * `inventory`: the latest inventory
/// * `current_boards`: a set of baseboards to consider updating
///   (it is possible to have baseboards in inventory that would never be
///   updated because they're not considered part of the current system)
/// * `current_updates`: the most recent set of configured `PendingMgsUpdates`
/// * `current_artifacts`: information about artifacts from the current target
///   release (if any)
/// * `nmax_updates`: the maximum number of updates allowed at once
/// * `impossible_update_policy`: what to do if we detect an update has become
///   impossible due to unsatisfied preconditions
///
/// By current policy, `nmax_updates` is always 1, but the implementation here
/// supports more than one update per invocation.
pub fn plan_mgs_updates(
    log: &slog::Logger,
    inventory: &Collection,
    current_boards: &BTreeSet<Arc<BaseboardId>>,
    current_updates: &PendingMgsUpdates,
    current_artifacts: &TargetReleaseDescription,
    nmax_updates: usize,
    impossible_update_policy: ImpossibleUpdatePolicy,
) -> PendingMgsUpdates {
    let mut rv = PendingMgsUpdates::new();
    let mut boards_preferred = BTreeSet::new();

    // Determine the status of all currently pending updates by comparing what
    // they were trying to do (and their preconditions) against the current
    // state (from inventory).
    //
    // If a pending update is either done or impossible, we'll prioritize
    // evaluating the same board for any further updates.  For the "done" case,
    // this will cause us to update one board's SP, RoT, etc. before moving onto
    // another board.  For the "impossible" case, this should just fix up the
    // update request with updated preconditions so that it can complete.
    //
    // If a pending update is in-progress or if we cannot determine its status
    // because inventory is incomplete, then we'll "keep" it (that is, we copy
    // the same update into the `PendingMgsUpdates` that we're returning).
    for update in current_updates {
        match mgs_update_status(log, inventory, update) {
            Ok(MgsUpdateStatus::Done) => {
                info!(
                    log,
                    "MGS-driven update completed \
                     (will remove it and re-evaluate board)";
                    update
                );
                boards_preferred.insert(update.baseboard_id.clone());
            }
            Ok(MgsUpdateStatus::Impossible) => match impossible_update_policy {
                ImpossibleUpdatePolicy::Keep => {
                    info!(
                        log,
                        "keeping apparently-impossible MGS-driven update \
                         (waiting for recent update to be applied)";
                        update
                    );
                    rv.insert(update.clone());
                }
                ImpossibleUpdatePolicy::Reevaluate => {
                    info!(
                        log,
                        "MGS-driven update impossible \
                         (will remove it and re-evaluate board)";
                        update
                    );
                    boards_preferred.insert(update.baseboard_id.clone());
                }
            },
            Ok(MgsUpdateStatus::NotDone) => {
                info!(
                    log,
                    "MGS-driven update not yet completed (will keep it)";
                    update
                );
                rv.insert(update.clone());
            }
            Err(error) => {
                info!(
                    log,
                    "cannot determine MGS-driven update status (will keep it)";
                    update,
                    InlineErrorChain::new(&error)
                );
                rv.insert(update.clone());
            }
        }
    }

    // If we don't have a "real" target release (i.e., an uploaded TUF repo
    // containing artifacts), then we cannot configure more updates.
    let current_artifacts = match current_artifacts {
        TargetReleaseDescription::Initial => {
            warn!(
                log,
                "cannot issue more MGS-driven updates (no current artifacts)",
            );
            return rv;
        }
        TargetReleaseDescription::TufRepo(description) => description,
    };

    // Next, configure new updates for any boards that need an update, up to
    // `nmax_updates`.
    //
    // For the reasons mentioned above, we'll start with the boards that just
    // had an in-progress update that we elected not to keep.  Then we'll look
    // at all the other boards.  Note that using `extend` here will cause the
    // boards that we're prioritizing to appear twice in this list.
    let non_preferred =
        current_boards.iter().filter(|b| !boards_preferred.contains(*b));
    let candidates = boards_preferred.iter().chain(non_preferred);
    for board in candidates {
        if rv.len() >= nmax_updates {
            info!(
                log,
                "reached maximum number of pending MGS-driven updates";
                "max" => nmax_updates
            );
            return rv;
        }

        match try_make_update(log, board, inventory, current_artifacts) {
            Some(update) => {
                info!(log, "configuring MGS-driven update"; &update);
                rv.insert(update);
            }
            None => {
                info!(log, "skipping board for MGS-driven update"; board);
            }
        }
    }

    info!(log, "ran out of boards for MGS-driven update");
    rv
}

#[derive(Debug)]
enum MgsUpdateStatus {
    /// the requested update has completed (i.e., the active slot is running the
    /// requested version)
    Done,
    /// the requested update has not completed, but remains possible
    /// (i.e., the active slot is not running the requested version and the
    /// preconditions remain true)
    NotDone,
    /// the requested update has not completed and the preconditions are not
    /// currently met
    ///
    /// The only way for this update to complete as-written is if reality
    /// changes such that the preconditions become met.  But the only normal
    /// operation that could make that happen is blueprint execution, which
    /// won't do anything while the preconditions aren't met.  So if an update
    /// is in this state, generally the planner must remove this update (and
    /// presumably add one with updated preconditions).
    Impossible,
}

#[derive(Debug, Error)]
enum MgsUpdateStatusError {
    #[error("no SP info found in inventory")]
    MissingSpInfo,
    #[error("no caboose found for active slot in inventory")]
    MissingActiveCaboose,
    #[error("no RoT state found in inventory")]
    MissingRotState,
    #[error("unable to parse input into ArtifactVersion: {0:?}")]
    FailedArtifactVersionParse(ArtifactVersionError),
}

/// Determine the status of a single MGS-driven update based on what's in
/// inventory for that board.
fn mgs_update_status(
    log: &slog::Logger,
    inventory: &Collection,
    update: &PendingMgsUpdate,
) -> Result<MgsUpdateStatus, MgsUpdateStatusError> {
    let baseboard_id = &update.baseboard_id;
    let desired_version = &update.artifact_version;

    // Check the contents of the target of `update` against what we expect
    // either before or after the update.
    //
    // We check this before anything else because if we get back
    // `MgsUpdateStatus::Done`, then we're done no matter what else is true.
    let update_status = match &update.details {
        PendingMgsUpdateDetails::RotBootloader {
            expected_stage0_version,
            expected_stage0_next_version,
        } => {
            let Some(stage0_caboose) =
                inventory.caboose_for(CabooseWhich::Stage0, baseboard_id)
            else {
                return Err(MgsUpdateStatusError::MissingActiveCaboose);
            };

            let found_stage0_next_version = inventory
                .caboose_for(CabooseWhich::Stage0Next, baseboard_id)
                .map(|c| c.caboose.version.as_ref());

            Ok(mgs_update_status_rot_bootloader(
                desired_version,
                expected_stage0_version,
                expected_stage0_next_version,
                &stage0_caboose.caboose.version,
                found_stage0_next_version,
            ))
        }
        PendingMgsUpdateDetails::Sp {
            expected_active_version,
            expected_inactive_version,
        } => {
            let Some(active_caboose) =
                inventory.caboose_for(CabooseWhich::SpSlot0, baseboard_id)
            else {
                return Err(MgsUpdateStatusError::MissingActiveCaboose);
            };

            let found_inactive_version = inventory
                .caboose_for(CabooseWhich::SpSlot1, baseboard_id)
                .map(|c| c.caboose.version.as_ref());

            Ok(mgs_update_status_sp(
                desired_version,
                expected_active_version,
                expected_inactive_version,
                &active_caboose.caboose.version,
                found_inactive_version,
            ))
        }
        PendingMgsUpdateDetails::Rot {
            expected_active_slot,
            expected_inactive_version,
            expected_persistent_boot_preference,
            expected_pending_persistent_boot_preference,
            expected_transient_boot_preference,
        } => {
            let active_caboose_which = match &expected_active_slot.slot {
                RotSlot::A => CabooseWhich::RotSlotA,
                RotSlot::B => CabooseWhich::RotSlotB,
            };

            let Some(active_caboose) =
                inventory.caboose_for(active_caboose_which, baseboard_id)
            else {
                return Err(MgsUpdateStatusError::MissingActiveCaboose);
            };

            let found_inactive_version = inventory
                .caboose_for(active_caboose_which.toggled_slot(), baseboard_id)
                .map(|c| c.caboose.version.as_ref());

            let rot_state = inventory
                .rots
                .get(baseboard_id)
                .ok_or(MgsUpdateStatusError::MissingRotState)?;

            let found_active_version =
                ArtifactVersion::new(active_caboose.caboose.version.clone())
                    .map_err(|e| {
                        MgsUpdateStatusError::FailedArtifactVersionParse(e)
                    })?;

            let found_active_slot = ExpectedActiveRotSlot {
                slot: rot_state.active_slot,
                version: found_active_version,
            };

            let expected = RotUpdateState {
                active_slot: expected_active_slot.clone(),
                persistent_boot_preference:
                    *expected_persistent_boot_preference,
                pending_persistent_boot_preference:
                    *expected_pending_persistent_boot_preference,
                transient_boot_preference: *expected_transient_boot_preference,
            };

            let found = RotUpdateState {
                active_slot: found_active_slot,
                persistent_boot_preference: rot_state
                    .persistent_boot_preference,
                pending_persistent_boot_preference: rot_state
                    .pending_persistent_boot_preference,
                transient_boot_preference: rot_state.transient_boot_preference,
            };

            Ok(mgs_update_status_rot(
                desired_version,
                expected,
                found,
                expected_inactive_version,
                found_inactive_version,
            ))
        }
    };

    // If we're able to reach a clear determination based on the status alone,
    // great.  Return that.
    if matches!(
        update_status,
        Err(_) | Ok(MgsUpdateStatus::Done) | Ok(MgsUpdateStatus::Impossible)
    ) {
        return update_status;
    }

    // If based on the status we're only able to determine that the update is
    // not yet done, there's another "impossible" case to consider: that the
    // baseboard has moved in the rack.
    let sp_info = inventory
        .sps
        .get(baseboard_id)
        .ok_or(MgsUpdateStatusError::MissingSpInfo)?;
    if sp_info.sp_type != update.sp_type {
        // This should be impossible.  This same baseboard has somehow changed
        // its type (e.g., sled vs. switch vs. PSC).  This doesn't affect what
        // we do here but definitely raises a red flag.
        error!(
            log,
            "baseboard appears to have changed board type";
            "sp_info" => #?sp_info,
            update,
        );
        Ok(MgsUpdateStatus::Impossible)
    } else if sp_info.sp_slot != update.slot_id {
        warn!(
            log,
            "baseboard with in-progress MGS-driven update has moved";
            "sp_info" => #?sp_info,
            update,
        );
        Ok(MgsUpdateStatus::Impossible)
    } else {
        update_status
    }
}

/// Compares a configured SP update with information from inventory and
/// determines the current status of the update.  See `MgsUpdateStatus`.
fn mgs_update_status_rot_bootloader(
    desired_version: &ArtifactVersion,
    expected_stage0_version: &ArtifactVersion,
    expected_stage0_next_version: &ExpectedVersion,
    found_stage0_version: &str,
    found_stage0_next_version: Option<&str>,
) -> MgsUpdateStatus {
    if found_stage0_version == desired_version.as_str() {
        // If we find the desired version in the active slot, we're done.
        return MgsUpdateStatus::Done;
    }

    // The update hasn't completed.
    //
    // Check to make sure the contents of the active slot are still what they
    // were when we configured this update.  If not, then this update cannot
    // proceed as currently configured.  It will fail its precondition check.
    if found_stage0_version != expected_stage0_version.as_str() {
        return MgsUpdateStatus::Impossible;
    }

    // TODO-K: All SP components have the same logic below. Make it into a
    // common function

    // Similarly, check the contents of the inactive slot to determine if it
    // still matches what we saw when we configured this update.  If not, then
    // this update cannot proceed as currently configured.  It will fail its
    // precondition check.
    //
    // This logic is more complex than for the active slot because unlike the
    // active slot, it's possible for both the found contents and the expected
    // contents to be missing and that's not necessarily an error.
    match (found_stage0_next_version, expected_stage0_next_version) {
        (Some(_), ExpectedVersion::NoValidVersion) => {
            // We expected nothing in the inactive slot, but found something.
            MgsUpdateStatus::Impossible
        }
        (Some(found), ExpectedVersion::Version(expected)) => {
            if found == expected.as_str() {
                // We found something in the inactive slot that matches what we
                // expected.
                MgsUpdateStatus::NotDone
            } else {
                // We found something in the inactive slot that differs from
                // what we expected.
                MgsUpdateStatus::Impossible
            }
        }
        (None, ExpectedVersion::Version(_)) => {
            // We expected something in the inactive slot, but found nothing.
            // This case is tricky because we can't tell from the inventory
            // whether we transiently failed to fetch the caboose for some
            // reason or whether the caboose is actually garbage.  We choose to
            // assume that it's actually garbage, which would mean that this
            // update as-configured is impossible.  This will cause us to
            // generate a new update that expects garbage in the inactive slot.
            // If we're right, great.  If we're wrong, then *that* update will
            // be impossible to complete, but we should fix this again if the
            // transient error goes away.
            //
            // If we instead assumed that this was a transient error, we'd do
            // nothing here instead.  But if the caboose was really missing,
            // then we'd get stuck forever waiting for something that would
            // never happen.
            MgsUpdateStatus::Impossible
        }
        (None, ExpectedVersion::NoValidVersion) => {
            // We expected nothing in the inactive slot and found nothing there.
            // No problem!
            MgsUpdateStatus::NotDone
        }
    }
}

/// Compares a configured SP update with information from inventory and
/// determines the current status of the update.  See `MgsUpdateStatus`.
fn mgs_update_status_sp(
    desired_version: &ArtifactVersion,
    expected_active_version: &ArtifactVersion,
    expected_inactive_version: &ExpectedVersion,
    found_active_version: &str,
    found_inactive_version: Option<&str>,
) -> MgsUpdateStatus {
    if found_active_version == desired_version.as_str() {
        // If we find the desired version in the active slot, we're done.
        return MgsUpdateStatus::Done;
    }

    // The update hasn't completed.
    //
    // Check to make sure the contents of the active slot are still what they
    // were when we configured this update.  If not, then this update cannot
    // proceed as currently configured.  It will fail its precondition check.
    if found_active_version != expected_active_version.as_str() {
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

fn mgs_update_status_inactive_versions(
    found_inactive_version: Option<&str>,
    expected_inactive_version: &ExpectedVersion,
) -> MgsUpdateStatus {
    // This logic is more complex than for the active slot because unlike the
    // active slot, it's possible for both the found contents and the expected
    // contents to be missing and that's not necessarily an error.
    match (found_inactive_version, expected_inactive_version) {
        (Some(_), ExpectedVersion::NoValidVersion) => {
            // We expected nothing in the inactive slot, but found something.
            MgsUpdateStatus::Impossible
        }
        (Some(found), ExpectedVersion::Version(expected)) => {
            if found == expected.as_str() {
                // We found something in the inactive slot that matches what we
                // expected.
                MgsUpdateStatus::NotDone
            } else {
                // We found something in the inactive slot that differs from
                // what we expected.
                MgsUpdateStatus::Impossible
            }
        }
        (None, ExpectedVersion::Version(_)) => {
            // We expected something in the inactive slot, but found nothing.
            // This case is tricky because we can't tell from the inventory
            // whether we transiently failed to fetch the caboose for some
            // reason or whether the caboose is actually garbage.  We choose to
            // assume that it's actually garbage, which would mean that this
            // update as-configured is impossible.  This will cause us to
            // generate a new update that expects garbage in the inactive slot.
            // If we're right, great.  If we're wrong, then *that* update will
            // be impossible to complete, but we should fix this again if the
            // transient error goes away.
            //
            // If we instead assumed that this was a transient error, we'd do
            // nothing here instead.  But if the caboose was really missing,
            // then we'd get stuck forever waiting for something that would
            // never happen.
            MgsUpdateStatus::Impossible
        }
        (None, ExpectedVersion::NoValidVersion) => {
            // We expected nothing in the inactive slot and found nothing there.
            // No problem!
            MgsUpdateStatus::NotDone
        }
    }
}

/// Determine if the given baseboard needs any MGS-driven update (e.g., update
/// to its SP, RoT, etc.).  If so, returns the update.  If not, returns `None`.
fn try_make_update(
    log: &slog::Logger,
    baseboard_id: &Arc<BaseboardId>,
    inventory: &Collection,
    current_artifacts: &TufRepoDescription,
) -> Option<PendingMgsUpdate> {
    // TODO When we add support for planning RoT, and host OS
    // updates, we'll try these in a hardcoded priority order until any of them
    // returns `Some`.  The order is described in RFD 565 section "Update
    // Sequence".  For now, we only plan SP and RoT bootloader updates.
    try_make_update_rot_bootloader(
        log,
        baseboard_id,
        inventory,
        current_artifacts,
    )
    .or_else(|| {
        try_make_update_rot(log, baseboard_id, inventory, current_artifacts)
    })
    .or_else(|| {
        try_make_update_sp(log, baseboard_id, inventory, current_artifacts)
    })
}

/// Determine if the given baseboard needs an RoT bootloader update and, if so,
/// returns it.
fn try_make_update_rot_bootloader(
    log: &slog::Logger,
    baseboard_id: &Arc<BaseboardId>,
    inventory: &Collection,
    current_artifacts: &TufRepoDescription,
) -> Option<PendingMgsUpdate> {
    let Some(sp_info) = inventory.sps.get(baseboard_id) else {
        warn!(
            log,
            "cannot configure RoT bootloader update for board \
             (missing SP info from inventory)";
            baseboard_id
        );
        return None;
    };

    let Some(stage0_caboose) =
        inventory.caboose_for(CabooseWhich::Stage0, baseboard_id)
    else {
        warn!(
            log,
            "cannot configure RoT bootloader update for board \
             (missing stage0 caboose from inventory)";
            baseboard_id,
        );
        return None;
    };

    let Ok(expected_stage0_version) = stage0_caboose.caboose.version.parse()
    else {
        warn!(
            log,
            "cannot configure RoT bootloader update for board \
             (cannot parse current stage0 version as an ArtifactVersion)";
            baseboard_id,
            "found_version" => &stage0_caboose.caboose.version,
        );
        return None;
    };

    let board = &stage0_caboose.caboose.board;
    let matching_artifacts: Vec<_> = current_artifacts
        .artifacts
        .iter()
        .filter(|a| {
            // A matching RoT bootloader artifact will have:
            //
            // - "name" matching the board name (found above from caboose)
            // - "kind" matching one of the known SP kinds
            // - "rkth" verified against the CMPA/CFPA found in inventory

            if a.id.name != *board {
                return false;
            }

            match a.id.kind.to_known() {
                None => false,
                Some(
                    KnownArtifactKind::GimletRotBootloader
                    | KnownArtifactKind::PscRotBootloader
                    | KnownArtifactKind::SwitchRotBootloader,
                ) => true,
                Some(
                    KnownArtifactKind::GimletRot
                    | KnownArtifactKind::Host
                    | KnownArtifactKind::InstallinatorDocument
                    | KnownArtifactKind::Trampoline
                    | KnownArtifactKind::ControlPlane
                    | KnownArtifactKind::Zone
                    | KnownArtifactKind::PscRot
                    | KnownArtifactKind::SwitchRot
                    | KnownArtifactKind::GimletSp
                    | KnownArtifactKind::PscSp
                    | KnownArtifactKind::SwitchSp,
                ) => false,
            }

            // TODO-K: Verify rkth value against CMPA/CFPA
        })
        .collect();
    if matching_artifacts.is_empty() {
        warn!(
            log,
            "cannot configure RoT bootloader update for board (no matching artifact)";
            baseboard_id,
        );
        return None;
    }

    if matching_artifacts.len() > 1 {
        // This should be impossible unless we shipped a TUF repo with multiple
        // artifacts for the same board and with the same signature. But it
        // doesn't prevent us from picking one and proceeding.
        // Make a note and proceed.
        warn!(
            log,
            "found more than one matching artifact for RoT bootloader update"
        );
    }

    let artifact = matching_artifacts[0];

    // If the artifact's version matches what's deployed, then no update is
    // needed.
    if artifact.id.version == expected_stage0_version {
        debug!(log, "no RoT bootloader update needed for board"; baseboard_id);
        return None;
    }

    // Begin configuring an update.
    let expected_stage0_next_version = match inventory
        .caboose_for(CabooseWhich::Stage0Next, baseboard_id)
        .map(|c| c.caboose.version.parse::<ArtifactVersion>())
        .transpose()
    {
        Ok(None) => ExpectedVersion::NoValidVersion,
        Ok(Some(v)) => ExpectedVersion::Version(v),
        Err(_) => {
            warn!(
                log,
                "cannot configure RoT bootloader update for board \
                 (found stage0 next contents but version was not valid)";
                baseboard_id
            );
            return None;
        }
    };

    Some(PendingMgsUpdate {
        baseboard_id: baseboard_id.clone(),
        sp_type: sp_info.sp_type,
        slot_id: sp_info.sp_slot,
        details: PendingMgsUpdateDetails::RotBootloader {
            expected_stage0_version,
            expected_stage0_next_version,
        },
        artifact_hash: artifact.hash,
        artifact_version: artifact.id.version.clone(),
    })
}

/// Determine if the given baseboard needs an SP update and, if so, returns it.
fn try_make_update_sp(
    log: &slog::Logger,
    baseboard_id: &Arc<BaseboardId>,
    inventory: &Collection,
    current_artifacts: &TufRepoDescription,
) -> Option<PendingMgsUpdate> {
    let Some(sp_info) = inventory.sps.get(baseboard_id) else {
        warn!(
            log,
            "cannot configure SP update for board \
             (missing SP info from inventory)";
            baseboard_id
        );
        return None;
    };

    let Some(active_caboose) =
        inventory.caboose_for(CabooseWhich::SpSlot0, baseboard_id)
    else {
        warn!(
            log,
            "cannot configure SP update for board \
             (missing active caboose from inventory)";
            baseboard_id,
        );
        return None;
    };

    let Ok(expected_active_version) = active_caboose.caboose.version.parse()
    else {
        warn!(
            log,
            "cannot configure SP update for board \
             (cannot parse current active version as an ArtifactVersion)";
            baseboard_id,
            "found_version" => &active_caboose.caboose.version,
        );
        return None;
    };

    let board = &active_caboose.caboose.board;
    let matching_artifacts: Vec<_> = current_artifacts
        .artifacts
        .iter()
        .filter(|a| {
            // A matching SP artifact will have:
            //
            // - "name" matching the board name (found above from caboose)
            // - "kind" matching one of the known SP kinds

            if a.id.name != *board {
                return false;
            }

            match a.id.kind.to_known() {
                None => false,
                Some(
                    KnownArtifactKind::GimletSp
                    | KnownArtifactKind::PscSp
                    | KnownArtifactKind::SwitchSp,
                ) => true,
                Some(
                    KnownArtifactKind::GimletRot
                    | KnownArtifactKind::Host
                    | KnownArtifactKind::Trampoline
                    | KnownArtifactKind::InstallinatorDocument
                    | KnownArtifactKind::ControlPlane
                    | KnownArtifactKind::Zone
                    | KnownArtifactKind::PscRot
                    | KnownArtifactKind::SwitchRot
                    | KnownArtifactKind::GimletRotBootloader
                    | KnownArtifactKind::PscRotBootloader
                    | KnownArtifactKind::SwitchRotBootloader,
                ) => false,
            }
        })
        .collect();
    if matching_artifacts.is_empty() {
        warn!(
            log,
            "cannot configure SP update for board (no matching artifact)";
            baseboard_id,
        );
        return None;
    }

    if matching_artifacts.len() > 1 {
        // This should be impossible unless we shipped a TUF repo with multiple
        // artifacts for the same board.  But it doesn't prevent us from picking
        // one and proceeding.  Make a note and proceed.
        warn!(log, "found more than one matching artifact for SP update");
    }

    let artifact = matching_artifacts[0];

    // If the artifact's version matches what's deployed, then no update is
    // needed.
    if artifact.id.version == expected_active_version {
        debug!(log, "no SP update needed for board"; baseboard_id);
        return None;
    }

    // Begin configuring an update.
    let expected_inactive_version = match inventory
        .caboose_for(CabooseWhich::SpSlot1, baseboard_id)
        .map(|c| c.caboose.version.parse::<ArtifactVersion>())
        .transpose()
    {
        Ok(None) => ExpectedVersion::NoValidVersion,
        Ok(Some(v)) => ExpectedVersion::Version(v),
        Err(_) => {
            warn!(
                log,
                "cannot configure SP update for board \
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
        details: PendingMgsUpdateDetails::Sp {
            expected_active_version,
            expected_inactive_version,
        },
        artifact_hash: artifact.hash,
        artifact_version: artifact.id.version.clone(),
    })
}

#[cfg(test)]
mod test {
    use super::ImpossibleUpdatePolicy;
    use super::plan_mgs_updates;
    use chrono::Utc;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingLevel;
    use gateway_client::types::PowerState;
    use gateway_client::types::RotState;
    use gateway_client::types::SpComponentCaboose;
    use gateway_client::types::SpState;
    use gateway_client::types::SpType;
    use nexus_types::deployment::ExpectedVersion;
    use nexus_types::deployment::PendingMgsUpdate;
    use nexus_types::deployment::PendingMgsUpdateDetails;
    use nexus_types::deployment::PendingMgsUpdates;
    use nexus_types::deployment::TargetReleaseDescription;
    use nexus_types::inventory::CabooseWhich;
    use nexus_types::inventory::Collection;
    use nexus_types::inventory::RotSlot;
    use omicron_common::api::external::TufArtifactMeta;
    use omicron_common::api::external::TufRepoDescription;
    use omicron_common::api::external::TufRepoMeta;
    use omicron_common::update::ArtifactId;
    use omicron_test_utils::dev::LogContext;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use strum::IntoEnumIterator;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::ArtifactKind;
    use tufaceous_artifact::ArtifactVersion;
    use tufaceous_artifact::KnownArtifactKind;

    /// Version that will be used for all artifacts in the TUF repo
    const ARTIFACT_VERSION_2: ArtifactVersion =
        ArtifactVersion::new_const("2.0.0");
    /// Version that will be "deployed" in the SP we want to update
    const ARTIFACT_VERSION_1: ArtifactVersion =
        ArtifactVersion::new_const("1.0.0");
    /// Version that's different from the other two
    const ARTIFACT_VERSION_1_5: ArtifactVersion =
        ArtifactVersion::new_const("1.5.0");

    /// Hash of fake artifact for fake gimlet-e SP
    const ARTIFACT_HASH_SP_GIMLET_E: ArtifactHash = ArtifactHash([1; 32]);
    /// Hash of fake artifact for fake gimlet-d SP
    const ARTIFACT_HASH_SP_GIMLET_D: ArtifactHash = ArtifactHash([2; 32]);
    /// Hash of fake artifact for fake sidecar-b SP
    const ARTIFACT_HASH_SP_SIDECAR_B: ArtifactHash = ArtifactHash([5; 32]);
    /// Hash of fake artifact for fake sidecar-c SP
    const ARTIFACT_HASH_SP_SIDECAR_C: ArtifactHash = ArtifactHash([6; 32]);
    /// Hash of fake artifact for fake psc-b SP
    const ARTIFACT_HASH_SP_PSC_B: ArtifactHash = ArtifactHash([9; 32]);
    /// Hash of fake artifact for fake psc-c SP
    const ARTIFACT_HASH_SP_PSC_C: ArtifactHash = ArtifactHash([10; 32]);
    /// Hash of fake artifact for fake gimlet RoT slot A
    const ARTIFACT_HASH_ROT_GIMLET_A: ArtifactHash = ArtifactHash([13; 32]);
    /// Hash of fake artifact for fake gimlet RoT slot B
    const ARTIFACT_HASH_ROT_GIMLET_B: ArtifactHash = ArtifactHash([14; 32]);
    /// Hash of fake artifact for fake psc RoT slot A
    const ARTIFACT_HASH_ROT_PSC_A: ArtifactHash = ArtifactHash([17; 32]);
    /// Hash of fake artifact for fake psc RoT slot B
    const ARTIFACT_HASH_ROT_PSC_B: ArtifactHash = ArtifactHash([18; 32]);
    /// Hash of fake artifact for fake switch RoT slot A
    const ARTIFACT_HASH_ROT_SWITCH_A: ArtifactHash = ArtifactHash([21; 32]);
    /// Hash of fake artifact for fake switch RoT slot B
    const ARTIFACT_HASH_ROT_SWITCH_B: ArtifactHash = ArtifactHash([22; 32]);

    // unused artifact hashes

    const ARTIFACT_HASH_CONTROL_PLANE: ArtifactHash = ArtifactHash([33; 32]);
    const ARTIFACT_HASH_NEXUS: ArtifactHash = ArtifactHash([34; 32]);
    const ARTIFACT_HASH_HOST_OS: ArtifactHash = ArtifactHash([35; 32]);

    /// Hash of a fake RoT signing keys
    const ROT_SIGN_GIMLET: &str =
        "1111111111111111111111111111111111111111111111111111111111111111";
    const ROT_SIGN_PSC: &str =
        "2222222222222222222222222222222222222222222222222222222222222222";
    const ROT_SIGN_SWITCH: &str =
        "3333333333333333333333333333333333333333333333333333333333333333";

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum MgsUpdateComponent {
        Sp,
        Rot,
        RotBootloader,
        #[allow(unused)]
        HostOs,
    }

    fn test_artifact_for_board(board: &str) -> ArtifactHash {
        match board {
            "gimlet-d" => ARTIFACT_HASH_SP_GIMLET_D,
            "gimlet-e" => ARTIFACT_HASH_SP_GIMLET_E,
            "sidecar-b" => ARTIFACT_HASH_SP_SIDECAR_B,
            "sidecar-c" => ARTIFACT_HASH_SP_SIDECAR_C,
            "psc-b" => ARTIFACT_HASH_SP_PSC_B,
            "psc-c" => ARTIFACT_HASH_SP_PSC_C,
            _ => panic!("test bug: no artifact for board {board:?}"),
        }
    }

    fn test_artifact_for_artifact_kind(kind: ArtifactKind) -> ArtifactHash {
        let artifact_hash = if kind == ArtifactKind::GIMLET_ROT_IMAGE_A {
            ARTIFACT_HASH_ROT_GIMLET_A
        } else if kind == ArtifactKind::GIMLET_ROT_IMAGE_B {
            ARTIFACT_HASH_ROT_GIMLET_B
        } else if kind == ArtifactKind::PSC_ROT_IMAGE_A {
            ARTIFACT_HASH_ROT_PSC_A
        } else if kind == ArtifactKind::PSC_ROT_IMAGE_B {
            ARTIFACT_HASH_ROT_PSC_B
        } else if kind == ArtifactKind::SWITCH_ROT_IMAGE_A {
            ARTIFACT_HASH_ROT_SWITCH_A
        } else if kind == ArtifactKind::SWITCH_ROT_IMAGE_B {
            ARTIFACT_HASH_ROT_SWITCH_B
        } else {
            panic!("test bug: no artifact for artifact kind {kind:?}")
        };

        return artifact_hash;
    }

    /// Describes the SPs and RoTs in the environment used in these tests
    ///
    /// There will be:
    ///
    /// - 4 sled SPs
    /// - 2 switch SPs
    /// - 2 PSC SPs
    ///
    /// The specific set of hardware (boards) vary and are hardcoded:
    ///
    /// - sled 0: gimlet-d, oxide-rot-1
    /// - other sleds: gimlet-e, oxide-rot-1
    /// - switch 0: sidecar-b, oxide-rot-1
    /// - switch 1: sidecar-c, oxide-rot-1
    /// - psc 0: psc-b, oxide-rot-1
    /// - psc 1: psc-c, oxide-rot-1
    fn test_collection_config() -> BTreeMap<
        (SpType, u16),
        (&'static str, &'static str, &'static str, &'static str),
    > {
        BTreeMap::from([
            (
                (SpType::Sled, 0),
                ("sled_0", "gimlet-d", "oxide-rot-1", ROT_SIGN_GIMLET),
            ),
            (
                (SpType::Sled, 1),
                ("sled_1", "gimlet-e", "oxide-rot-1", ROT_SIGN_GIMLET),
            ),
            (
                (SpType::Sled, 2),
                ("sled_2", "gimlet-e", "oxide-rot-1", ROT_SIGN_GIMLET),
            ),
            (
                (SpType::Sled, 3),
                ("sled_3", "gimlet-e", "oxide-rot-1", ROT_SIGN_GIMLET),
            ),
            (
                (SpType::Switch, 0),
                ("switch_0", "sidecar-b", "oxide-rot-1", ROT_SIGN_SWITCH),
            ),
            (
                (SpType::Switch, 1),
                ("switch_1", "sidecar-c", "oxide-rot-1", ROT_SIGN_SWITCH),
            ),
            (
                (SpType::Power, 0),
                ("power_0", "psc-b", "oxide-rot-1", ROT_SIGN_PSC),
            ),
            (
                (SpType::Power, 1),
                ("power_1", "psc-c", "oxide-rot-1", ROT_SIGN_PSC),
            ),
        ])
    }

    /// Describes the SPs and RoTs in the environment used in these tests, but
    /// spearated by component for use in sequential testing
    fn test_config()
    -> BTreeMap<(SpType, u16, MgsUpdateComponent), (&'static str, &'static str)>
    {
        test_collection_config()
            .into_iter()
            .flat_map(
                |(
                    (sp_type, slot_id),
                    (serial, sp_board_name, rot_board_name, ..),
                )| {
                    [
                        (
                            (sp_type, slot_id, MgsUpdateComponent::Sp),
                            (serial, sp_board_name),
                        ),
                        (
                            (sp_type, slot_id, MgsUpdateComponent::Rot),
                            (serial, rot_board_name),
                        ),
                    ]
                },
            )
            .collect()
    }

    /// Returns a TufRepoDescription that we can use to exercise the planning
    /// code.
    fn make_tuf_repo() -> TufRepoDescription {
        const SYSTEM_VERSION: semver::Version = semver::Version::new(0, 0, 1);
        const SYSTEM_HASH: ArtifactHash = ArtifactHash([3; 32]);

        // Include a bunch of SP-related artifacts, as well as a few others just
        // to make sure those are properly ignored.
        let artifacts = vec![
            make_artifact(
                "control-plane",
                KnownArtifactKind::ControlPlane.into(),
                ARTIFACT_HASH_CONTROL_PLANE,
                None,
            ),
            make_artifact(
                "nexus",
                KnownArtifactKind::Zone.into(),
                ARTIFACT_HASH_NEXUS,
                None,
            ),
            make_artifact(
                "host-os",
                KnownArtifactKind::Host.into(),
                ARTIFACT_HASH_HOST_OS,
                None,
            ),
            make_artifact(
                "gimlet-d",
                KnownArtifactKind::GimletSp.into(),
                test_artifact_for_board("gimlet-d"),
                None,
            ),
            make_artifact(
                "gimlet-e",
                KnownArtifactKind::GimletSp.into(),
                test_artifact_for_board("gimlet-e"),
                None,
            ),
            make_artifact(
                "sidecar-b",
                KnownArtifactKind::SwitchSp.into(),
                test_artifact_for_board("sidecar-b"),
                None,
            ),
            make_artifact(
                "sidecar-c",
                KnownArtifactKind::SwitchSp.into(),
                test_artifact_for_board("sidecar-c"),
                None,
            ),
            make_artifact(
                "psc-b",
                KnownArtifactKind::PscSp.into(),
                test_artifact_for_board("psc-b"),
                None,
            ),
            make_artifact(
                "psc-c",
                KnownArtifactKind::PscSp.into(),
                test_artifact_for_board("psc-c"),
                None,
            ),
            make_artifact(
                "oxide-rot-1",
                ArtifactKind::GIMLET_ROT_IMAGE_A,
                test_artifact_for_artifact_kind(
                    ArtifactKind::GIMLET_ROT_IMAGE_A,
                ),
                Some(ROT_SIGN_GIMLET.into()),
            ),
            make_artifact(
                "oxide-rot-1",
                ArtifactKind::GIMLET_ROT_IMAGE_B,
                test_artifact_for_artifact_kind(
                    ArtifactKind::GIMLET_ROT_IMAGE_B,
                ),
                Some(ROT_SIGN_GIMLET.into()),
            ),
            make_artifact(
                "oxide-rot-1",
                ArtifactKind::PSC_ROT_IMAGE_A,
                test_artifact_for_artifact_kind(ArtifactKind::PSC_ROT_IMAGE_A),
                Some(ROT_SIGN_PSC.into()),
            ),
            make_artifact(
                "oxide-rot-1",
                ArtifactKind::PSC_ROT_IMAGE_B,
                test_artifact_for_artifact_kind(ArtifactKind::PSC_ROT_IMAGE_B),
                Some(ROT_SIGN_PSC.into()),
            ),
            make_artifact(
                "oxide-rot-1",
                ArtifactKind::SWITCH_ROT_IMAGE_A,
                test_artifact_for_artifact_kind(
                    ArtifactKind::SWITCH_ROT_IMAGE_A,
                ),
                Some(ROT_SIGN_SWITCH.into()),
            ),
            make_artifact(
                "oxide-rot-1",
                ArtifactKind::SWITCH_ROT_IMAGE_B,
                test_artifact_for_artifact_kind(
                    ArtifactKind::SWITCH_ROT_IMAGE_B,
                ),
                Some(ROT_SIGN_SWITCH.into()),
            ),
        ];

        TufRepoDescription {
            repo: TufRepoMeta {
                hash: SYSTEM_HASH,
                targets_role_version: 0,
                valid_until: Utc::now(),
                system_version: SYSTEM_VERSION,
                file_name: String::new(),
            },
            artifacts,
        }
    }

    fn make_artifact(
        name: &str,
        kind: ArtifactKind,
        hash: ArtifactHash,
        sign: Option<Vec<u8>>,
    ) -> TufArtifactMeta {
        TufArtifactMeta {
            id: ArtifactId {
                name: name.to_string(),
                version: ARTIFACT_VERSION_2,
                kind,
            },
            hash,
            size: 0, // unused here
            sign,
        }
    }

    // Construct inventory for an environment suitable for our testing.
    //
    // See test_config() for information about the hardware.  All SPs and RoTs
    // will appear to be running version `active_version` except those
    // identified in `active_version_exceptions`.  All SPs and RoTs will appear
    // to have `inactive_version` in the inactive slot.
    fn make_collection(
        active_version: ArtifactVersion,
        active_version_exceptions: &BTreeMap<(SpType, u16), ArtifactVersion>,
        inactive_version: ExpectedVersion,
        active_rot_version: ArtifactVersion,
        active_rot_version_exceptions: &BTreeMap<
            (SpType, u16),
            ArtifactVersion,
        >,
        inactive_rot_version: ExpectedVersion,
    ) -> Collection {
        let mut builder = nexus_inventory::CollectionBuilder::new(
            "planning_mgs_updates_basic",
        );

        let dummy_sp_state = SpState {
            base_mac_address: [0; 6],
            hubris_archive_id: String::from("unused"),
            model: String::from("unused"),
            power_state: PowerState::A0,
            revision: 0,
            rot: RotState::V3 {
                active: RotSlot::A,
                pending_persistent_boot_preference: None,
                persistent_boot_preference: RotSlot::A,
                slot_a_error: None,
                slot_a_fwid: Default::default(),
                slot_b_error: None,
                slot_b_fwid: Default::default(),
                stage0_error: None,
                stage0_fwid: Default::default(),
                stage0next_error: None,
                stage0next_fwid: Default::default(),
                transient_boot_preference: None,
            },
            serial_number: String::from("unused"),
        };

        let test_config = test_collection_config();
        for (
            (sp_type, sp_slot),
            (serial, caboose_sp_board, caboose_rot_board, rkth),
        ) in test_config
        {
            let sp_state = SpState {
                model: format!("dummy_{}", sp_type),
                serial_number: serial.to_string(),
                ..dummy_sp_state.clone()
            };

            let baseboard_id = builder
                .found_sp_state("test", sp_type, sp_slot, sp_state)
                .unwrap();
            let active_version = active_version_exceptions
                .get(&(sp_type, sp_slot))
                .unwrap_or(&active_version);
            let active_rot_version = active_rot_version_exceptions
                .get(&(sp_type, sp_slot))
                .unwrap_or(&active_rot_version);

            builder
                .found_caboose(
                    &baseboard_id,
                    CabooseWhich::SpSlot0,
                    "test",
                    SpComponentCaboose {
                        board: caboose_sp_board.to_string(),
                        epoch: None,
                        git_commit: String::from("unused"),
                        name: caboose_sp_board.to_string(),
                        sign: None,
                        version: active_version.as_str().to_string(),
                    },
                )
                .unwrap();

            builder
                .found_caboose(
                    &baseboard_id,
                    CabooseWhich::RotSlotA,
                    "test",
                    SpComponentCaboose {
                        board: caboose_rot_board.to_string(),
                        epoch: None,
                        git_commit: String::from("unused"),
                        name: caboose_rot_board.to_string(),
                        sign: Some(rkth.to_string()),
                        version: active_rot_version.as_str().to_string(),
                    },
                )
                .unwrap();

            if let ExpectedVersion::Version(inactive_version) =
                &inactive_version
            {
                builder
                    .found_caboose(
                        &baseboard_id,
                        CabooseWhich::SpSlot1,
                        "test",
                        SpComponentCaboose {
                            board: caboose_sp_board.to_string(),
                            epoch: None,
                            git_commit: String::from("unused"),
                            name: caboose_sp_board.to_string(),
                            sign: None,
                            version: inactive_version.as_str().to_string(),
                        },
                    )
                    .unwrap();
            }

            if let ExpectedVersion::Version(inactive_rot_version) =
                &inactive_rot_version
            {
                builder
                    .found_caboose(
                        &baseboard_id,
                        CabooseWhich::RotSlotB,
                        "test",
                        SpComponentCaboose {
                            board: caboose_rot_board.to_string(),
                            epoch: None,
                            git_commit: String::from("unused"),
                            name: caboose_rot_board.to_string(),
                            sign: Some(rkth.to_string()),
                            version: inactive_rot_version.as_str().to_string(),
                        },
                    )
                    .unwrap();
            }
        }

        builder.build()
    }

    // Short hand-rolled update sequence that exercises some basic behavior for
    // SP updates.
    #[test]
    fn test_basic_sp() {
        let logctx = LogContext::new(
            "planning_mgs_updates_basic_sp",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let log = &logctx.log;

        // Test that with no updates pending and no TUF repo specified, there
        // will remain no updates pending.
        let collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Sled, 0), ARTIFACT_VERSION_1)]),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
        );
        let current_boards = &collection.baseboards;
        let initial_updates = PendingMgsUpdates::new();
        let nmax_updates = 1;
        let impossible_update_policy = ImpossibleUpdatePolicy::Reevaluate;
        let updates = plan_mgs_updates(
            log,
            &collection,
            current_boards,
            &initial_updates,
            &TargetReleaseDescription::Initial,
            nmax_updates,
            impossible_update_policy,
        );
        assert!(updates.is_empty());

        // Test that when a TUF repo is specified and one SP is outdated, then
        // it's configured with an update (and the update looks correct).
        let repo = make_tuf_repo();
        let updates = plan_mgs_updates(
            log,
            &collection,
            current_boards,
            &initial_updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert_eq!(updates.len(), 1);
        let first_update = updates.iter().next().expect("at least one update");
        assert_eq!(first_update.baseboard_id.serial_number, "sled_0");
        assert_eq!(first_update.sp_type, SpType::Sled);
        assert_eq!(first_update.slot_id, 0);
        assert_eq!(first_update.artifact_hash, ARTIFACT_HASH_SP_GIMLET_D);
        assert_eq!(first_update.artifact_version, ARTIFACT_VERSION_2);

        // Test that when an update is already pending, and nothing changes
        // about the state of the world (i.e., the inventory), then the planner
        // makes no changes.
        let later_updates = plan_mgs_updates(
            log,
            &collection,
            current_boards,
            &updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert_eq!(updates, later_updates);

        // Test that when two updates are needed, but one is already pending,
        // then the other one is *not* started (because it exceeds
        // nmax_updates).
        let later_collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::from([
                ((SpType::Sled, 0), ARTIFACT_VERSION_1),
                ((SpType::Switch, 1), ARTIFACT_VERSION_1),
            ]),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
        );
        let later_updates = plan_mgs_updates(
            log,
            &later_collection,
            current_boards,
            &updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert_eq!(updates, later_updates);

        // At this point, we're ready to test that when the first update
        // completes, then the second one *is* started.  This tests two
        // different things: first that we noticed the first one completed, and
        // second that we noticed another thing needed an update
        let later_collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Switch, 1), ARTIFACT_VERSION_1)]),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
        );
        let later_updates = plan_mgs_updates(
            log,
            &later_collection,
            current_boards,
            &updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert_eq!(later_updates.len(), 1);
        let next_update =
            later_updates.iter().next().expect("at least one update");
        assert_ne!(first_update, next_update);
        assert_eq!(next_update.baseboard_id.serial_number, "switch_1");
        assert_eq!(next_update.sp_type, SpType::Switch);
        assert_eq!(next_update.slot_id, 1);
        assert_eq!(next_update.artifact_hash, ARTIFACT_HASH_SP_SIDECAR_C);
        assert_eq!(next_update.artifact_version, ARTIFACT_VERSION_2);

        // Finally, test that when all SPs are in spec, then no updates are
        // configured.
        let updated_collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
        );
        let later_updates = plan_mgs_updates(
            log,
            &updated_collection,
            current_boards,
            &later_updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert!(later_updates.is_empty());

        // Test that we don't try to update boards that aren't in
        // `current_boards`, even if they're in inventory and outdated.
        let collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Sled, 0), ARTIFACT_VERSION_1)]),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
        );
        let updates = plan_mgs_updates(
            log,
            &collection,
            &BTreeSet::new(),
            &PendingMgsUpdates::new(),
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert!(updates.is_empty());
        let updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &PendingMgsUpdates::new(),
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        // We verified most of the details above.  Here we're just double
        // checking that the baseboard being missing is the only reason that no
        // update was generated.
        assert_eq!(updates.len(), 1);

        // Verify the precondition details of an ordinary update.
        let old_update =
            updates.into_iter().next().expect("at least one update");
        let PendingMgsUpdateDetails::Sp {
            expected_active_version: old_expected_active_version,
            expected_inactive_version: old_expected_inactive_version,
        } = &old_update.details
        else {
            panic!("expected SP update");
        };
        assert_eq!(ARTIFACT_VERSION_1, *old_expected_active_version);
        assert_eq!(
            ExpectedVersion::NoValidVersion,
            *old_expected_inactive_version
        );

        // Test that if the inactive slot contents have changed, then we'll get
        // a new update reflecting that.
        let collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Sled, 0), ARTIFACT_VERSION_1)]),
            ExpectedVersion::Version(ARTIFACT_VERSION_1),
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
        );
        let new_updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert_ne!(updates, new_updates);
        assert_eq!(new_updates.len(), 1);
        let new_update =
            new_updates.into_iter().next().expect("at least one update");
        assert_eq!(old_update.baseboard_id, new_update.baseboard_id);
        assert_eq!(old_update.sp_type, new_update.sp_type);
        assert_eq!(old_update.slot_id, new_update.slot_id);
        assert_eq!(old_update.artifact_hash, new_update.artifact_hash);
        assert_eq!(old_update.artifact_version, new_update.artifact_version);
        let PendingMgsUpdateDetails::Sp {
            expected_active_version: new_expected_active_version,
            expected_inactive_version: new_expected_inactive_version,
        } = &new_update.details
        else {
            panic!("expected SP update");
        };
        assert_eq!(ARTIFACT_VERSION_1, *new_expected_active_version);
        assert_eq!(
            ExpectedVersion::Version(ARTIFACT_VERSION_1),
            *new_expected_inactive_version
        );

        // Test that if instead it's the active slot whose contents have changed
        // to something other than the new expected version, then we'll also get
        // a new update reflecting that.
        let collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Sled, 0), ARTIFACT_VERSION_1_5)]),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
        );
        let new_updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert_ne!(updates, new_updates);
        assert_eq!(new_updates.len(), 1);
        let new_update =
            new_updates.into_iter().next().expect("at least one update");
        assert_eq!(old_update.baseboard_id, new_update.baseboard_id);
        assert_eq!(old_update.sp_type, new_update.sp_type);
        assert_eq!(old_update.slot_id, new_update.slot_id);
        assert_eq!(old_update.artifact_hash, new_update.artifact_hash);
        assert_eq!(old_update.artifact_version, new_update.artifact_version);
        let PendingMgsUpdateDetails::Sp {
            expected_active_version: new_expected_active_version,
            expected_inactive_version: new_expected_inactive_version,
        } = &new_update.details
        else {
            panic!("expected SP update");
        };
        assert_eq!(ARTIFACT_VERSION_1_5, *new_expected_active_version);
        assert_eq!(
            ExpectedVersion::NoValidVersion,
            *new_expected_inactive_version
        );

        logctx.cleanup_successful();
    }

    // Short hand-rolled update sequence that exercises some basic behavior for
    // RoT updates.
    #[test]
    fn test_basic_rot() {
        let logctx = LogContext::new(
            "planning_mgs_updates_basic_rot",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let log = &logctx.log;

        // Test that with no updates pending and no TUF repo specified, there
        // will remain no updates pending.
        let collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Sled, 0), ARTIFACT_VERSION_1)]),
            ExpectedVersion::NoValidVersion,
        );
        let current_boards = &collection.baseboards;
        let initial_updates = PendingMgsUpdates::new();
        let nmax_updates = 1;
        let impossible_update_policy = ImpossibleUpdatePolicy::Reevaluate;
        let updates = plan_mgs_updates(
            log,
            &collection,
            current_boards,
            &initial_updates,
            &TargetReleaseDescription::Initial,
            nmax_updates,
            impossible_update_policy,
        );
        assert!(updates.is_empty());

        // Test that when a TUF repo is specified and one RoT is outdated, then
        // it's configured with an update (and the update looks correct).
        let repo = make_tuf_repo();
        let updates = plan_mgs_updates(
            log,
            &collection,
            current_boards,
            &initial_updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert_eq!(updates.len(), 1);
        let first_update = updates.iter().next().expect("at least one update");
        assert_eq!(first_update.baseboard_id.serial_number, "sled_0");
        assert_eq!(first_update.sp_type, SpType::Sled);
        assert_eq!(first_update.slot_id, 0);
        assert_eq!(first_update.artifact_hash, ARTIFACT_HASH_ROT_GIMLET_B);
        assert_eq!(first_update.artifact_version, ARTIFACT_VERSION_2);

        // Test that when an update is already pending, and nothing changes
        // about the state of the world (i.e., the inventory), then the planner
        // makes no changes.
        let later_updates = plan_mgs_updates(
            log,
            &collection,
            current_boards,
            &updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert_eq!(updates, later_updates);

        // Test that when two updates are needed, but one is already pending,
        // then the other one is *not* started (because it exceeds
        // nmax_updates).
        let later_collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::from([
                ((SpType::Sled, 0), ARTIFACT_VERSION_1),
                ((SpType::Switch, 1), ARTIFACT_VERSION_1),
            ]),
            ExpectedVersion::NoValidVersion,
        );
        let later_updates = plan_mgs_updates(
            log,
            &later_collection,
            current_boards,
            &updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert_eq!(updates, later_updates);

        // At this point, we're ready to test that when the first SpType update
        // completes, then the second one *is* started.  This tests three
        // different things: first that we noticed the first one completed,
        // second that we noticed another thing needed an update, and third that
        // the planner schedules the updates in the correct order: first RoT,
        // and second SP.
        let later_collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Switch, 1), ARTIFACT_VERSION_1)]),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Switch, 1), ARTIFACT_VERSION_1)]),
            ExpectedVersion::NoValidVersion,
        );
        let later_updates = plan_mgs_updates(
            log,
            &later_collection,
            current_boards,
            &updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert_eq!(later_updates.len(), 1);
        let next_update =
            later_updates.iter().next().expect("at least one update");
        assert_ne!(first_update, next_update);
        assert_eq!(next_update.baseboard_id.serial_number, "switch_1");
        assert_eq!(next_update.sp_type, SpType::Switch);
        assert_eq!(next_update.slot_id, 1);
        assert_eq!(next_update.artifact_hash, ARTIFACT_HASH_ROT_SWITCH_B);
        assert_eq!(next_update.artifact_version, ARTIFACT_VERSION_2);

        // Finally, test that when all components are in spec, then no updates
        // are configured.
        let updated_collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
        );
        let later_updates = plan_mgs_updates(
            log,
            &updated_collection,
            current_boards,
            &later_updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert!(later_updates.is_empty());

        // Test that we don't try to update boards that aren't in
        // `current_boards`, even if they're in inventory and outdated.
        let collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Sled, 0), ARTIFACT_VERSION_1)]),
            ExpectedVersion::NoValidVersion,
        );
        let updates = plan_mgs_updates(
            log,
            &collection,
            &BTreeSet::new(),
            &PendingMgsUpdates::new(),
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert!(updates.is_empty());
        let updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &PendingMgsUpdates::new(),
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        // We verified most of the details above.  Here we're just double
        // checking that the baseboard being missing is the only reason that no
        // update was generated.
        assert_eq!(updates.len(), 1);

        // Verify the precondition details of an ordinary RoT update.
        let old_update =
            updates.into_iter().next().expect("at least one update");
        let PendingMgsUpdateDetails::Rot {
            expected_active_slot: old_expected_active_slot,
            expected_inactive_version: old_expected_inactive_version,
            ..
        } = &old_update.details
        else {
            panic!("expected RoT update");
        };
        assert_eq!(ARTIFACT_VERSION_1, old_expected_active_slot.version());
        assert_eq!(
            ExpectedVersion::NoValidVersion,
            *old_expected_inactive_version
        );

        // Test that if the inactive slot contents have changed, then we'll get
        // a new update reflecting that.
        let collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::Version(ARTIFACT_VERSION_1),
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Sled, 0), ARTIFACT_VERSION_1)]),
            ExpectedVersion::Version(ARTIFACT_VERSION_1),
        );
        let new_updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert_ne!(updates, new_updates);
        assert_eq!(new_updates.len(), 1);
        let new_update =
            new_updates.into_iter().next().expect("at least one update");
        assert_eq!(old_update.baseboard_id, new_update.baseboard_id);
        assert_eq!(old_update.sp_type, new_update.sp_type);
        assert_eq!(old_update.slot_id, new_update.slot_id);
        assert_eq!(old_update.artifact_hash, new_update.artifact_hash);
        assert_eq!(old_update.artifact_version, new_update.artifact_version);
        let PendingMgsUpdateDetails::Rot {
            expected_active_slot: new_expected_active_slot,
            expected_inactive_version: new_expected_inactive_version,
            ..
        } = &new_update.details
        else {
            panic!("expected RoT update");
        };
        assert_eq!(ARTIFACT_VERSION_1, new_expected_active_slot.version());
        assert_eq!(
            ExpectedVersion::Version(ARTIFACT_VERSION_1),
            *new_expected_inactive_version
        );

        // Test that if instead it's the active slot whose contents have changed
        // to something other than the new expected version, then we'll also get
        // a new update reflecting that.
        let collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Sled, 0), ARTIFACT_VERSION_1_5)]),
            ExpectedVersion::NoValidVersion,
        );
        let new_updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert_ne!(updates, new_updates);
        assert_eq!(new_updates.len(), 1);
        let new_update =
            new_updates.into_iter().next().expect("at least one update");
        assert_eq!(old_update.baseboard_id, new_update.baseboard_id);
        assert_eq!(old_update.sp_type, new_update.sp_type);
        assert_eq!(old_update.slot_id, new_update.slot_id);
        assert_eq!(old_update.artifact_hash, new_update.artifact_hash);
        assert_eq!(old_update.artifact_version, new_update.artifact_version);
        let PendingMgsUpdateDetails::Rot {
            expected_active_slot: new_expected_active_slot,
            expected_inactive_version: new_expected_inactive_version,
            ..
        } = &new_update.details
        else {
            panic!("expected RoT update");
        };
        assert_eq!(ARTIFACT_VERSION_1_5, new_expected_active_slot.version());
        assert_eq!(
            ExpectedVersion::NoValidVersion,
            *new_expected_inactive_version
        );

        logctx.cleanup_successful();
    }

    // Confirm our behavior for impossible updates
    #[test]
    fn test_impossible_update_policy() {
        let logctx = LogContext::new(
            "planning_mgs_updates_impossible_update_policy",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let log = &logctx.log;

        // Initial setup: sled 0 has active version 1 and inactive version 1.5.
        let collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Sled, 0), ARTIFACT_VERSION_1)]),
            ExpectedVersion::Version(ARTIFACT_VERSION_1_5),
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::Version(ARTIFACT_VERSION_1_5),
        );
        let current_boards = &collection.baseboards;
        let initial_updates = PendingMgsUpdates::new();
        let nmax_updates = 1;
        let repo = make_tuf_repo();

        // We should attempt to update this SP to version 2 no matter what our
        // impossible update policy is; we have no updates at all, currently!
        //
        // We stash the updates from either iteration into this `updates` value;
        // they're both the same.
        let mut updates = None;
        for impossible_update_policy in ImpossibleUpdatePolicy::iter() {
            let planned_updates = plan_mgs_updates(
                log,
                &collection,
                current_boards,
                &initial_updates,
                &TargetReleaseDescription::TufRepo(repo.clone()),
                nmax_updates,
                impossible_update_policy,
            );
            assert_eq!(planned_updates.len(), 1);
            let first_update =
                planned_updates.iter().next().expect("at least one update");
            assert_eq!(first_update.baseboard_id.serial_number, "sled_0");
            assert_eq!(first_update.sp_type, SpType::Sled);
            assert_eq!(first_update.slot_id, 0);
            assert_eq!(first_update.artifact_hash, ARTIFACT_HASH_SP_GIMLET_D);
            assert_eq!(first_update.artifact_version, ARTIFACT_VERSION_2);
            let PendingMgsUpdateDetails::Sp {
                expected_active_version,
                expected_inactive_version,
            } = &first_update.details
            else {
                panic!("expected SP update");
            };
            assert_eq!(*expected_active_version, ARTIFACT_VERSION_1);
            assert_eq!(
                *expected_inactive_version,
                ExpectedVersion::Version(ARTIFACT_VERSION_1_5)
            );
            updates = Some(planned_updates);
        }
        let updates = updates.unwrap();

        // Create a new collection that differs from the original collection in
        // that sled 0's inactive slot has no valid version. This emulates an
        // update in progress; we've partially written the contents, so there is
        // no caboose to read.
        let collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Sled, 0), ARTIFACT_VERSION_1)]),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::Version(ARTIFACT_VERSION_1_5),
        );

        // If we plan with `ImpossibleUpdatePolicy::Keep`, we should _not_
        // replace the update, even though its preconditions are no longer
        // valid.
        let keep_updates = plan_mgs_updates(
            log,
            &collection,
            current_boards,
            &updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            ImpossibleUpdatePolicy::Keep,
        );
        assert_eq!(updates, keep_updates);

        // On the other hand, if we plan with
        // `ImpossibleUpdatePolicy::Reevaluate`, we should replace the update.
        let reeval_updates = plan_mgs_updates(
            log,
            &collection,
            current_boards,
            &initial_updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            ImpossibleUpdatePolicy::Keep,
        );
        assert_eq!(reeval_updates.len(), 1);
        let first_update =
            reeval_updates.iter().next().expect("at least one update");
        assert_eq!(first_update.baseboard_id.serial_number, "sled_0");
        assert_eq!(first_update.sp_type, SpType::Sled);
        assert_eq!(first_update.slot_id, 0);
        assert_eq!(first_update.artifact_hash, ARTIFACT_HASH_SP_GIMLET_D);
        assert_eq!(first_update.artifact_version, ARTIFACT_VERSION_2);
        let PendingMgsUpdateDetails::Sp {
            expected_active_version,
            expected_inactive_version,
        } = &first_update.details
        else {
            panic!("expected SP update");
        };
        assert_eq!(*expected_active_version, ARTIFACT_VERSION_1);
        // This is the only field that should have changed:
        assert_eq!(*expected_inactive_version, ExpectedVersion::NoValidVersion);

        logctx.cleanup_successful();
    }

    // Updates a whole system's SPs one at a time
    #[test]
    fn test_whole_system_sequential() {
        let logctx = LogContext::new(
            "planning_mgs_updates_whole_system_sequential",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let log = &logctx.log;
        let mut latest_updates = PendingMgsUpdates::new();
        let repo = make_tuf_repo();
        let nmax_updates = 1;
        let impossible_update_policy = ImpossibleUpdatePolicy::Reevaluate;

        // Maintain a map of SPs and RoTs that we've updated.  We'll use this to
        // configure the inventory collection that we create at each step.
        let mut sp_exceptions = BTreeMap::new();
        let mut rot_exceptions = BTreeMap::new();

        // We do not control the order of updates.  But we expect to update each
        // of the SPs in this map.  When we do, we expect to find the given
        // artifact.
        let mut expected_updates: BTreeMap<_, _> = test_config()
            .into_iter()
            .map(|(k, (serial, board_name))| {
                if board_name == "oxide-rot-1" {
                    let kind = match k.0 {
                        SpType::Sled => ArtifactKind::GIMLET_ROT_IMAGE_B,
                        SpType::Power => ArtifactKind::PSC_ROT_IMAGE_B,
                        SpType::Switch => ArtifactKind::SWITCH_ROT_IMAGE_B,
                    };
                    (k, (serial, test_artifact_for_artifact_kind(kind)))
                } else {
                    (k, (serial, test_artifact_for_board(board_name)))
                }
            })
            .collect();

        for _ in 0..expected_updates.len() {
            // Generate an inventory collection reflecting that everything is at
            // version 1 except for what we've already updated.
            let collection = make_collection(
                ARTIFACT_VERSION_1,
                &sp_exceptions,
                ExpectedVersion::NoValidVersion,
                ARTIFACT_VERSION_1,
                &rot_exceptions,
                ExpectedVersion::NoValidVersion,
            );

            // For this test, all systems that are found in inventory are part
            // of the control plane.
            let current_boards = &collection.baseboards;

            // Run the planner and verify that we got one of our expected
            // updates.
            let new_updates = plan_mgs_updates(
                log,
                &collection,
                current_boards,
                &latest_updates,
                &TargetReleaseDescription::TufRepo(repo.clone()),
                nmax_updates,
                impossible_update_policy,
            );
            assert_eq!(new_updates.len(), 1);
            let update =
                new_updates.iter().next().expect("at least one update");
            verify_one_sp_update(&mut expected_updates, update);

            // Update `exceptions` or `rot_exceptions` for the next iteration.
            let sp_type = update.sp_type;
            let sp_slot = update.slot_id;
            match update.details {
                PendingMgsUpdateDetails::Rot { .. } => {
                    assert!(
                        rot_exceptions
                            .insert((sp_type, sp_slot), ARTIFACT_VERSION_2)
                            .is_none()
                    );
                }
                PendingMgsUpdateDetails::Sp { .. } => {
                    assert!(
                        sp_exceptions
                            .insert((sp_type, sp_slot), ARTIFACT_VERSION_2)
                            .is_none()
                    );
                }
                PendingMgsUpdateDetails::RotBootloader { .. } => {
                    unimplemented!()
                }
            }

            latest_updates = new_updates;
        }

        // Take one more lap.  It should reflect zero updates.
        let collection = make_collection(
            ARTIFACT_VERSION_1,
            &sp_exceptions,
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_1,
            &rot_exceptions,
            ExpectedVersion::NoValidVersion,
        );
        let last_updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &latest_updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert!(last_updates.is_empty());

        logctx.cleanup_successful();
    }

    // Updates a whole system's SPs at once
    #[test]
    fn test_whole_system_simultaneous() {
        let logctx = LogContext::new(
            "planning_mgs_updates_whole_system_simultaneous",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let log = &logctx.log;
        let repo = make_tuf_repo();

        let mut expected_updates: BTreeMap<_, _> = test_config()
            .into_iter()
            .map(|(k, (serial, board_name))| {
                if board_name == "oxide-rot-1" {
                    let kind = match k.0 {
                        SpType::Sled => ArtifactKind::GIMLET_ROT_IMAGE_B,
                        SpType::Power => ArtifactKind::PSC_ROT_IMAGE_B,
                        SpType::Switch => ArtifactKind::SWITCH_ROT_IMAGE_B,
                    };
                    (k, (serial, test_artifact_for_artifact_kind(kind)))
                } else {
                    (k, (serial, test_artifact_for_board(board_name)))
                }
            })
            .collect();

        // Update the whole system at once.
        let impossible_update_policy = ImpossibleUpdatePolicy::Reevaluate;
        let collection = make_collection(
            ARTIFACT_VERSION_1,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_1,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
        );
        let all_updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &PendingMgsUpdates::new(),
            &TargetReleaseDescription::TufRepo(repo.clone()),
            usize::MAX,
            impossible_update_policy,
        );
        // `all_updates` counts each update per SpType. This means an update for
        // SP and RoT for the same SpType count as a single update. For
        // `expected_updates`, each component update counts as an update, so the
        // amount of `all_updates` should be half of `expected_updates`.
        assert_eq!(all_updates.len(), expected_updates.len() / 2);
        for update in &all_updates {
            verify_one_sp_update(&mut expected_updates, update);
        }

        // Now, notice when they've all been updated, even if the limit is only
        // one.
        let collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::new(),
            ExpectedVersion::NoValidVersion,
        );
        let all_updates_done = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &all_updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            1,
            impossible_update_policy,
        );
        assert!(all_updates_done.is_empty());

        logctx.cleanup_successful();
    }

    fn verify_one_sp_update(
        expected_updates: &mut BTreeMap<
            (SpType, u16, MgsUpdateComponent),
            (&str, ArtifactHash),
        >,
        update: &PendingMgsUpdate,
    ) {
        let sp_type = update.sp_type;
        let sp_slot = update.slot_id;
        let component = match &update.details {
            PendingMgsUpdateDetails::Rot { .. } => MgsUpdateComponent::Rot,
            PendingMgsUpdateDetails::RotBootloader { .. } => {
                MgsUpdateComponent::RotBootloader
            }
            PendingMgsUpdateDetails::Sp { .. } => MgsUpdateComponent::Sp,
        };
        println!("found update: {} slot {}", sp_type, sp_slot);
        let (expected_serial, expected_artifact) = expected_updates
            .remove(&(sp_type, sp_slot, component))
            .expect("unexpected update");
        assert_eq!(update.artifact_hash, expected_artifact);
        assert_eq!(update.artifact_version, ARTIFACT_VERSION_2);
        assert_eq!(update.baseboard_id.serial_number, *expected_serial);
        let (expected_active_version, expected_inactive_version) = match &update
            .details
        {
            PendingMgsUpdateDetails::Rot {
                expected_active_slot,
                expected_inactive_version,
                ..
            } => (&expected_active_slot.version, expected_inactive_version),
            PendingMgsUpdateDetails::Sp {
                expected_active_version,
                expected_inactive_version,
            } => (expected_active_version, expected_inactive_version),
            PendingMgsUpdateDetails::RotBootloader { .. } => unimplemented!(),
        };
        assert_eq!(*expected_active_version, ARTIFACT_VERSION_1);
        assert_eq!(*expected_inactive_version, ExpectedVersion::NoValidVersion);
    }

    // Tests the case where an SP appears to move while an update is pending
    #[test]
    fn test_sp_move() {
        let logctx = LogContext::new(
            "planning_mgs_updates_whole_system_simultaneous",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );

        // Configure an update for one SP.
        let log = &logctx.log;
        let repo = make_tuf_repo();
        let mut collection = make_collection(
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Sled, 0), ARTIFACT_VERSION_1)]),
            ExpectedVersion::NoValidVersion,
            ARTIFACT_VERSION_2,
            &BTreeMap::from([((SpType::Sled, 0), ARTIFACT_VERSION_1)]),
            ExpectedVersion::NoValidVersion,
        );
        let nmax_updates = 1;
        let impossible_update_policy = ImpossibleUpdatePolicy::Reevaluate;
        let updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &PendingMgsUpdates::new(),
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert!(!updates.is_empty());
        let update = updates.into_iter().next().expect("at least one update");

        // Move an SP (as if someone had moved the sled to a different cubby).
        // This is awful, but at least it's easy.
        let sp_info = collection
            .sps
            .values_mut()
            .find(|sp| sp.sp_type == SpType::Sled && sp.sp_slot == 0)
            .expect("missing sled 0 SP");
        sp_info.sp_slot = 9;

        // Plan again.  The configured update should be updated to reflect the
        // new location.
        let new_updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &updates,
            &TargetReleaseDescription::TufRepo(repo.clone()),
            nmax_updates,
            impossible_update_policy,
        );
        assert!(!new_updates.is_empty());
        let new_update =
            new_updates.into_iter().next().expect("at least one update");
        assert_ne!(new_update.slot_id, update.slot_id);
        assert_eq!(new_update.baseboard_id, update.baseboard_id);
        assert_eq!(new_update.sp_type, update.sp_type);
        assert_eq!(new_update.artifact_hash, update.artifact_hash);
        assert_eq!(new_update.artifact_version, update.artifact_version);
        assert_eq!(new_update.details, update.details);

        logctx.cleanup_successful();
    }
}
