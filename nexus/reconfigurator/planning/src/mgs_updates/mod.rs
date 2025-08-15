// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for making choices about MGS-managed updates

mod rot;
mod rot_bootloader;
mod sp;

use crate::mgs_updates::rot::RotUpdateState;
use crate::mgs_updates::rot::mgs_update_status_rot;
use crate::mgs_updates::rot::try_make_update_rot;
use crate::mgs_updates::rot_bootloader::mgs_update_status_rot_bootloader;
use crate::mgs_updates::rot_bootloader::try_make_update_rot_bootloader;
use crate::mgs_updates::sp::mgs_update_status_sp;
use crate::mgs_updates::sp::try_make_update_sp;

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
use slog::{error, info, warn};
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::sync::Arc;
use thiserror::Error;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::ArtifactVersionError;

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
    #[error("not yet implemented")]
    NotYetImplemented,
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
        PendingMgsUpdateDetails::HostPhase1(_) => {
            return Err(MgsUpdateStatusError::NotYetImplemented);
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
    // We try MGS-driven update components in a hardcoded priority order until
    // any of them returns `Some`. The order is described in RFD 565 section
    // "Update Sequence". For now, we only plan SP, RoT and RoT bootloader
    // updates. When implemented, host OS updates will be the last to try.
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

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod test {
    use super::ImpossibleUpdatePolicy;
    use super::plan_mgs_updates;
    use super::test_helpers::ARTIFACT_HASH_ROT_BOOTLOADER_GIMLET;
    use super::test_helpers::ARTIFACT_HASH_ROT_BOOTLOADER_SWITCH;
    use super::test_helpers::ARTIFACT_HASH_ROT_GIMLET_B;
    use super::test_helpers::ARTIFACT_HASH_ROT_SWITCH_B;
    use super::test_helpers::ARTIFACT_HASH_SP_GIMLET_D;
    use super::test_helpers::ARTIFACT_HASH_SP_SIDECAR_C;
    use super::test_helpers::ARTIFACT_VERSION_1;
    use super::test_helpers::ARTIFACT_VERSION_1_5;
    use super::test_helpers::ARTIFACT_VERSION_2;
    use super::test_helpers::TestBoards;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingLevel;
    use gateway_client::types::SpType;
    use nexus_types::deployment::ExpectedVersion;
    use nexus_types::deployment::PendingMgsUpdateDetails;
    use nexus_types::deployment::PendingMgsUpdates;
    use nexus_types::deployment::TargetReleaseDescription;
    use omicron_test_utils::dev::LogContext;
    use std::collections::BTreeSet;
    use strum::IntoEnumIterator;

    // Short hand-rolled update sequence that exercises some basic behavior for
    // SP updates.
    #[test]
    fn test_basic_sp() {
        let test_name = "planning_mgs_updates_basic_sp";
        let logctx = LogContext::new(
            test_name,
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let log = &logctx.log;
        let test_boards = TestBoards::new(test_name);

        // Test that with no updates pending and no TUF repo specified, there
        // will remain no updates pending.
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
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
        let repo = test_boards.tuf_repo();
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
        let later_collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .sp_active_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .build();
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
        let later_collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .sp_active_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .build();
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
        let updated_collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .build();
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
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
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
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .sp_versions(
                ARTIFACT_VERSION_2,
                ExpectedVersion::Version(ARTIFACT_VERSION_1),
            )
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
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
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1_5)
            .build();
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
        let test_name = "planning_mgs_updates_basic_rot";
        let logctx = LogContext::new(
            test_name,
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let log = &logctx.log;
        let test_boards = TestBoards::new(test_name);

        // Test that with no updates pending and no TUF repo specified, there
        // will remain no updates pending.
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .rot_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
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
        let repo = test_boards.tuf_repo();
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
        let later_collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .rot_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .rot_active_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .build();
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
        let later_collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .sp_active_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .rot_active_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .build();
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
        let updated_collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .build();
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
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .rot_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
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
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::Version(ARTIFACT_VERSION_1),
            )
            .rot_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
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
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .rot_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1_5)
            .build();
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

    // Short hand-rolled update sequence that exercises some basic behavior for
    // RoT bootloader updates.
    #[test]
    fn test_basic_rot_bootloader() {
        let test_name = "planning_mgs_updates_basic_rot_bootloader";
        let logctx = LogContext::new(
            test_name,
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let log = &logctx.log;
        let test_boards = TestBoards::new(test_name);

        // Test that with no updates pending and no TUF repo specified, there
        // will remain no updates pending.
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .stage0_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
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
        let repo = test_boards.tuf_repo();
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
        assert_eq!(
            first_update.artifact_hash,
            ARTIFACT_HASH_ROT_BOOTLOADER_GIMLET
        );
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
        let later_collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .stage0_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .stage0_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .build();
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
        // the planner schedules the updates in the correct order: first RoT
        // bootloader, second RoT and third SP.
        let later_collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .sp_active_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .rot_active_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .stage0_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .build();
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
        assert_eq!(
            next_update.artifact_hash,
            ARTIFACT_HASH_ROT_BOOTLOADER_SWITCH
        );
        assert_eq!(next_update.artifact_version, ARTIFACT_VERSION_2);

        // Finally, test that when all components are in spec, then no updates
        // are configured.
        let updated_collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .build();
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
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .stage0_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
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
        let PendingMgsUpdateDetails::RotBootloader {
            expected_stage0_version: old_expected_stage0_version,
            expected_stage0_next_version: old_expected_stage0_next_version,
        } = &old_update.details
        else {
            panic!("expected RoT bootloader update");
        };
        assert_eq!(ARTIFACT_VERSION_1, *old_expected_stage0_version);
        assert_eq!(
            ExpectedVersion::NoValidVersion,
            *old_expected_stage0_next_version
        );

        // Test that if the inactive slot contents have changed, then we'll get
        // a new update reflecting that.
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .stage0_versions(
                ARTIFACT_VERSION_2,
                ExpectedVersion::Version(ARTIFACT_VERSION_1),
            )
            .stage0_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
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
        let PendingMgsUpdateDetails::RotBootloader {
            expected_stage0_version: new_expected_stage0_version,
            expected_stage0_next_version: new_expected_stage0_next_version,
        } = &new_update.details
        else {
            panic!("expected RoT bootloader update");
        };
        assert_eq!(ARTIFACT_VERSION_1, *new_expected_stage0_version);
        assert_eq!(
            ExpectedVersion::Version(ARTIFACT_VERSION_1),
            *new_expected_stage0_next_version
        );

        // Test that if instead it's the active slot whose contents have changed
        // to something other than the new expected version, then we'll also get
        // a new update reflecting that.
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .stage0_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1_5)
            .build();
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
        let PendingMgsUpdateDetails::RotBootloader {
            expected_stage0_version: new_expected_stage0_version,
            expected_stage0_next_version: new_expected_stage0_next_version,
        } = &new_update.details
        else {
            panic!("expected RoT bootloader update");
        };
        assert_eq!(ARTIFACT_VERSION_1_5, *new_expected_stage0_version);
        assert_eq!(
            ExpectedVersion::NoValidVersion,
            *new_expected_stage0_next_version
        );

        logctx.cleanup_successful();
    }

    // Confirm our behavior for impossible updates
    #[test]
    fn test_impossible_update_policy() {
        let test_name = "planning_mgs_updates_impossible_update_policy";
        let logctx = LogContext::new(
            test_name,
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let log = &logctx.log;
        let test_boards = TestBoards::new(test_name);

        // Initial setup: sled 0 has active version 1 and inactive version 1.5.
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::Version(ARTIFACT_VERSION_1_5),
            )
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
        let current_boards = &collection.baseboards;
        let initial_updates = PendingMgsUpdates::new();
        let nmax_updates = 1;
        let repo = test_boards.tuf_repo();

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
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .rot_versions(
                ARTIFACT_VERSION_2,
                ExpectedVersion::Version(ARTIFACT_VERSION_1_5),
            )
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();

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
        let test_name = "planning_mgs_updates_whole_system_sequential";
        let logctx = LogContext::new(
            test_name,
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let log = &logctx.log;
        let test_boards = TestBoards::new(test_name);
        let mut latest_updates = PendingMgsUpdates::new();
        let repo = test_boards.tuf_repo();
        let nmax_updates = 1;
        let impossible_update_policy = ImpossibleUpdatePolicy::Reevaluate;

        // We do not control the order of updates.  But we expect to update each
        // of the SPs in this map.  When we do, we expect to find the given
        // artifact.
        let mut expected_updates = test_boards.expected_updates();

        // Start with collections that record everything at version 1. We'll add
        // exceptions as we step through updates below.
        let mut builder = test_boards.collection_builder(
            ARTIFACT_VERSION_1,
            ExpectedVersion::NoValidVersion,
        );
        for _ in 0..expected_updates.len() {
            let collection = builder.clone().build();

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
            expected_updates.verify_one(update);

            // Update our builder with an addition exception for the update we
            // just planned for the next iteration.
            let sp_type = update.sp_type;
            let sp_slot = update.slot_id;
            match update.details {
                PendingMgsUpdateDetails::Rot { .. } => {
                    assert!(
                        !builder
                            .has_rot_active_version_exception(sp_type, sp_slot)
                    );
                    builder = builder.rot_active_version_exception(
                        sp_type,
                        sp_slot,
                        ARTIFACT_VERSION_2,
                    );
                }
                PendingMgsUpdateDetails::Sp { .. } => {
                    assert!(
                        !builder
                            .has_sp_active_version_exception(sp_type, sp_slot)
                    );
                    builder = builder.sp_active_version_exception(
                        sp_type,
                        sp_slot,
                        ARTIFACT_VERSION_2,
                    );
                }
                PendingMgsUpdateDetails::RotBootloader { .. } => {
                    assert!(
                        !builder.has_stage0_version_exception(sp_type, sp_slot)
                    );
                    builder = builder.stage0_version_exception(
                        sp_type,
                        sp_slot,
                        ARTIFACT_VERSION_2,
                    );
                }
                PendingMgsUpdateDetails::HostPhase1(_) => {
                    unimplemented!()
                }
            }

            latest_updates = new_updates;
        }
        assert!(expected_updates.is_empty());

        // Take one more lap.  It should reflect zero updates.
        let collection = builder.build();
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

    // Updates as much of a whole system at once as we can
    #[test]
    fn test_whole_system_simultaneous() {
        let test_name = "planning_mgs_updates_whole_system_simultaneous";
        let logctx = LogContext::new(
            test_name,
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let log = &logctx.log;
        let test_boards = TestBoards::new(test_name);
        let repo = test_boards.tuf_repo();
        let impossible_update_policy = ImpossibleUpdatePolicy::Reevaluate;

        let mut expected_updates = test_boards.expected_updates();

        // Update the whole system at once; this should attempt to update all of
        // the RoT bootloaders, but stages at most one pending update per board.
        //
        // TODO THIS IS WRONG! We should only be willing to stage at most one
        // bootloader update at a time, across the whole system. This is
        // currently enforced by the fact that the real planner passes 1 instead
        // of usize::MAX, but we should probably fix this.
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_1,
                ExpectedVersion::NoValidVersion,
            )
            .build();
        let all_updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &PendingMgsUpdates::new(),
            &TargetReleaseDescription::TufRepo(repo.clone()),
            usize::MAX,
            impossible_update_policy,
        );

        for update in &all_updates {
            // Confirm all our updates are to RoT bootloaders.
            match &update.details {
                PendingMgsUpdateDetails::RotBootloader { .. } => (),
                PendingMgsUpdateDetails::Rot { .. }
                | PendingMgsUpdateDetails::Sp { .. }
                | PendingMgsUpdateDetails::HostPhase1(..) => {
                    panic!("unexpected update type: {update:?}")
                }
            }
            expected_updates.verify_one(update);
        }

        // Update the whole system at once again, but note the RoT bootloaders
        // have all been updated already; this should attempt to update all of
        // the RoTs.
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_1,
                ExpectedVersion::NoValidVersion,
            )
            .stage0_versions(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .build();
        let all_updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &PendingMgsUpdates::new(),
            &TargetReleaseDescription::TufRepo(repo.clone()),
            usize::MAX,
            impossible_update_policy,
        );
        for update in &all_updates {
            // Confirm all our updates are to RoTs.
            match &update.details {
                PendingMgsUpdateDetails::Rot { .. } => (),
                PendingMgsUpdateDetails::Sp { .. }
                | PendingMgsUpdateDetails::RotBootloader { .. }
                | PendingMgsUpdateDetails::HostPhase1(..) => {
                    panic!("unexpected update type: {update:?}")
                }
            }
            expected_updates.verify_one(update);
        }

        // Update the whole system at once again, but note the RoT bootloaders
        // and RoTs have all been updated already; this should attempt to update
        // all of the SPs.
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_1,
                ExpectedVersion::NoValidVersion,
            )
            .rot_versions(ARTIFACT_VERSION_2, ExpectedVersion::NoValidVersion)
            .stage0_versions(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .build();
        let all_updates = plan_mgs_updates(
            log,
            &collection,
            &collection.baseboards,
            &PendingMgsUpdates::new(),
            &TargetReleaseDescription::TufRepo(repo.clone()),
            usize::MAX,
            impossible_update_policy,
        );
        for update in &all_updates {
            // Confirm all our updates are to SPs.
            match &update.details {
                PendingMgsUpdateDetails::Sp { .. } => (),
                PendingMgsUpdateDetails::Rot { .. }
                | PendingMgsUpdateDetails::RotBootloader { .. }
                | PendingMgsUpdateDetails::HostPhase1(..) => {
                    panic!("unexpected update type: {update:?}")
                }
            }
            expected_updates.verify_one(update);
        }

        // We should have performed all expected updates.
        assert!(expected_updates.is_empty());

        // Now, notice when they've all been updated, even if the limit is only
        // one.
        let collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .build();
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

    // Tests the case where an SP appears to move while an update is pending
    #[test]
    fn test_sp_move() {
        let test_name = "planning_mgs_updates_whole_system_simultaneous";
        let logctx = LogContext::new(
            test_name,
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let test_boards = TestBoards::new(test_name);

        // Configure an update for one SP.
        let log = &logctx.log;
        let repo = test_boards.tuf_repo();
        let mut collection = test_boards
            .collection_builder(
                ARTIFACT_VERSION_2,
                ExpectedVersion::NoValidVersion,
            )
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .rot_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .stage0_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
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
