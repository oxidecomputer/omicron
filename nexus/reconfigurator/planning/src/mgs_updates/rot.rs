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
use nexus_types::deployment::PendingMgsUpdateRotDetails;
use nexus_types::deployment::planning_report::FailedMgsUpdateReason;
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
        || expected_persistent_boot_preference != expected_active_slot.slot
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
/// An error means an update is still necessary but cannot be completed.
pub fn try_make_update_rot(
    log: &slog::Logger,
    baseboard_id: &Arc<BaseboardId>,
    inventory: &Collection,
    current_artifacts: &TufRepoDescription,
// TODO-K: Like the Host OS, use an enum here as the return type as suggested in
// https://github.com/oxidecomputer/omicron/pull/9001#discussion_r2372837627
) -> Result<Option<PendingMgsUpdate>, FailedMgsUpdateReason> {
    let Some(sp_info) = inventory.sps.get(baseboard_id) else {
        return Err(FailedMgsUpdateReason::SpNotInInventory);
    };

    let Some(rot_state) = inventory.rots.get(baseboard_id) else {
        return Err(FailedMgsUpdateReason::RotStateNotInInventory);
    };

    let active_slot = rot_state.active_slot;

    let active_caboose_which = CabooseWhich::from_rot_slot(active_slot);
    let Some(active_caboose) =
        inventory.caboose_for(active_caboose_which, baseboard_id)
    else {
        return Err(FailedMgsUpdateReason::CabooseNotInInventory(
            active_caboose_which,
        ));
    };

    let expected_active_version = match active_caboose.caboose.version.parse() {
        Ok(v) => v,
        Err(e) => {
            return Err(FailedMgsUpdateReason::FailedVersionParse {
                caboose: active_caboose_which,
                err: format!("{}", e),
            });
        }
    };

    let board = &active_caboose.caboose.board;
    let Some(rkth) = &active_caboose.caboose.sign else {
        return Err(FailedMgsUpdateReason::CabooseMissingSign(
            active_caboose_which,
        ));
    };

    let matching_artifacts: Vec<_> = current_artifacts
        .artifacts
        .iter()
        .filter(|a| {
            // A matching RoT artifact will have:
            //
            // - "board" matching the board name (found above from caboose)
            // - "kind" matching one of the known RoT kinds
            // - "sign" matching the rkth (found above from caboose)

            if a.board.as_ref() != Some(board) {
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
        return Err(FailedMgsUpdateReason::NoMatchingArtifactFound);
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
        return Ok(None);
    }

    let expected_active_slot = ExpectedActiveRotSlot {
        slot: active_slot,
        version: expected_active_version,
    };

    // Begin configuring an update.
    let inactive_caboose_which =
        CabooseWhich::from_rot_slot(active_slot.toggled());
    let expected_inactive_version = match inventory
        .caboose_for(inactive_caboose_which, baseboard_id)
        .map(|c| c.caboose.version.parse::<ArtifactVersion>())
        .transpose()
    {
        Ok(None) => ExpectedVersion::NoValidVersion,
        Ok(Some(v)) => ExpectedVersion::Version(v),
        Err(e) => {
            return Err(FailedMgsUpdateReason::FailedVersionParse {
                caboose: inactive_caboose_which,
                err: format!("{}", e),
            });
        }
    };

    Ok(Some(PendingMgsUpdate {
        baseboard_id: baseboard_id.clone(),
        sp_type: sp_info.sp_type,
        slot_id: sp_info.sp_slot,
        details: PendingMgsUpdateDetails::Rot(PendingMgsUpdateRotDetails {
            expected_active_slot,
            expected_inactive_version,
            expected_persistent_boot_preference: rot_state
                .persistent_boot_preference,
            expected_pending_persistent_boot_preference: rot_state
                .pending_persistent_boot_preference,
            expected_transient_boot_preference: rot_state
                .transient_boot_preference,
        }),
        artifact_hash: artifact.hash,
        artifact_version: artifact.id.version.clone(),
    }))
}

#[cfg(test)]
mod tests {
    use crate::mgs_updates::ImpossibleUpdatePolicy;
    use crate::mgs_updates::PlannedMgsUpdates;
    use crate::mgs_updates::plan_mgs_updates;
    use crate::mgs_updates::test_helpers::ARTIFACT_HASH_ROT_GIMLET_B;
    use crate::mgs_updates::test_helpers::ARTIFACT_HASH_ROT_SWITCH_B;
    use crate::mgs_updates::test_helpers::ARTIFACT_VERSION_1;
    use crate::mgs_updates::test_helpers::ARTIFACT_VERSION_1_5;
    use crate::mgs_updates::test_helpers::ARTIFACT_VERSION_2;
    use crate::mgs_updates::test_helpers::TestBoards;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingLevel;
    use dropshot::test_util::LogContext;
    use gateway_client::types::SpType;
    use gateway_types::rot::RotSlot;
    use nexus_types::deployment::ExpectedVersion;
    use nexus_types::deployment::PendingMgsUpdateDetails;
    use nexus_types::deployment::PendingMgsUpdateRotDetails;
    use nexus_types::deployment::PendingMgsUpdates;
    use nexus_types::deployment::TargetReleaseDescription;
    use std::collections::BTreeSet;

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
            .collection_builder()
            .rot_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
        let current_boards = &collection.baseboards;
        let initial_updates = PendingMgsUpdates::new();
        let nmax_updates = 1;
        let impossible_update_policy = ImpossibleUpdatePolicy::Reevaluate;
        let PlannedMgsUpdates { pending_updates: updates, .. } =
            plan_mgs_updates(
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
        let PlannedMgsUpdates { pending_updates: updates, .. } =
            plan_mgs_updates(
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
        let PlannedMgsUpdates { pending_updates: later_updates, .. } =
            plan_mgs_updates(
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
            .collection_builder()
            .rot_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .rot_active_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .build();
        let PlannedMgsUpdates { pending_updates: later_updates, .. } =
            plan_mgs_updates(
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
        // completes, then the second one *is* started.  This tests four
        // different things: first that we noticed the first one completed,
        // second that we noticed another thing needed an update, third that
        // an update is correctly configured after the active slot has changed
        // from the previous component update, and fourth that the planner
        // schedules the updates in the correct order: first RoT, and second SP.
        let later_collection = test_boards
            .collection_builder()
            .sp_active_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .rot_active_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .rot_active_slot_exception(SpType::Sled, 0, RotSlot::B)
            .rot_persistent_boot_preference_exception(
                SpType::Sled,
                0,
                RotSlot::B,
            )
            .build();
        let PlannedMgsUpdates { pending_updates: later_updates, .. } =
            plan_mgs_updates(
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
        let updated_collection = test_boards.collection_builder().build();
        let PlannedMgsUpdates { pending_updates: later_updates, .. } =
            plan_mgs_updates(
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
            .collection_builder()
            .rot_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
        let PlannedMgsUpdates { pending_updates: updates, .. } =
            plan_mgs_updates(
                log,
                &collection,
                &BTreeSet::new(),
                &PendingMgsUpdates::new(),
                &TargetReleaseDescription::TufRepo(repo.clone()),
                nmax_updates,
                impossible_update_policy,
            );
        assert!(updates.is_empty());
        let PlannedMgsUpdates { pending_updates: updates, .. } =
            plan_mgs_updates(
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
        let PendingMgsUpdateDetails::Rot(PendingMgsUpdateRotDetails {
            expected_active_slot: old_expected_active_slot,
            expected_inactive_version: old_expected_inactive_version,
            ..
        }) = &old_update.details
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
            .collection_builder()
            .sp_versions(
                ARTIFACT_VERSION_2,
                ExpectedVersion::Version(ARTIFACT_VERSION_1),
            )
            .rot_versions(
                ARTIFACT_VERSION_2,
                ExpectedVersion::Version(ARTIFACT_VERSION_1),
            )
            .stage0_versions(
                ARTIFACT_VERSION_2,
                ExpectedVersion::Version(ARTIFACT_VERSION_1),
            )
            .rot_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
        let PlannedMgsUpdates { pending_updates: new_updates, .. } =
            plan_mgs_updates(
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
        let PendingMgsUpdateDetails::Rot(PendingMgsUpdateRotDetails {
            expected_active_slot: new_expected_active_slot,
            expected_inactive_version: new_expected_inactive_version,
            ..
        }) = &new_update.details
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
            .collection_builder()
            .rot_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1_5)
            .build();
        let PlannedMgsUpdates { pending_updates: new_updates, .. } =
            plan_mgs_updates(
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
        let PendingMgsUpdateDetails::Rot(PendingMgsUpdateRotDetails {
            expected_active_slot: new_expected_active_slot,
            expected_inactive_version: new_expected_inactive_version,
            ..
        }) = &new_update.details
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
}
