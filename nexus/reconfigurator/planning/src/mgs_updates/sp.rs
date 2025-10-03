// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for making choices about SP updates

use super::MgsUpdateStatus;
use super::mgs_update_status_inactive_versions;
use crate::mgs_updates::MgsUpdateOutcome;

use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::deployment::PendingMgsUpdateSpDetails;
use nexus_types::deployment::planning_report::FailedSpUpdateReason;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::Collection;
use omicron_common::api::external::TufRepoDescription;
use slog::{debug, warn};
use std::collections::BTreeSet;
use std::sync::Arc;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::KnownArtifactKind;

/// Compares a configured SP update with information from inventory and
/// determines the current status of the update.  See `MgsUpdateStatus`.
pub(super) fn update_status(
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

/// Determine if the given baseboard needs an SP update and, if so, returns it.
/// An error means an update is still necessary but cannot be completed.
pub(super) fn try_make_update(
    log: &slog::Logger,
    baseboard_id: &Arc<BaseboardId>,
    inventory: &Collection,
    current_artifacts: &TufRepoDescription,
    safe_zone_boards: &BTreeSet<Arc<BaseboardId>>,
) -> Result<MgsUpdateOutcome, FailedSpUpdateReason> {
    let Some(sp_info) = inventory.sps.get(baseboard_id) else {
        return Err(FailedSpUpdateReason::SpNotInInventory);
    };

    let Some(active_caboose) =
        inventory.caboose_for(CabooseWhich::SpSlot0, baseboard_id)
    else {
        return Err(FailedSpUpdateReason::CabooseNotInInventory(
            CabooseWhich::SpSlot0,
        ));
    };

    let expected_active_version = match active_caboose.caboose.version.parse() {
        Ok(v) => v,
        Err(e) => {
            return Err(FailedSpUpdateReason::FailedVersionParse {
                caboose: CabooseWhich::SpSlot0,
                err: format!("{}", e),
            });
        }
    };

    let board = &active_caboose.caboose.board;
    let matching_artifacts: Vec<_> = current_artifacts
        .artifacts
        .iter()
        .filter(|a| {
            // A matching SP artifact will have:
            //
            // - "board" matching the board name (found above from caboose)
            // - "kind" matching one of the known SP kinds

            if a.board.as_ref() != Some(board) {
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
        return Err(FailedSpUpdateReason::NoMatchingArtifactFound);
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
        return Ok(MgsUpdateOutcome::NoUpdateNeeded);
    }

    // Make sure the board we're targetting doesn't contain any zones that are
    // unsafe to shut down
    if !safe_zone_boards.contains(baseboard_id) {
        return Err(FailedSpUpdateReason::UnsafeZoneFound);
    }

    // Begin configuring an update.
    let expected_inactive_version = match inventory
        .caboose_for(CabooseWhich::SpSlot1, baseboard_id)
        .map(|c| c.caboose.version.parse::<ArtifactVersion>())
        .transpose()
    {
        Ok(None) => ExpectedVersion::NoValidVersion,
        Ok(Some(v)) => ExpectedVersion::Version(v),
        Err(e) => {
            return Err(FailedSpUpdateReason::FailedVersionParse {
                caboose: CabooseWhich::SpSlot1,
                err: format!("{}", e),
            });
        }
    };

    Ok(MgsUpdateOutcome::pending_with_update_only(PendingMgsUpdate {
        baseboard_id: baseboard_id.clone(),
        sp_type: sp_info.sp_type,
        slot_id: sp_info.sp_slot,
        details: PendingMgsUpdateDetails::Sp(PendingMgsUpdateSpDetails {
            expected_active_version,
            expected_inactive_version,
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
    use crate::mgs_updates::test_helpers::ARTIFACT_HASH_SP_GIMLET_D;
    use crate::mgs_updates::test_helpers::ARTIFACT_HASH_SP_SIDECAR_C;
    use crate::mgs_updates::test_helpers::ARTIFACT_VERSION_1;
    use crate::mgs_updates::test_helpers::ARTIFACT_VERSION_1_5;
    use crate::mgs_updates::test_helpers::ARTIFACT_VERSION_2;
    use crate::mgs_updates::test_helpers::TestBoards;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingLevel;
    use dropshot::test_util::LogContext;
    use nexus_types::deployment::ExpectedVersion;
    use nexus_types::deployment::PendingMgsUpdateDetails;
    use nexus_types::deployment::PendingMgsUpdateSpDetails;
    use nexus_types::deployment::PendingMgsUpdates;
    use nexus_types::deployment::TargetReleaseDescription;
    use nexus_types::inventory::SpType;
    use std::collections::BTreeSet;

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
            .collection_builder()
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
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
        let PlannedMgsUpdates { pending_updates: updates, .. } =
            plan_mgs_updates(
                log,
                &collection,
                current_boards,
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
        let PlannedMgsUpdates { pending_updates: later_updates, .. } =
            plan_mgs_updates(
                log,
                &collection,
                current_boards,
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
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .sp_active_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .build();
        let PlannedMgsUpdates { pending_updates: later_updates, .. } =
            plan_mgs_updates(
                log,
                &later_collection,
                current_boards,
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
            .collection_builder()
            .sp_active_version_exception(SpType::Switch, 1, ARTIFACT_VERSION_1)
            .build();
        let PlannedMgsUpdates { pending_updates: later_updates, .. } =
            plan_mgs_updates(
                log,
                &later_collection,
                current_boards,
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
        let updated_collection = test_boards.collection_builder().build();
        let PlannedMgsUpdates { pending_updates: later_updates, .. } =
            plan_mgs_updates(
                log,
                &updated_collection,
                current_boards,
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
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
        let PlannedMgsUpdates { pending_updates: updates, .. } =
            plan_mgs_updates(
                log,
                &collection,
                &BTreeSet::new(),
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
        let PendingMgsUpdateDetails::Sp(PendingMgsUpdateSpDetails {
            expected_active_version: old_expected_active_version,
            expected_inactive_version: old_expected_inactive_version,
        }) = &old_update.details
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
            .collection_builder()
            .sp_versions(
                ARTIFACT_VERSION_2,
                ExpectedVersion::Version(ARTIFACT_VERSION_1),
            )
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .build();
        let PlannedMgsUpdates { pending_updates: new_updates, .. } =
            plan_mgs_updates(
                log,
                &collection,
                &collection.baseboards,
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
        let PendingMgsUpdateDetails::Sp(PendingMgsUpdateSpDetails {
            expected_active_version: new_expected_active_version,
            expected_inactive_version: new_expected_inactive_version,
        }) = &new_update.details
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
            .collection_builder()
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1_5)
            .build();
        let PlannedMgsUpdates { pending_updates: new_updates, .. } =
            plan_mgs_updates(
                log,
                &collection,
                &collection.baseboards,
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
        let PendingMgsUpdateDetails::Sp(PendingMgsUpdateSpDetails {
            expected_active_version: new_expected_active_version,
            expected_inactive_version: new_expected_inactive_version,
        }) = &new_update.details
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
}
