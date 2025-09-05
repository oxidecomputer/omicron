// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for making choices about SP updates

use super::MgsUpdateStatus;
use super::mgs_update_status_inactive_versions;

use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::deployment::PendingMgsUpdateSpDetails;
use nexus_types::deployment::planning_report::FailedMgsUpdateReason;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::Collection;
use omicron_common::api::external::TufRepoDescription;
use slog::{debug, warn};
use std::sync::Arc;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::KnownArtifactKind;

/// Compares a configured SP update with information from inventory and
/// determines the current status of the update.  See `MgsUpdateStatus`.
pub fn mgs_update_status_sp(
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
pub fn try_make_update_sp(
    log: &slog::Logger,
    baseboard_id: &Arc<BaseboardId>,
    inventory: &Collection,
    current_artifacts: &TufRepoDescription,
) -> Result<Option<PendingMgsUpdate>, FailedMgsUpdateReason> {
    let Some(sp_info) = inventory.sps.get(baseboard_id) else {
        warn!(
            log,
            "cannot configure SP update for board \
             (missing SP info from inventory)";
            baseboard_id
        );
        return Err(FailedMgsUpdateReason::SpNotInInventory);
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
        return Err(FailedMgsUpdateReason::CabooseNotInInventory);
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
        return Err(FailedMgsUpdateReason::FailedVersionParse);
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
        warn!(
            log,
            "cannot configure SP update for board (no matching artifact)";
            baseboard_id,
        );
        return Err(FailedMgsUpdateReason::NoMatchingArtifactFound);
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
        return Ok(None);
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
            return Err(FailedMgsUpdateReason::FailedVersionParse);
        }
    };

    Ok(Some(PendingMgsUpdate {
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
