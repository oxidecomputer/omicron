// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for making choices about RoT bootloader updates

use super::MgsUpdateStatus;
use super::mgs_update_status_inactive_versions;

use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::deployment::PendingMgsUpdateRotBootloaderDetails;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::Collection;
use omicron_common::api::external::TufRepoDescription;
use slog::{debug, warn};
use std::sync::Arc;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::KnownArtifactKind;

/// Compares a configured RoT bootloader update with information from inventory
/// and determines the current status of the update.  See `MgsUpdateStatus`.
pub fn mgs_update_status_rot_bootloader(
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

    // Similarly, check the contents of the inactive slot to determine if it
    // still matches what we saw when we configured this update.  If not, then
    // this update cannot proceed as currently configured.  It will fail its
    // precondition check.
    mgs_update_status_inactive_versions(
        found_stage0_next_version,
        expected_stage0_next_version,
    )
}

/// Determine if the given baseboard needs an RoT bootloader update and, if so,
/// returns it.
pub fn try_make_update_rot_bootloader(
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
    let Some(rkth) = &stage0_caboose.caboose.sign else {
        warn!(
            log,
            "cannot configure RoT bootloader update for board \
             (missing sign in stage0 caboose from inventory)";
            baseboard_id
        );
        return None;
    };

    let matching_artifacts: Vec<_> = current_artifacts
        .artifacts
        .iter()
        .filter(|a| {
            // A matching RoT bootloader artifact will have:
            //
            // - "name" matching the board name (found above from caboose)
            // - "kind" matching one of the known SP kinds
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
        details: PendingMgsUpdateDetails::RotBootloader(
            PendingMgsUpdateRotBootloaderDetails {
                expected_stage0_version,
                expected_stage0_next_version,
            },
        ),
        artifact_hash: artifact.hash,
        artifact_version: artifact.id.version.clone(),
    })
}
