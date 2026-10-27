// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Comparing the MGS-managed software on one board with a TUF repo
// <review>
// _the software in_ a _specific_ TUF repo
// </review>
//!
//! The planner decides whether a board needs an update by finding the
//! artifact in the target release that applies to the board and comparing it
//! with what the board is running.  This module exposes that same comparison
//! for callers outside the planner (e.g., to check that a sled being added to
//! the system is already running the target release).  Both paths use the
//! same functions to choose artifacts, so they cannot disagree about which
//! artifact a board ought to be running.
// <review>
// This is true but it's a confused way to frame it: the function of determining whether what's there matches what it should be is used in two co-equal places: the planner and the "add sled" validation.
//
// This also makes me wonder if this is the right module for this functionality.
// </review>

use super::host_phase_1;
use super::rot;
use super::rot_bootloader;
use super::sp;
use gateway_types::rot::RotSlot;
use nexus_types::deployment::planning_report::FailedHostOsUpdateReason;
use nexus_types::deployment::planning_report::FailedRotBootloaderUpdateReason;
use nexus_types::deployment::planning_report::FailedRotUpdateReason;
use nexus_types::deployment::planning_report::FailedSpUpdateReason;
use nexus_types::inventory::Collection;
use nexus_types::inventory::RotState;
use nexus_types::inventory::SpType;
use nexus_types::tuf_repo::TufRepoDescription;
use sled_hardware_types::BaseboardId;
use thiserror::Error;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;

/// Compares the active SP, RoT, RoT bootloader, and host phase 1 images on
/// `baseboard_id` with the artifacts in `repo`
///
/// This also reports whether the RoT's boot preferences show an RoT update
/// that has not fully taken effect.
///
/// Host phase 2 (the image on the sled's boot disk) is not checked here.
/// Inventory only reports it for sleds whose sled-agent is running, and this
/// function is intended to work for sleds that are not yet part of the
/// control plane.
pub fn check_active_versions(
// <review>
// This is a vague name for a `pub` function.  What about `compare_deployed_versions`?
// </review>
    baseboard_id: &BaseboardId,
    inventory: &Collection,
    repo: &TufRepoDescription,
) -> Result<ActiveVersionsReport, BoardNotInInventory> {
    let Some(sp_info) = inventory.sps.get(baseboard_id) else {
        return Err(BoardNotInInventory(baseboard_id.clone()));
    };

    let sp = ActiveImageCheck::from_result(
        sp::active_and_target(baseboard_id, inventory, repo)
            .map(|t| (t.active_version, t.artifact.version.clone())),
    );

    let rot = ActiveImageCheck::from_result(
        rot::active_and_target(baseboard_id, inventory, repo)
            .map(|t| (t.active_version, t.artifact.version.clone())),
    );

    // The RoT state can be present in inventory even when we could not
    // determine the RoT's version, so look it up separately.
    let rot_boot_preferences = match inventory.rot_state_for(baseboard_id) {
        Some(rot_state) => RotBootPreferenceCheck::new(rot_state),
        None => RotBootPreferenceCheck::Indeterminate,
    };

    let rot_bootloader = ActiveImageCheck::from_result(
        rot_bootloader::active_and_target(baseboard_id, inventory, repo)
            .map(|t| (t.active_version, t.artifact.version.clone())),
    );

    let host_phase_1 = match sp_info.sp_type {
        SpType::Sled => Some(ActiveImageCheck::from_result(
            host_phase_1_active_and_target(baseboard_id, inventory, repo),
        )),
        SpType::Power | SpType::Switch => None,
    };

    Ok(ActiveVersionsReport {
        sp,
        rot,
        rot_bootloader,
        host_phase_1,
        rot_boot_preferences,
    })
}

/// Results of comparing the MGS-managed software on one board with a TUF repo
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveVersionsReport {
    /// comparison of the active SP image (slot 0) with the TUF repo
    pub sp: ActiveImageCheck<ArtifactVersion, FailedSpUpdateReason>,
    /// comparison of the active RoT image with the TUF repo
    pub rot: ActiveImageCheck<ArtifactVersion, FailedRotUpdateReason>,
    /// comparison of the active RoT bootloader image (stage0) with the TUF
    /// repo
    pub rot_bootloader:
        ActiveImageCheck<ArtifactVersion, FailedRotBootloaderUpdateReason>,
    /// comparison of the active host phase 1 image with the TUF repo, or
    /// `None` if the board is not a sled
    pub host_phase_1:
        Option<ActiveImageCheck<ArtifactHash, FailedHostOsUpdateReason>>,
    /// whether the RoT's boot preferences agree with its active slot
    pub rot_boot_preferences: RotBootPreferenceCheck,
}

/// Result of comparing one component's active image with a TUF repo
///
/// `T` identifies an image.  For components with a caboose, it is the
/// caboose version.  For host phase 1, it is the hash of the flash contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActiveImageCheck<T, E> {
    /// the active image matches the artifact in the TUF repo
    Matches,
    /// the active image differs from the artifact in the TUF repo
    Differs { found: T, expected: T },
    /// the active image could not be compared with the TUF repo
    Indeterminate(E),
}

impl<T: PartialEq, E> ActiveImageCheck<T, E> {
    fn from_result(result: Result<(T, T), E>) -> Self {
    // <review>
    // This could use a doc comment.  It's not obvious what it means to turn a `Result` into this type.
    // </review>
        match result {
            Ok((found, expected)) => {
                if found == expected {
                    ActiveImageCheck::Matches
                } else {
                    ActiveImageCheck::Differs { found, expected }
                }
            }
            Err(error) => ActiveImageCheck::Indeterminate(error),
        }
    }
}

/// Result of checking whether an RoT's boot preferences agree with its active
/// slot
///
/// An RoT update writes the inactive slot and then changes the boot
/// preference so that the RoT boots from that slot.  If the preferences
/// disagree with the active slot, the RoT has been changed in a way that has
/// not yet fully taken effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RotBootPreferenceCheck {
// <review>
// I wonder about folding this into an Error case of `ActiveImageCheck`.  i.e., we're trying to figure out if the image matches.  If it matches but there's an update pending, that's an error case because we don't expect it.  (That seems true in the "add sled" case.  It's not necessarily true in the regular planning case.)
// </review>
    /// the persistent boot preference matches the active slot, and neither
    /// the pending persistent boot preference nor the transient boot
    /// preference is set
    Settled,
    /// the boot preferences disagree with the active slot
    Unsettled {
        active_slot: RotSlot,
        persistent_boot_preference: RotSlot,
        pending_persistent_boot_preference: Option<RotSlot>,
        transient_boot_preference: Option<RotSlot>,
    },
    /// the RoT state is not in inventory
    Indeterminate,
}

impl RotBootPreferenceCheck {
    fn new(rot_state: &RotState) -> Self {
        let active_slot = rot_state.active_slot;
        let persistent_boot_preference = rot_state.persistent_boot_preference;
        let pending_persistent_boot_preference =
            rot_state.pending_persistent_boot_preference;
        let transient_boot_preference = rot_state.transient_boot_preference;
        if persistent_boot_preference == active_slot
            && pending_persistent_boot_preference.is_none()
            && transient_boot_preference.is_none()
        {
            RotBootPreferenceCheck::Settled
        } else {
            RotBootPreferenceCheck::Unsettled {
                active_slot,
                persistent_boot_preference,
                pending_persistent_boot_preference,
                transient_boot_preference,
            }
        }
    }
}

/// The board's SP was not found in inventory, so none of its software could
/// be checked
#[derive(Debug, Clone, Error)]
#[error("SP for board {0} is not in inventory")]
pub struct BoardNotInInventory(pub BaseboardId);

/// Returns the hash of the active host phase 1 image on a sled, paired with
/// the hash of the phase 1 artifact that `repo` specifies for that sled
fn host_phase_1_active_and_target(
// <review>
// Why is this function not in the `host_phase_1` module like the others?
// </review>
    baseboard_id: &BaseboardId,
    inventory: &Collection,
    repo: &TufRepoDescription,
) -> Result<(ArtifactHash, ArtifactHash), FailedHostOsUpdateReason> {
    let Some(active_slot) =
        inventory.host_phase_1_active_slot_for(baseboard_id).map(|s| s.slot)
    else {
        return Err(
            FailedHostOsUpdateReason::ActiveHostPhase1SlotNotInInventory,
        );
    };
    let Some(active_hash) = inventory
        .host_phase_1_flash_hash_for(active_slot, baseboard_id)
        .map(|h| h.hash)
    else {
        return Err(
            FailedHostOsUpdateReason::ActiveHostPhase1HashNotInInventory(
                active_slot,
            ),
        );
    };
    let artifacts = host_phase_1::target_artifacts(baseboard_id, repo)?;
    Ok((active_hash, artifacts.phase_1.hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mgs_updates::test_helpers::ARTIFACT_HASH_GIMLET_HOST_PHASE_1;
    use crate::mgs_updates::test_helpers::ARTIFACT_HASH_HOST_PHASE_1_V1;
    use crate::mgs_updates::test_helpers::ARTIFACT_HASH_HOST_PHASE_2;
    use crate::mgs_updates::test_helpers::ARTIFACT_VERSION_1;
    use crate::mgs_updates::test_helpers::ARTIFACT_VERSION_2;
    use crate::mgs_updates::test_helpers::TestBoards;
    use nexus_types::inventory::CabooseWhich;

    // Returns the baseboard id of the board with the given SP type and slot.
    fn baseboard_id(
        collection: &Collection,
        sp_type: SpType,
        sp_slot: u16,
    ) -> BaseboardId {
        let (baseboard_id, _) = collection
            .sps
            .iter()
            .find(|(_, sp)| sp.sp_type == sp_type && sp.sp_slot == sp_slot)
            .expect("test collection contains requested board");
        BaseboardId::clone(baseboard_id)
    }

    // Returns a report in which every check succeeded.
    fn all_matching_sled_report() -> ActiveVersionsReport {
        ActiveVersionsReport {
            sp: ActiveImageCheck::Matches,
            rot: ActiveImageCheck::Matches,
            rot_bootloader: ActiveImageCheck::Matches,
            host_phase_1: Some(ActiveImageCheck::Matches),
            rot_boot_preferences: RotBootPreferenceCheck::Settled,
        }
    }

    #[test]
    fn test_all_components_match() {
        let boards = TestBoards::new("test_all_components_match");
        let repo = boards.tuf_repo();
        let collection = boards.collection_builder().build();

        // Check both a Gimlet (slot 0) and a Cosmo (slot 1).
        for sp_slot in [0, 1] {
            let sled = baseboard_id(&collection, SpType::Sled, sp_slot);
            let report = check_active_versions(&sled, &collection, &repo)
                .expect("sled is in inventory");
            assert_eq!(report, all_matching_sled_report());
        }

        // A switch has no host phase 1 to check.
        let switch = baseboard_id(&collection, SpType::Switch, 0);
        let report = check_active_versions(&switch, &collection, &repo)
            .expect("switch is in inventory");
        assert_eq!(
            report,
            ActiveVersionsReport {
                host_phase_1: None,
                ..all_matching_sled_report()
            }
        );
    }

    #[test]
    fn test_component_versions_differ() {
        let boards = TestBoards::new("test_component_versions_differ");
        let repo = boards.tuf_repo();

        // Put a different component on an old version in each of sleds 0
        // through 3.
        let collection = boards
            .collection_builder()
            .sp_active_version_exception(SpType::Sled, 0, ARTIFACT_VERSION_1)
            .rot_active_version_exception(SpType::Sled, 1, ARTIFACT_VERSION_1)
            .stage0_version_exception(SpType::Sled, 2, ARTIFACT_VERSION_1)
            .host_active_exception(
                3,
                ARTIFACT_HASH_HOST_PHASE_1_V1,
                ARTIFACT_HASH_HOST_PHASE_2,
            )
            .build();
        let check = |sp_slot| {
            let sled = baseboard_id(&collection, SpType::Sled, sp_slot);
            check_active_versions(&sled, &collection, &repo)
                .expect("sled is in inventory")
        };

        assert_eq!(
            check(0),
            ActiveVersionsReport {
                sp: ActiveImageCheck::Differs {
                    found: ARTIFACT_VERSION_1,
                    expected: ARTIFACT_VERSION_2,
                },
                ..all_matching_sled_report()
            }
        );
        assert_eq!(
            check(1),
            ActiveVersionsReport {
                rot: ActiveImageCheck::Differs {
                    found: ARTIFACT_VERSION_1,
                    expected: ARTIFACT_VERSION_2,
                },
                ..all_matching_sled_report()
            }
        );
        assert_eq!(
            check(2),
            ActiveVersionsReport {
                rot_bootloader: ActiveImageCheck::Differs {
                    found: ARTIFACT_VERSION_1,
                    expected: ARTIFACT_VERSION_2,
                },
                ..all_matching_sled_report()
            }
        );
        assert_eq!(
            check(3),
            ActiveVersionsReport {
                host_phase_1: Some(ActiveImageCheck::Differs {
                    found: ARTIFACT_HASH_HOST_PHASE_1_V1,
                    expected: ARTIFACT_HASH_GIMLET_HOST_PHASE_1,
                }),
                ..all_matching_sled_report()
            }
        );
    }

    #[test]
    fn test_rot_boot_preference_unsettled() {
        let boards = TestBoards::new("test_rot_boot_preference_unsettled");
        let repo = boards.tuf_repo();
        let collection = boards
            .collection_builder()
            .rot_persistent_boot_preference_exception(
                SpType::Sled,
                0,
                RotSlot::B,
            )
            .build();
        let sled = baseboard_id(&collection, SpType::Sled, 0);
        let report = check_active_versions(&sled, &collection, &repo)
            .expect("sled is in inventory");
        assert_eq!(
            report,
            ActiveVersionsReport {
                rot_boot_preferences: RotBootPreferenceCheck::Unsettled {
                    active_slot: RotSlot::A,
                    persistent_boot_preference: RotSlot::B,
                    pending_persistent_boot_preference: None,
                    transient_boot_preference: None,
                },
                ..all_matching_sled_report()
            }
        );
    }

    #[test]
    fn test_missing_inventory() {
        let boards = TestBoards::new("test_missing_inventory");
        let repo = boards.tuf_repo();
        let mut collection = boards.collection_builder().build();
        let sled = baseboard_id(&collection, SpType::Sled, 0);

        // Remove the active SP caboose and the RoT state.  The SP and RoT
        // checks should report that they could not compare anything.
        let sp_cabooses = collection
            .cabooses_found
            .get_mut(&CabooseWhich::SpSlot0)
            .expect("test collection contains SP slot 0 cabooses");
        sp_cabooses.retain(|bb, _| bb.as_ref() != &sled);
        collection.rots.retain(|bb, _| bb.as_ref() != &sled);
        let report = check_active_versions(&sled, &collection, &repo)
            .expect("sled is in inventory");
        assert_eq!(
            report,
            ActiveVersionsReport {
                sp: ActiveImageCheck::Indeterminate(
                    FailedSpUpdateReason::CabooseNotInInventory(
                        CabooseWhich::SpSlot0
                    )
                ),
                rot: ActiveImageCheck::Indeterminate(
                    FailedRotUpdateReason::RotStateNotInInventory
                ),
                rot_boot_preferences: RotBootPreferenceCheck::Indeterminate,
                ..all_matching_sled_report()
            }
        );

        // Remove the SP itself.  Nothing can be checked.
        collection.sps.retain(|bb, _| bb.as_ref() != &sled);
        let error = check_active_versions(&sled, &collection, &repo)
            .expect_err("sled is not in inventory");
        assert_eq!(error.0, sled);
    }
}
