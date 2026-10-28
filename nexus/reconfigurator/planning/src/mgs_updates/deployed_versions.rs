// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for comparing the MGS-managed software on one board with the
//! software in a specific TUF repo
//!
//! Two parts of the system need to know whether a board is running the
//! software in a particular TUF repo.  The planner needs to know so that it
//! can decide whether to update the board.  Nexus needs to know before it
//! allows a sled to be added to the system because a new sled must already be
//! running the system's target release.
//!
//! Inventory's `Collection` provides each component's active version.  The
//! sibling modules for each component (`sp`, `rot`, `rot_bootloader`, and
//! `host_phase_1`) provide functions that choose the artifact that the
//! component ought to be running.  The planner uses both of these directly.
//! This module uses the same functions and combines the results into one
//! report for callers outside the planner.

use super::host_phase_1::HostOsTargetArtifacts;
use super::rot;
use super::rot_bootloader;
use super::sp;
use gateway_types::rot::RotSlot;
use nexus_types::deployment::planning_report::HostOsTargetArtifactError;
use nexus_types::deployment::planning_report::TargetArtifactError;
use nexus_types::inventory::CabooseVersionError;
use nexus_types::inventory::Collection;
use nexus_types::inventory::InventoryItem;
use nexus_types::inventory::NotInInventory;
use nexus_types::inventory::SpType;
use nexus_types::tuf_repo::TufRepoDescription;
use sled_hardware_types::BaseboardId;
use thiserror::Error;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;

/// Results of comparing the MGS-managed software on one board with the
/// software in a TUF repo
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployedVersions {
    /// comparison of the active SP image (slot 0) with the TUF repo
    pub sp: ActiveImageCheck<
        ArtifactVersion,
        ImageCheckError<CabooseVersionError, TargetArtifactError>,
    >,
    /// comparison of the active RoT image with the TUF repo
    pub rot: ActiveImageCheck<ArtifactVersion, RotCheckError>,
    /// comparison of the active RoT bootloader image (stage0) with the TUF
    /// repo
    pub rot_bootloader: ActiveImageCheck<
        ArtifactVersion,
        ImageCheckError<CabooseVersionError, TargetArtifactError>,
    >,
    /// comparison of the active host phase 1 image with the TUF repo, or
    /// `None` if the board is not a sled
    pub host_phase_1: Option<
        ActiveImageCheck<
            ArtifactHash,
            ImageCheckError<NotInInventory, HostOsTargetArtifactError>,
        >,
    >,
}

impl DeployedVersions {
    /// Compares the active SP, RoT, RoT bootloader, and host phase 1 images
    /// on `baseboard_id` with the artifacts in `repo`
    ///
    /// The RoT comparison also reports an error if the RoT's boot preferences
    /// show an RoT update that has not fully taken effect.
    ///
    /// Host phase 2 (the image on the sled's boot disk) is not checked here.
    /// Inventory only reports it for sleds whose sled-agent is running, and
    /// this function is intended to work for sleds that are not yet part of
    /// the control plane.
    pub fn compute(
        baseboard_id: &BaseboardId,
        inventory: &Collection,
        repo: &TufRepoDescription,
    ) -> Result<DeployedVersions, NotInInventory> {
        let sp_info = inventory.sp_for(baseboard_id)?;

        let sp = ActiveImageCheck::compare(
            inventory
                .active_sp_version_for(baseboard_id)
                .map_err(ImageCheckError::Active),
            sp::target_artifact(baseboard_id, inventory, repo)
                .map(|a| a.version.clone())
                .map_err(ImageCheckError::Target),
        );

        let rot = compare_rot(baseboard_id, inventory, repo);

        let rot_bootloader = ActiveImageCheck::compare(
            inventory
                .active_rot_bootloader_version_for(baseboard_id)
                .map_err(ImageCheckError::Active),
            rot_bootloader::target_artifact(baseboard_id, inventory, repo)
                .map(|a| a.version.clone())
                .map_err(ImageCheckError::Target),
        );

        let host_phase_1 = match sp_info.sp_type {
            SpType::Sled => Some(ActiveImageCheck::compare(
                inventory
                    .active_host_phase_1_hash_for(baseboard_id)
                    .map_err(ImageCheckError::Active),
                HostOsTargetArtifacts::for_board(baseboard_id, repo)
                    .map(|a| a.phase_1.hash)
                    .map_err(ImageCheckError::Target),
            )),
            SpType::Power | SpType::Switch => None,
        };

        Ok(DeployedVersions { sp, rot, rot_bootloader, host_phase_1 })
    }
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
    /// the active image could not be confirmed to match the artifact in the
    /// TUF repo
    Error(E),
}

impl<T: PartialEq, E> ActiveImageCheck<T, E> {
    /// Compares the identity of a component's active image (`found`) with
    /// the identity of the artifact that the component ought to be running
    /// (`expected`)
    ///
    /// If either one could not be determined, the result is `Error`.  If both
    /// could not be determined, the error from `found` is used.
    fn compare(found: Result<T, E>, expected: Result<T, E>) -> Self {
        match (found, expected) {
            (Ok(found), Ok(expected)) => {
                if found == expected {
                    ActiveImageCheck::Matches
                } else {
                    ActiveImageCheck::Differs { found, expected }
                }
            }
            (Err(error), _) | (Ok(_), Err(error)) => {
                ActiveImageCheck::Error(error)
            }
        }
    }
}

/// Reasons that a component's active image could not be compared with the
/// artifact in a TUF repo
///
/// `A` describes why the active image could not be identified from
/// inventory.  `T` describes why the artifact could not be chosen from the
/// TUF repo.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ImageCheckError<A, T> {
    /// the active image could not be identified
    #[error("could not identify the active image")]
    Active(#[source] A),
    /// the artifact that the component ought to be running could not be
    /// chosen
    #[error("could not choose the artifact from the TUF repo")]
    Target(#[source] T),
}

/// Reasons that an RoT's active image could not be confirmed to match the
/// artifact in a TUF repo
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RotCheckError {
    /// the active image could not be compared with the TUF repo
    #[error(transparent)]
    Image(#[from] ImageCheckError<CabooseVersionError, TargetArtifactError>),
    /// the RoT's boot preferences disagree with its active slot
    #[error(
        "RoT boot preferences show an update that has not fully taken \
         effect (active slot: {active_slot:?}, persistent boot preference: \
         {persistent_boot_preference:?}, pending persistent boot \
         preference: {pending_persistent_boot_preference:?}, transient boot \
         preference: {transient_boot_preference:?})"
    )]
    BootPreferencesUnsettled {
        active_slot: RotSlot,
        persistent_boot_preference: RotSlot,
        pending_persistent_boot_preference: Option<RotSlot>,
        transient_boot_preference: Option<RotSlot>,
    },
}

/// Compares the active RoT image on `baseboard_id` with the RoT artifact in
/// `repo`
///
/// An RoT update writes the inactive slot and then changes the boot
/// preference so that the RoT boots from that slot.  If the boot preferences
/// disagree with the active slot, the RoT has been changed in a way that has
/// not yet fully taken effect.  We report that as an error even if the active
/// image matches `repo` because the RoT may soon be running something else.
fn compare_rot(
    baseboard_id: &BaseboardId,
    inventory: &Collection,
    repo: &TufRepoDescription,
) -> ActiveImageCheck<ArtifactVersion, RotCheckError> {
    let Some(rot_state) = inventory.rot_state_for(baseboard_id) else {
        return ActiveImageCheck::Error(RotCheckError::from(
            ImageCheckError::Active(CabooseVersionError::from(
                NotInInventory::new(baseboard_id, InventoryItem::RotState),
            )),
        ));
    };

    let active_slot = rot_state.active_slot;
    let persistent_boot_preference = rot_state.persistent_boot_preference;
    let pending_persistent_boot_preference =
        rot_state.pending_persistent_boot_preference;
    let transient_boot_preference = rot_state.transient_boot_preference;
    if persistent_boot_preference != active_slot
        || pending_persistent_boot_preference.is_some()
        || transient_boot_preference.is_some()
    {
        return ActiveImageCheck::Error(
            RotCheckError::BootPreferencesUnsettled {
                active_slot,
                persistent_boot_preference,
                pending_persistent_boot_preference,
                transient_boot_preference,
            },
        );
    }

    ActiveImageCheck::compare(
        inventory
            .active_rot_version_for(baseboard_id)
            .map_err(|e| RotCheckError::from(ImageCheckError::Active(e))),
        rot::target_artifact(baseboard_id, active_slot, inventory, repo)
            .map(|a| a.version.clone())
            .map_err(|e| RotCheckError::from(ImageCheckError::Target(e))),
    )
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
    fn all_matching_sled_report() -> DeployedVersions {
        DeployedVersions {
            sp: ActiveImageCheck::Matches,
            rot: ActiveImageCheck::Matches,
            rot_bootloader: ActiveImageCheck::Matches,
            host_phase_1: Some(ActiveImageCheck::Matches),
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
            let report = DeployedVersions::compute(&sled, &collection, &repo)
                .expect("sled is in inventory");
            assert_eq!(report, all_matching_sled_report());
        }

        // A switch has no host phase 1 to check.
        let switch = baseboard_id(&collection, SpType::Switch, 0);
        let report = DeployedVersions::compute(&switch, &collection, &repo)
            .expect("switch is in inventory");
        assert_eq!(
            report,
            DeployedVersions {
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
            DeployedVersions::compute(&sled, &collection, &repo)
                .expect("sled is in inventory")
        };

        assert_eq!(
            check(0),
            DeployedVersions {
                sp: ActiveImageCheck::Differs {
                    found: ARTIFACT_VERSION_1,
                    expected: ARTIFACT_VERSION_2,
                },
                ..all_matching_sled_report()
            }
        );
        assert_eq!(
            check(1),
            DeployedVersions {
                rot: ActiveImageCheck::Differs {
                    found: ARTIFACT_VERSION_1,
                    expected: ARTIFACT_VERSION_2,
                },
                ..all_matching_sled_report()
            }
        );
        assert_eq!(
            check(2),
            DeployedVersions {
                rot_bootloader: ActiveImageCheck::Differs {
                    found: ARTIFACT_VERSION_1,
                    expected: ARTIFACT_VERSION_2,
                },
                ..all_matching_sled_report()
            }
        );
        assert_eq!(
            check(3),
            DeployedVersions {
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
        let report = DeployedVersions::compute(&sled, &collection, &repo)
            .expect("sled is in inventory");
        assert_eq!(
            report,
            DeployedVersions {
                rot: ActiveImageCheck::Error(
                    RotCheckError::BootPreferencesUnsettled {
                        active_slot: RotSlot::A,
                        persistent_boot_preference: RotSlot::B,
                        pending_persistent_boot_preference: None,
                        transient_boot_preference: None,
                    }
                ),
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
        let report = DeployedVersions::compute(&sled, &collection, &repo)
            .expect("sled is in inventory");
        assert_eq!(
            report,
            DeployedVersions {
                sp: ActiveImageCheck::Error(ImageCheckError::Active(
                    CabooseVersionError::NotInInventory(NotInInventory::new(
                        &sled,
                        InventoryItem::Caboose(CabooseWhich::SpSlot0),
                    ))
                )),
                rot: ActiveImageCheck::Error(RotCheckError::Image(
                    ImageCheckError::Active(
                        CabooseVersionError::NotInInventory(
                            NotInInventory::new(&sled, InventoryItem::RotState,)
                        )
                    )
                )),
                ..all_matching_sled_report()
            }
        );

        // Remove the SP itself.  Nothing can be checked.
        collection.sps.retain(|bb, _| bb.as_ref() != &sled);
        let error = DeployedVersions::compute(&sled, &collection, &repo)
            .expect_err("sled is not in inventory");
        assert_eq!(error, NotInInventory::new(&sled, InventoryItem::Sp));
    }
}
