// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for comparing the MGS-managed software on a sled with the
//! software from a specific TUF repo
//!
//! This is used during sled adoption (to determine if the sled is running
//! compatible software).  It uses the same helpers that the planner uses to
//! decide whether to schedule an update.

use super::host_phase_1::HostOsTargetArtifacts;
use super::rot;
use super::rot_bootloader;
use super::sp;
use gateway_types::rot::RotSlot;
use nexus_types::deployment::planning_report::HostOsTargetArtifactError;
use nexus_types::deployment::planning_report::TargetArtifactError;
use nexus_types::inventory::Caboose;
use nexus_types::inventory::CabooseVersionError;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::CabooseWithVersion;
use nexus_types::inventory::Collection;
use nexus_types::inventory::NotInInventory;
use nexus_types::inventory::RotState;
use nexus_types::inventory::SpType;
use nexus_types::tuf_repo::TufRepoDescription;
use sled_hardware_types::BaseboardId;
use thiserror::Error;
use tufaceous_artifact::Artifact;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;

/// Results of comparing the MGS-managed software on one sled with the
/// software in a TUF repo
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployedSledVersions {
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
    /// comparison of the active host phase 1 image with the TUF repo
    pub host_phase_1: ActiveImageCheck<
        ArtifactHash,
        ImageCheckError<NotInInventory, HostOsTargetArtifactError>,
    >,
}

impl DeployedSledVersions {
    /// Compares the active SP, RoT, RoT bootloader, and host phase 1 images
    /// on the sled `baseboard_id` with the artifacts in `repo`
    ///
    /// The RoT comparison also reports an error if the RoT's boot preferences
    /// show an RoT update that has not fully taken effect.
    ///
    /// This fails if the board is not a sled.
    // Host phase 2 (the image on the sled's boot disk) is not checked here.
    // Inventory only reports it for sleds whose sled-agent is running, and
    // this function is intended to work for sleds that are not yet part of
    // the control plane.
    pub fn compute(
        baseboard_id: &BaseboardId,
        inventory: &Collection,
        repo: &TufRepoDescription,
    ) -> Result<DeployedSledVersions, DeployedSledVersionsError> {
        let sp_info = inventory.sp_for(baseboard_id)?;
        match sp_info.sp_type {
            SpType::Sled => (),
            sp_type @ (SpType::Power | SpType::Switch) => {
                return Err(DeployedSledVersionsError::NotASled(sp_type));
            }
        }

        let sp = compare_caboose(
            inventory.active_sp_caboose_for(baseboard_id),
            |caboose| sp::target_artifact(caboose, repo),
        );

        let rot = compare_rot(baseboard_id, inventory, repo);

        let rot_bootloader = compare_caboose(
            inventory.active_rot_bootloader_caboose_for(baseboard_id),
            |caboose| rot_bootloader::target_artifact(caboose, repo),
        );

        let host_phase_1 = ActiveImageCheck::compare(
            inventory
                .active_host_phase_1_hash_for(baseboard_id)
                .map_err(ImageCheckError::Active),
            HostOsTargetArtifacts::for_board(baseboard_id, repo)
                .map(|a| a.phase_1.hash)
                .map_err(ImageCheckError::Target),
        );

        Ok(DeployedSledVersions { sp, rot, rot_bootloader, host_phase_1 })
    }
}

/// Reasons that the software on a board could not be compared with a TUF repo
/// at all
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DeployedSledVersionsError {
    /// the board's SP is not in inventory
    #[error(transparent)]
    NotInInventory(#[from] NotInInventory),
    /// the board is not a sled
    #[error("board is not a sled (its SP type is {0:?})")]
    NotASled(SpType),
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
    Differs {
        /// identity of the component's active image
        found: T,
        /// identity of the artifact in the TUF repo
        expected: T,
    },
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
    #[error("could not identify the active image")]
    Active(#[source] A),
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
    /// the RoT's state (which includes its active slot) is not in inventory
    #[error("RoT state is not in inventory")]
    RotStateNotInInventory,
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
fn compare_rot(
    baseboard_id: &BaseboardId,
    inventory: &Collection,
    repo: &TufRepoDescription,
) -> ActiveImageCheck<ArtifactVersion, RotCheckError> {
    let Some(rot_state) = inventory.rot_state_for(baseboard_id) else {
        return ActiveImageCheck::Error(RotCheckError::RotStateNotInInventory);
    };

    // Check whether there's indication of a pending update (e.g., active slot
    // doesn't match the boot preference).
    //
    // We don't do anything explicitly here with the image errors
    // (e.g., `slot_a_error`).  An inactive slot error is not a problem at all
    // and an active slot error will show up when we try to compare it to the
    // expected value.
    //
    // This exhaustive destructuring ensures that adding a field to `RotState`
    // causes a compile error.  If you get one, decide whether the new field
    // affects whether the RoT's boot preferences are settled.
    let RotState {
        time_collected: _,
        source: _,
        active_slot,
        persistent_boot_preference,
        pending_persistent_boot_preference,
        transient_boot_preference,
        slot_a_sha3_256_digest: _,
        slot_b_sha3_256_digest: _,
        stage0_digest: _,
        stage0next_digest: _,
        slot_a_error: _,
        slot_b_error: _,
        stage0_error: _,
        stage0next_error: _,
    } = *rot_state;
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

    compare_caboose(
        inventory.caboose_with_version_for(
            CabooseWhich::from_rot_slot(active_slot),
            baseboard_id,
        ),
        |caboose| rot::target_artifact(active_slot, caboose, repo),
    )
}

/// Compares the version in a component's active caboose with the version of
/// the artifact that `target_artifact` chooses based on that same caboose
fn compare_caboose<'a, E>(
    active: Result<CabooseWithVersion<'_>, CabooseVersionError>,
    target_artifact: impl FnOnce(
        &Caboose,
    ) -> Result<&'a Artifact, TargetArtifactError>,
) -> ActiveImageCheck<ArtifactVersion, E>
where
    E: From<ImageCheckError<CabooseVersionError, TargetArtifactError>>,
{
    let CabooseWithVersion { caboose, version } = match active {
        Ok(active) => active,
        Err(error) => {
            return ActiveImageCheck::Error(E::from(ImageCheckError::Active(
                error,
            )));
        }
    };
    ActiveImageCheck::compare(
        Ok(version),
        target_artifact(caboose)
            .map(|artifact| artifact.version.clone())
            .map_err(|error| E::from(ImageCheckError::Target(error))),
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
    use nexus_types::inventory::InventoryItem;

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
    fn all_matching_sled_report() -> DeployedSledVersions {
        DeployedSledVersions {
            sp: ActiveImageCheck::Matches,
            rot: ActiveImageCheck::Matches,
            rot_bootloader: ActiveImageCheck::Matches,
            host_phase_1: ActiveImageCheck::Matches,
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
            let report =
                DeployedSledVersions::compute(&sled, &collection, &repo)
                    .expect("sled is in inventory");
            assert_eq!(report, all_matching_sled_report());
        }

        // A switch is not a sled, so it cannot be checked.
        let switch = baseboard_id(&collection, SpType::Switch, 0);
        let error = DeployedSledVersions::compute(&switch, &collection, &repo)
            .expect_err("switch is not a sled");
        assert_eq!(error, DeployedSledVersionsError::NotASled(SpType::Switch));
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
            DeployedSledVersions::compute(&sled, &collection, &repo)
                .expect("sled is in inventory")
        };

        assert_eq!(
            check(0),
            DeployedSledVersions {
                sp: ActiveImageCheck::Differs {
                    found: ARTIFACT_VERSION_1,
                    expected: ARTIFACT_VERSION_2,
                },
                ..all_matching_sled_report()
            }
        );
        assert_eq!(
            check(1),
            DeployedSledVersions {
                rot: ActiveImageCheck::Differs {
                    found: ARTIFACT_VERSION_1,
                    expected: ARTIFACT_VERSION_2,
                },
                ..all_matching_sled_report()
            }
        );
        assert_eq!(
            check(2),
            DeployedSledVersions {
                rot_bootloader: ActiveImageCheck::Differs {
                    found: ARTIFACT_VERSION_1,
                    expected: ARTIFACT_VERSION_2,
                },
                ..all_matching_sled_report()
            }
        );
        assert_eq!(
            check(3),
            DeployedSledVersions {
                host_phase_1: ActiveImageCheck::Differs {
                    found: ARTIFACT_HASH_HOST_PHASE_1_V1,
                    expected: ARTIFACT_HASH_GIMLET_HOST_PHASE_1,
                },
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
        let report = DeployedSledVersions::compute(&sled, &collection, &repo)
            .expect("sled is in inventory");
        assert_eq!(
            report,
            DeployedSledVersions {
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
        let report = DeployedSledVersions::compute(&sled, &collection, &repo)
            .expect("sled is in inventory");
        assert_eq!(
            report,
            DeployedSledVersions {
                sp: ActiveImageCheck::Error(ImageCheckError::Active(
                    CabooseVersionError::NotInInventory(CabooseWhich::SpSlot0)
                )),
                rot: ActiveImageCheck::Error(
                    RotCheckError::RotStateNotInInventory
                ),
                ..all_matching_sled_report()
            }
        );

        // Remove the SP itself.  Nothing can be checked.
        collection.sps.retain(|bb, _| bb.as_ref() != &sled);
        let error = DeployedSledVersions::compute(&sled, &collection, &repo)
            .expect_err("sled is not in inventory");
        assert_eq!(
            error,
            DeployedSledVersionsError::NotInInventory(NotInInventory(
                InventoryItem::Sp
            ))
        );
    }
}
