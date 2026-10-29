// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Checks run before adding sleds to the system
//!
//! Before adding a sled, we check two things:
//!
//! 1. The system itself has finished updating to its target release.  We do
//!    not support self-service adding of sleds while an update is in
//!    progress, while a sled is waiting for a mupdate to be cleared, or while
//!    updates are suspended.
//! 2. The new sled's SP, RoT, RoT bootloader, and host phase 1 images match
//!    the target release, and the RoT's boot preferences show no RoT update
//!    that has not fully taken effect.  These come from the latest inventory
//!    collection.
//!
//! If no target release has ever been set, we skip both checks.
//!
//! We cannot check the new sled's host phase 2 image (the OS on its boot
//! disk) because inventory only reports that for sleds whose sled-agent is
//! already running.
//!
//! These checks are inherently racy.  An operator could set a new target
//! release, or the sled's software could change, between when we check and
//! when the sled is actually added.

use super::deployment::BlueprintTargetReleaseStatus;
use nexus_db_model::TargetReleaseSource;
use nexus_db_queries::context::OpContext;
use nexus_reconfigurator_planning::mgs_updates::ActiveImageCheck;
use nexus_reconfigurator_planning::mgs_updates::DeployedVersions;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::MgsUpdateComponent;
use nexus_types::inventory::Collection;
use nexus_types::inventory::NotInInventory;
use nexus_types::tuf_repo::TufRepoDescription;
use omicron_common::api::external::Error;
use omicron_generation_kinds::TargetReleaseGeneration;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::fmt::Display;

impl super::Nexus {
    /// Checks that the system and the software on the given sleds are ready
    /// for those sleds to be added
    ///
    /// See the module documentation for details.
    pub(super) async fn sled_check_versions(
        &self,
        opctx: &OpContext,
        sleds: impl IntoIterator<Item = &BaseboardId>,
    ) -> Result<(), Error> {
        let target_release =
            self.datastore().target_release_get_current(opctx).await?;
        let release_source = target_release.release_source().map_err(|e| {
            Error::internal_error(&format!(
                "loading current target release: {e:#}"
            ))
        })?;
        let tuf_repo_id = match release_source {
            TargetReleaseSource::Unspecified => {
                // This case exists primarily for systems that have not yet
                // adopted self-service update.  Those systems have never set a
                // target release, so there is nothing to check against.
                info!(
                    opctx.log,
                    "skipping software checks for sled add: \
                     no target release has been set"
                );
                return Ok(());
            }
            TargetReleaseSource::SystemVersion(tuf_repo_id) => tuf_repo_id,
        };

        let repo: TufRepoDescription = self
            .datastore()
            .tuf_repo_get_by_id(opctx, tuf_repo_id)
            .await?
            .into();
        let (_, blueprint) =
            self.datastore().blueprint_target_get_current_full(opctx).await?;
        let reject = |error: SledSoftwareError| {
            Error::invalid_request(format!(
                "sled cannot be added: {}",
                InlineErrorChain::new(&error)
            ))
        };

        check_system_settled(&blueprint, target_release.generation(), &repo)
            .map_err(reject)?;

        let Some(inventory) =
            self.datastore().inventory_get_latest_collection(opctx).await?
        else {
            return Err(reject(SledSoftwareError::NoInventory));
        };

        check_sleds_match(&repo, &inventory, sleds).map_err(reject)
        // <review covers="17 lines above">
        // Would this be cleaner as a sequence of `x.and_then(...)` with one `map_err` at the end?
        // </review>
    }
}

/// Checks that the system has settled on the target release described by
/// `repo`
///
/// The system has not settled if updates are suspended, a sled is waiting for
/// a mupdate to be cleared, or an update to the target release is still in
/// progress.
fn check_system_settled(
    blueprint: &Blueprint,
    target_release_generation: TargetReleaseGeneration,
    repo: &TufRepoDescription,
) -> Result<(), SledSoftwareError> {
    if blueprint.updates_suspended(target_release_generation) {
        return Err(SledSoftwareError::UpdateSuspended {
            current: target_release_generation,
            minimum: blueprint.target_release_minimum_generation,
        });
    }

    match BlueprintTargetReleaseStatus::new(blueprint, &repo.system_version) {
        BlueprintTargetReleaseStatus::AllComponentsMatchTargetRelease => (),
        BlueprintTargetReleaseStatus::WaitingForMupdateToBeCleared {
            how: _,
            sled_id,
        } => {
            return Err(SledSoftwareError::WaitingForMupdateToBeCleared {
                sled_id,
            });
        }
        BlueprintTargetReleaseStatus::FoundDifferentVersion {
            sled_id,
            version_found,
        } => {
            return Err(SledSoftwareError::UpdateInProgress {
                target_release_version: repo.system_version.clone(),
                sled_id,
                version_found,
            });
        }
    }

    Ok(())
}

/// Checks that each sled in `sleds` is running the software in `repo`,
/// according to `inventory`
fn check_sleds_match<'a>(
    repo: &TufRepoDescription,
    inventory: &Collection,
    sleds: impl IntoIterator<Item = &'a BaseboardId>,
) -> Result<(), SledSoftwareError> {
    for sled in sleds {
        let report = DeployedVersions::compute(sled, inventory, repo)?;
        let problems = sled_software_problems(sled, report)?;
        if !problems.is_empty() {
            return Err(SledSoftwareError::Mismatch {
                sled: sled.clone(),
                target_release_version: repo.system_version.clone(),
                problems,
            });
        }
    }

    Ok(())
}

/// Reasons that a sled's software is not ready for the sled to be added
#[derive(Debug, thiserror::Error)]
enum SledSoftwareError {
    #[error(
        "updates are suspended because the current target release has \
         generation {current}, but the blueprint requires at least \
         generation {minimum}"
    )]
    UpdateSuspended {
        current: TargetReleaseGeneration,
        minimum: TargetReleaseGeneration,
    },
    #[error(
        "a support-driven recovery (mupdate) has occurred on sled {sled_id} \
         and must be cleared first"
    )]
    WaitingForMupdateToBeCleared { sled_id: SledUuid },
    #[error(
        "an update to target release {target_release_version} is in \
         progress (sled {sled_id} has components on {version_found})"
    )]
    UpdateInProgress {
        target_release_version: semver::Version,
        sled_id: SledUuid,
        version_found: BlueprintArtifactVersion,
    },
    #[error("no inventory collection is available")]
    NoInventory,
    #[error("the sled's software could not be checked")]
    NotInInventory(#[from] NotInInventory),
    #[error("board {0} is not a sled")]
    NotASled(BaseboardId),
    #[error(
        "the software on sled {sled} does not match target release \
         {target_release_version}: {}",
        join_problems(problems)
    )]
    Mismatch {
        sled: BaseboardId,
        target_release_version: semver::Version,
        problems: Vec<SledSoftwareProblem>,
    },
}

fn join_problems(problems: &[SledSoftwareProblem]) -> String {
    problems.iter().map(|p| p.to_string()).collect::<Vec<_>>().join("; ")
}

/// One way in which a sled's software is not ready for the sled to be added
#[derive(Debug, Clone, PartialEq, Eq)]
enum SledSoftwareProblem {
    /// the component's active image differs from the target release
    WrongImage {
        component: MgsUpdateComponent,
        found: String,
        expected: String,
    },
    /// the component's active image could not be confirmed to match the
    /// target release
    Unverified { component: MgsUpdateComponent, reason: String },
}

impl Display for SledSoftwareProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SledSoftwareProblem::WrongImage { component, found, expected } => {
                write!(
                    f,
                    "{component} is running {found} (expected {expected})"
                )
            }
            SledSoftwareProblem::Unverified { component, reason } => {
                write!(f, "{component} could not be verified: {reason}")
            }
        }
    }
}

/// Converts a `DeployedVersions` into a list of
/// problems that prevent the sled from being added
///
/// An empty list means the sled's software is ready.
fn sled_software_problems(
    sled: &BaseboardId,
    report: DeployedVersions,
) -> Result<Vec<SledSoftwareProblem>, SledSoftwareError> {
    let DeployedVersions { sp, rot, rot_bootloader, host_phase_1 } = report;

    // A board without host phase 1 is not a sled.
    let Some(host_phase_1) = host_phase_1 else {
        return Err(SledSoftwareError::NotASled(sled.clone()));
    };

    Ok([
        image_problem(MgsUpdateComponent::Sp, sp),
        image_problem(MgsUpdateComponent::Rot, rot),
        image_problem(MgsUpdateComponent::RotBootloader, rot_bootloader),
        image_problem(MgsUpdateComponent::HostOs, host_phase_1),
    ]
    .into_iter()
    .flatten()
    .collect())
}

/// Converts one component's result from `DeployedVersions` into a
/// problem, if there is one
fn image_problem<T: Display, E: std::error::Error>(
    component: MgsUpdateComponent,
    check: ActiveImageCheck<T, E>,
) -> Option<SledSoftwareProblem> {
    match check {
        ActiveImageCheck::Matches => None,
        ActiveImageCheck::Differs { found, expected } => {
            Some(SledSoftwareProblem::WrongImage {
                component,
                found: found.to_string(),
                expected: expected.to_string(),
            })
        }
        ActiveImageCheck::Error(error) => {
            Some(SledSoftwareProblem::Unverified {
                component,
                reason: InlineErrorChain::new(&error).to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gateway_types::rot::RotSlot;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::mgs_updates::ImageCheckError;
    use nexus_reconfigurator_planning::mgs_updates::RotCheckError;
    use nexus_types::inventory::CabooseVersionError;
    use nexus_types::inventory::CabooseWhich;
    use nexus_types::inventory::InventoryItem;
    use std::collections::BTreeMap;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::ArtifactSet;
    use tufaceous_artifact::ArtifactVersion;

    fn test_sled() -> BaseboardId {
        BaseboardId {
            part_number: "913-0000019".to_string(),
            serial_number: "test-sled".to_string(),
        }
    }

    fn all_matching_report() -> DeployedVersions {
        DeployedVersions {
            sp: ActiveImageCheck::Matches,
            rot: ActiveImageCheck::Matches,
            rot_bootloader: ActiveImageCheck::Matches,
            host_phase_1: Some(ActiveImageCheck::Matches),
        }
    }

    #[test]
    fn test_system_not_settled() {
        let mut blueprint = BlueprintBuilder::build_empty("test");
        let repo = TufRepoDescription {
            artifacts: ArtifactSet::default(),
            metadata: BTreeMap::new(),
            system_version: "17.0.0".parse().unwrap(),
            hash: ArtifactHash([0; 32]),
            file_name: String::new(),
        };
        let generation = blueprint.target_release_minimum_generation;

        // With no sleds in the blueprint, the system is settled on any
        // target release.
        check_system_settled(&blueprint, generation, &repo)
            .expect("system is settled");

        // Simulate the planner having noticed a mupdate (which bumps the
        // blueprint's minimum target release generation) and the operator not
        // yet having set a new target release.
        blueprint.target_release_minimum_generation = generation.next();
        let error = check_system_settled(&blueprint, generation, &repo)
            .expect_err("updates suspended");
        assert!(
            matches!(
                error,
                SledSoftwareError::UpdateSuspended { current, minimum }
                    if current == generation && minimum == generation.next()
            ),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn test_no_problems_when_all_match() {
        let problems =
            sled_software_problems(&test_sled(), all_matching_report())
                .expect("board is a sled");
        assert_eq!(problems, Vec::new());
    }

    #[test]
    fn test_problems_reported() {
        let report = DeployedVersions {
            sp: ActiveImageCheck::Differs {
                found: ArtifactVersion::new_const("1.0.0"),
                expected: ArtifactVersion::new_const("2.0.0"),
            },
            rot: ActiveImageCheck::Error(
                RotCheckError::BootPreferencesUnsettled {
                    active_slot: RotSlot::A,
                    persistent_boot_preference: RotSlot::B,
                    pending_persistent_boot_preference: None,
                    transient_boot_preference: None,
                },
            ),
            rot_bootloader: ActiveImageCheck::Matches,
            host_phase_1: Some(ActiveImageCheck::Differs {
                found: ArtifactHash([1; 32]),
                expected: ArtifactHash([2; 32]),
            }),
        };
        let problems = sled_software_problems(&test_sled(), report)
            .expect("board is a sled");
        assert_eq!(problems.len(), 3);
        assert_eq!(
            problems[0],
            SledSoftwareProblem::WrongImage {
                component: MgsUpdateComponent::Sp,
                found: "1.0.0".to_string(),
                expected: "2.0.0".to_string(),
            }
        );
        assert!(matches!(
            problems[1],
            SledSoftwareProblem::Unverified {
                component: MgsUpdateComponent::Rot,
                ..
            }
        ));
        assert_eq!(
            problems[2],
            SledSoftwareProblem::WrongImage {
                component: MgsUpdateComponent::HostOs,
                found: ArtifactHash([1; 32]).to_string(),
                expected: ArtifactHash([2; 32]).to_string(),
            }
        );

        // Check that the error message names the sled, the target release,
        // and each problem.
        let error = SledSoftwareError::Mismatch {
            sled: test_sled(),
            target_release_version: "17.0.0".parse().unwrap(),
            problems,
        };
        let message = InlineErrorChain::new(&error).to_string();
        assert!(message.contains("test-sled"));
        assert!(message.contains("17.0.0"));
        assert!(message.contains("SP is running 1.0.0 (expected 2.0.0)"));
        assert!(message.contains(
            "RoT could not be verified: RoT boot preferences show an update"
        ));
        assert!(message.contains("Host OS is running"));
    }

    #[test]
    fn test_unverified_is_a_problem() {
        let report = DeployedVersions {
            sp: ActiveImageCheck::Error(ImageCheckError::Active(
                CabooseVersionError::NotInInventory(NotInInventory::new(
                    &test_sled(),
                    InventoryItem::Caboose(CabooseWhich::SpSlot0),
                )),
            )),
            ..all_matching_report()
        };
        let problems = sled_software_problems(&test_sled(), report)
            .expect("board is a sled");
        assert_eq!(
            problems,
            vec![SledSoftwareProblem::Unverified {
                component: MgsUpdateComponent::Sp,
                reason: "could not identify the active image: caboose \
                         SpSlot0 for baseboard 913-0000019:test-sled is not \
                         in inventory"
                    .to_string(),
            }]
        );
    }

    #[test]
    fn test_not_a_sled() {
        let report =
            DeployedVersions { host_phase_1: None, ..all_matching_report() };
        let error = sled_software_problems(&test_sled(), report)
            .expect_err("board is not a sled");
        assert!(matches!(error, SledSoftwareError::NotASled(_)));
    }
}
