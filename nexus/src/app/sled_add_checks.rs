// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Checks run before adding sleds to the system
//!
//! When the system has a target release, a sled being added ought to be
//! running the software from that release already.  Before adding a sled, we
//! check two things:
//!
//! 1. The system itself has finished updating to its target release.  We do
//!    not add sleds while an update is in progress, while a sled is waiting
//!    for a mupdate to be cleared, or while updates are suspended.
//! 2. The new sled's SP, RoT, RoT bootloader, and host phase 1 images match
//!    the target release, and the RoT's boot preferences show no RoT update
//!    that has not fully taken effect.  These come from the latest inventory
//!    collection.  The comparison is done by
//!    `nexus_reconfigurator_planning::mgs_updates::compare_deployed_versions()`,
//!    which uses the same logic that the planner uses to decide whether a
//!    board needs an update.
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
use nexus_reconfigurator_planning::mgs_updates::BoardNotInInventory;
use nexus_reconfigurator_planning::mgs_updates::DeployedVersionsReport;
use nexus_reconfigurator_planning::mgs_updates::compare_deployed_versions;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::inventory::Collection;
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
    pub(super) async fn sled_add_check_software(
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
        let inventory =
            self.datastore().inventory_get_latest_collection(opctx).await?;

        check_sled_software(
            &blueprint,
            target_release.generation(),
            &repo,
            inventory.as_ref(),
            sleds,
        )
        .map_err(|error| {
            Error::invalid_request(format!(
                "sled cannot be added: {}",
                InlineErrorChain::new(&error)
            ))
        })
    }
}

/// Checks that the system has settled on the target release described by
/// `repo` and that each sled in `sleds` is running that release's software
fn check_sled_software<'a>(
    blueprint: &Blueprint,
    target_release_generation: TargetReleaseGeneration,
    repo: &TufRepoDescription,
    inventory: Option<&Collection>,
    sleds: impl IntoIterator<Item = &'a BaseboardId>,
) -> Result<(), SledSoftwareError> {
    if blueprint.target_release_updates_suspended(target_release_generation) {
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

    let Some(inventory) = inventory else {
        return Err(SledSoftwareError::NoInventory);
    };

    for sled in sleds {
        let report = compare_deployed_versions(sled, inventory, repo)?;
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
    NotInInventory(#[from] BoardNotInInventory),
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

/// A component on a sled whose software we check before adding the sled
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SledComponent {
    Sp,
    Rot,
    RotBootloader,
    HostPhase1,
}

impl Display for SledComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SledComponent::Sp => "SP",
            SledComponent::Rot => "RoT",
            SledComponent::RotBootloader => "RoT bootloader",
            SledComponent::HostPhase1 => "host OS phase 1",
        })
    }
}

/// One way in which a sled's software is not ready for the sled to be added
#[derive(Debug, Clone, PartialEq, Eq)]
enum SledSoftwareProblem {
    /// the component's active image differs from the target release
    WrongImage { component: SledComponent, found: String, expected: String },
    /// the component's active image could not be confirmed to match the
    /// target release
    Unverified { component: SledComponent, reason: String },
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

/// Converts a report from `compare_deployed_versions()` into a list of
/// problems that prevent the sled from being added
///
/// An empty list means the sled's software is ready.
fn sled_software_problems(
    sled: &BaseboardId,
    report: DeployedVersionsReport,
) -> Result<Vec<SledSoftwareProblem>, SledSoftwareError> {
    let DeployedVersionsReport { sp, rot, rot_bootloader, host_phase_1 } =
        report;

    // A board without host phase 1 is not a sled.
    let Some(host_phase_1) = host_phase_1 else {
        return Err(SledSoftwareError::NotASled(sled.clone()));
    };

    Ok([
        image_problem(SledComponent::Sp, sp),
        image_problem(SledComponent::Rot, rot),
        image_problem(SledComponent::RotBootloader, rot_bootloader),
        image_problem(SledComponent::HostPhase1, host_phase_1),
    ]
    .into_iter()
    .flatten()
    .collect())
}

/// Converts one component's result from `compare_deployed_versions()` into a
/// problem, if there is one
fn image_problem<T: Display, E: std::error::Error>(
    component: SledComponent,
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
    use nexus_reconfigurator_planning::mgs_updates::RotCheckError;
    use nexus_types::deployment::planning_report::FailedSpUpdateReason;
    use nexus_types::inventory::CabooseWhich;
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

    fn all_matching_report() -> DeployedVersionsReport {
        DeployedVersionsReport {
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
        // target release.  We then fail only because there is no inventory.
        let error = check_sled_software(
            &blueprint,
            generation,
            &repo,
            None,
            [&test_sled()],
        )
        .expect_err("no inventory");
        assert!(
            matches!(error, SledSoftwareError::NoInventory),
            "unexpected error: {error:?}"
        );

        // Simulate the planner having noticed a mupdate (which bumps the
        // blueprint's minimum target release generation) and the operator not
        // yet having set a new target release.
        blueprint.target_release_minimum_generation = generation.next();
        let error = check_sled_software(
            &blueprint,
            generation,
            &repo,
            None,
            [&test_sled()],
        )
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
        let report = DeployedVersionsReport {
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
                component: SledComponent::Sp,
                found: "1.0.0".to_string(),
                expected: "2.0.0".to_string(),
            }
        );
        assert!(matches!(
            problems[1],
            SledSoftwareProblem::Unverified {
                component: SledComponent::Rot,
                ..
            }
        ));
        assert_eq!(
            problems[2],
            SledSoftwareProblem::WrongImage {
                component: SledComponent::HostPhase1,
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
        assert!(message.contains("host OS phase 1 is running"));
    }

    #[test]
    fn test_unverified_is_a_problem() {
        let report = DeployedVersionsReport {
            sp: ActiveImageCheck::Error(
                FailedSpUpdateReason::CabooseNotInInventory(
                    CabooseWhich::SpSlot0,
                ),
            ),
            ..all_matching_report()
        };
        let problems = sled_software_problems(&test_sled(), report)
            .expect("board is a sled");
        assert_eq!(
            problems,
            vec![SledSoftwareProblem::Unverified {
                component: SledComponent::Sp,
                reason: "caboose for SpSlot0 is not in inventory".to_string(),
            }]
        );
    }

    #[test]
    fn test_not_a_sled() {
        let report = DeployedVersionsReport {
            host_phase_1: None,
            ..all_matching_report()
        };
        let error = sled_software_problems(&test_sled(), report)
            .expect_err("board is not a sled");
        assert!(matches!(error, SledSoftwareError::NotASled(_)));
    }
}
