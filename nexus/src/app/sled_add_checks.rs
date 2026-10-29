// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Checks run before adding sleds to the system

use super::deployment::BlueprintTargetReleaseStatus;
use nexus_db_model::TargetReleaseSource;
use nexus_db_queries::context::OpContext;
use nexus_reconfigurator_planning::mgs_updates::ActiveImageCheck;
use nexus_reconfigurator_planning::mgs_updates::DeployedSledVersions;
use nexus_reconfigurator_planning::mgs_updates::DeployedSledVersionsError;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::MgsUpdateComponent;
use nexus_types::external_api::rack::SledAddPrecheckOverrides;
use nexus_types::inventory::NotInInventory;
use nexus_types::inventory::SpType;
use nexus_types::tuf_repo::TufRepoDescription;
use omicron_common::api::external::Error;
use omicron_generation_kinds::TargetReleaseGeneration;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::fmt::Display;

impl super::Nexus {
    /// Checks that both the system and the sleds are ready for these sleds to
    /// be added.
    ///
    /// `overrides` describes checks that the operator asked to skip.  We still
    /// run those checks when we can, but we log any problem they find instead
    /// of returning it.
    pub(super) async fn sled_check_versions(
        &self,
        opctx: &OpContext,
        sleds: impl IntoIterator<Item = &BaseboardId>,
        overrides: &SledAddPrecheckOverrides,
    ) -> Result<(), Error> {
        let log = opctx.log.new(o!("overrides" => format!("{overrides:?}")));
        let target_release =
            self.datastore().target_release_get_current(opctx).await?;
        let release_source = target_release.release_source().map_err(|e| {
            Error::internal_error(&format!(
                "loading current target release: {e:#}"
            ))
        })?;
        let tuf_repo_id = match release_source {
            TargetReleaseSource::Unspecified => {
                // It's uncommon for no target release to be specified.  This
                // only happens when a system has never performed the initial
                // MUPdate recovery nor a self-service update.  This is
                // supported primarily for customer deployments that haven't yet
                // adopted self-service update.  In these cases, it's not
                // obvious what versions of software we would check against.
                info!(
                    &log,
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

        // Each call to `apply_override` names the single field of `overrides`
        // that applies to every error the check can return.
        let SledAddPrecheckOverrides {
            allow_without_settled_target_release,
            allow_unverified_sled_software,
        } = *overrides;

        apply_override(
            &log,
            allow_without_settled_target_release,
            check_system_settled(
                &blueprint,
                target_release.generation(),
                &repo,
            ),
        )?;

        let Some(inventory) =
            self.datastore().inventory_get_latest_collection(opctx).await?
        else {
            if allow_unverified_sled_software {
                warn!(
                    log,
                    "skipping software checks for sled add: no inventory \
                     collection found, and the operator overrode these checks"
                );
                return Ok(());
            }
            return Err(Error::internal_error("no inventory collection found"));
        };

        for sled in sleds {
            let report =
                match DeployedSledVersions::compute(sled, &inventory, &repo) {
                    Ok(report) => check_sled_software(sled, &repo, report),
                    Err(DeployedSledVersionsError::NotInInventory(err)) => {
                        Err(SledSoftwareError::IncompleteInventory {
                            sled: sled.clone(),
                            err,
                        })
                    }
                    Err(DeployedSledVersionsError::NotASled(sp_type)) => {
                        // This is not overridable.  Return immediately.
                        return Err(SledAddPrecheckError::NotASled {
                            board: sled.clone(),
                            sp_type,
                        }
                        .into());
                    }
                };
            apply_override(&log, allow_unverified_sled_software, report)?;
        }
        Ok(())
    }
}

/// Returns the error from a failed check unless `allowed` is set, in which
/// case the error is logged instead
fn apply_override<E>(
    log: &Logger,
    allowed: bool,
    result: Result<(), E>,
) -> Result<(), SledAddPrecheckError>
where
    E: std::error::Error + Into<SledAddPrecheckError>,
{
    let Err(error) = result else {
        return Ok(());
    };
    if !allowed {
        return Err(error.into());
    }
    warn!(
        log,
        "sled add precheck failed, but proceeding because the operator \
         overrode it";
        InlineErrorChain::new(&error),
    );
    Ok(())
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
) -> Result<(), SystemNotSettledError> {
    if blueprint.updates_suspended(target_release_generation) {
        return Err(SystemNotSettledError::UpdateSuspended {
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
            return Err(SystemNotSettledError::WaitingForMupdateToBeCleared {
                sled_id,
            });
        }
        BlueprintTargetReleaseStatus::FoundDifferentVersion {
            sled_id,
            version_found,
        } => {
            return Err(SystemNotSettledError::UpdateInProgress {
                target_release_version: repo.system_version.clone(),
                sled_id,
                version_found,
            });
        }
    }

    Ok(())
}

/// Checks that the software described by `report` matches `repo`
fn check_sled_software(
    sled: &BaseboardId,
    repo: &TufRepoDescription,
    report: DeployedSledVersions,
) -> Result<(), SledSoftwareError> {
    let problems = sled_software_problems(report);
    if !problems.is_empty() {
        return Err(SledSoftwareError::Mismatch {
            sled: sled.clone(),
            target_release_version: repo.system_version.clone(),
            problems,
        });
    }

    Ok(())
}

/// Reasons that a sled cannot be added right now
#[derive(Debug, thiserror::Error)]
enum SledAddPrecheckError {
    #[error(transparent)]
    SystemNotSettled(#[from] SystemNotSettledError),
    #[error(transparent)]
    SledSoftware(#[from] SledSoftwareError),
    #[error("board {board} is not a sled (its SP type is {sp_type:?})")]
    NotASled { board: BaseboardId, sp_type: SpType },
}

/// Reasons that the system has not settled on its target release
///
/// The operator can override all of these with
/// `allow_without_settled_target_release`.
#[derive(Debug, thiserror::Error)]
enum SystemNotSettledError {
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
}

/// Reasons that a sled's software could not be confirmed to match the target
/// release
///
/// The operator can override all of these with
/// `allow_unverified_sled_software`.
#[derive(Debug, thiserror::Error)]
enum SledSoftwareError {
    #[error("the software on sled {sled} could not be checked")]
    IncompleteInventory {
        sled: BaseboardId,
        #[source]
        err: NotInInventory,
    },
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

impl From<SledAddPrecheckError> for Error {
    fn from(error: SledAddPrecheckError) -> Self {
        Error::invalid_request(format!(
            "sled cannot be added: {}",
            InlineErrorChain::new(&error)
        ))
    }
}

fn join_problems(problems: &[SledSoftwareProblem]) -> String {
    problems.iter().map(|p| p.to_string()).collect::<Vec<_>>().join("; ")
}

/// Describes one way in which a sled's software does not match what's expected
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

/// Converts a `DeployedSledVersions` into a list of
/// problems that prevent the sled from being added
///
/// An empty list means the sled's software is ready.
fn sled_software_problems(
    report: DeployedSledVersions,
) -> Vec<SledSoftwareProblem> {
    let DeployedSledVersions { sp, rot, rot_bootloader, host_phase_1 } = report;
    [
        image_problem(MgsUpdateComponent::Sp, sp),
        image_problem(MgsUpdateComponent::Rot, rot),
        image_problem(MgsUpdateComponent::RotBootloader, rot_bootloader),
        image_problem(MgsUpdateComponent::HostOs, host_phase_1),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Converts one component's result from `DeployedSledVersions` into a
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
    use omicron_test_utils::dev::test_setup_log;
    use sled_hardware_types::GIMLET_SLED_MODEL;
    use std::collections::BTreeMap;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::ArtifactSet;
    use tufaceous_artifact::ArtifactVersion;

    fn test_sled() -> BaseboardId {
        BaseboardId {
            part_number: GIMLET_SLED_MODEL.to_string(),
            serial_number: "test-sled".to_string(),
        }
    }

    fn all_matching_report() -> DeployedSledVersions {
        DeployedSledVersions {
            sp: ActiveImageCheck::Matches,
            rot: ActiveImageCheck::Matches,
            rot_bootloader: ActiveImageCheck::Matches,
            host_phase_1: ActiveImageCheck::Matches,
        }
    }

    #[test]
    fn test_system_not_settled() {
        let mut blueprint = BlueprintBuilder::build_empty("test");
        let repo = TufRepoDescription {
            artifacts: ArtifactSet::default(),
            metadata: BTreeMap::new(),
            system_version: "17.0.0".parse().expect("valid version"),
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
                SystemNotSettledError::UpdateSuspended { current, minimum }
                    if current == generation && minimum == generation.next()
            ),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn test_no_problems_when_all_match() {
        let problems = sled_software_problems(all_matching_report());
        assert_eq!(problems, Vec::new());
    }

    #[test]
    fn test_problems_reported() {
        let report = DeployedSledVersions {
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
            host_phase_1: ActiveImageCheck::Differs {
                found: ArtifactHash([1; 32]),
                expected: ArtifactHash([2; 32]),
            },
        };
        let problems = sled_software_problems(report);
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
            target_release_version: "17.0.0".parse().expect("valid version"),
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
        let report = DeployedSledVersions {
            sp: ActiveImageCheck::Error(ImageCheckError::Active(
                CabooseVersionError::NotInInventory(CabooseWhich::SpSlot0),
            )),
            ..all_matching_report()
        };
        let problems = sled_software_problems(report);
        assert_eq!(
            problems,
            vec![SledSoftwareProblem::Unverified {
                component: MgsUpdateComponent::Sp,
                reason: "could not identify the active image: caboose for \
                         SpSlot0 is not in inventory"
                    .to_string(),
            }]
        );
    }

    #[test]
    fn test_apply_override() {
        let logctx = test_setup_log("test_apply_override");
        let log = &logctx.log;
        let mismatch = || SledSoftwareError::Mismatch {
            sled: test_sled(),
            target_release_version: "17.0.0".parse().expect("valid version"),
            problems: vec![SledSoftwareProblem::WrongImage {
                component: MgsUpdateComponent::Sp,
                found: "1.0.0".to_string(),
                expected: "2.0.0".to_string(),
            }],
        };

        // A check that passed is never an error, whether or not it was
        // overridden.
        apply_override::<SledSoftwareError>(log, false, Ok(()))
            .expect("check passed");
        apply_override::<SledSoftwareError>(log, true, Ok(()))
            .expect("check passed");

        // A check that failed is an error unless it was overridden.
        let error = apply_override(log, false, Err(mismatch()))
            .expect_err("not overridden");
        assert!(
            matches!(
                error,
                SledAddPrecheckError::SledSoftware(
                    SledSoftwareError::Mismatch { .. }
                )
            ),
            "unexpected error: {error:?}"
        );
        apply_override(log, true, Err(mismatch())).expect("overridden");

        logctx.cleanup_successful();
    }
}
