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
//!    collection.  We use the planner's own logic for choosing artifacts (see
//!    `nexus_reconfigurator_planning::mgs_updates::check_active_versions()`),
//!    so this check agrees with what the planner would decide to update.
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

use super::deployment::SystemNotSettledError;
use super::deployment::validate_system_settled_on_target_release;
use nexus_db_model::TargetReleaseSource;
use nexus_db_queries::context::OpContext;
use nexus_reconfigurator_planning::mgs_updates::ActiveImageCheck;
use nexus_reconfigurator_planning::mgs_updates::ActiveVersionsReport;
use nexus_reconfigurator_planning::mgs_updates::BoardNotInInventory;
use nexus_reconfigurator_planning::mgs_updates::RotBootPreferenceCheck;
use nexus_reconfigurator_planning::mgs_updates::check_active_versions;
use nexus_types::deployment::Blueprint;
use nexus_types::inventory::Collection;
use nexus_types::tuf_repo::TufRepoDescription;
use omicron_common::api::external::Error;
use omicron_generation_kinds::TargetReleaseGeneration;
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
    validate_system_settled_on_target_release(
        blueprint,
        target_release_generation,
        &repo.system_version,
    )?;

    let Some(inventory) = inventory else {
        return Err(SledSoftwareError::NoInventory);
    };

    for sled in sleds {
        let report = check_active_versions(sled, inventory, repo)?;
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
    #[error("the system is not ready to add sleds")]
    SystemNotSettled(#[from] SystemNotSettledError),
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
    /// the component's active image could not be compared with the target
    /// release
    Indeterminate { component: SledComponent, reason: String },
    /// the RoT's boot preferences disagree with its active slot
    RotBootPreferenceUnsettled { detail: String },
    /// the RoT's boot preferences could not be determined
    RotBootPreferenceIndeterminate,
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
            SledSoftwareProblem::Indeterminate { component, reason } => {
                write!(f, "{component} could not be checked: {reason}")
            }
            SledSoftwareProblem::RotBootPreferenceUnsettled { detail } => {
                write!(
                    f,
                    "RoT boot preferences show an update that has not fully \
                     taken effect ({detail})"
                )
            }
            SledSoftwareProblem::RotBootPreferenceIndeterminate => {
                write!(f, "RoT boot preferences could not be determined")
            }
        }
    }
}

/// Converts a report from `check_active_versions()` into a list of problems
/// that prevent the sled from being added
///
/// An empty list means the sled's software is ready.
fn sled_software_problems(
    sled: &BaseboardId,
    report: ActiveVersionsReport,
) -> Result<Vec<SledSoftwareProblem>, SledSoftwareError> {
    let ActiveVersionsReport {
        sp,
        rot,
        rot_bootloader,
        host_phase_1,
        rot_boot_preferences,
    } = report;

    // A board without host phase 1 is not a sled.
    let Some(host_phase_1) = host_phase_1 else {
        return Err(SledSoftwareError::NotASled(sled.clone()));
    };

    let mut problems: Vec<_> = [
        image_problem(SledComponent::Sp, sp),
        image_problem(SledComponent::Rot, rot),
        image_problem(SledComponent::RotBootloader, rot_bootloader),
        image_problem(SledComponent::HostPhase1, host_phase_1),
    ]
    .into_iter()
    .flatten()
    .collect();

    match rot_boot_preferences {
        RotBootPreferenceCheck::Settled => (),
        RotBootPreferenceCheck::Unsettled {
            active_slot,
            persistent_boot_preference,
            pending_persistent_boot_preference,
            transient_boot_preference,
        } => {
            problems.push(SledSoftwareProblem::RotBootPreferenceUnsettled {
                detail: format!(
                    "active slot: {active_slot:?}, \
                     persistent boot preference: \
                     {persistent_boot_preference:?}, \
                     pending persistent boot preference: \
                     {pending_persistent_boot_preference:?}, \
                     transient boot preference: \
                     {transient_boot_preference:?}"
                ),
            });
        }
        RotBootPreferenceCheck::Indeterminate => {
            problems.push(SledSoftwareProblem::RotBootPreferenceIndeterminate);
        }
    }

    Ok(problems)
}

/// Converts one component's result from `check_active_versions()` into a
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
        ActiveImageCheck::Indeterminate(error) => {
            Some(SledSoftwareProblem::Indeterminate {
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
    use nexus_types::deployment::planning_report::FailedSpUpdateReason;
    use nexus_types::inventory::CabooseWhich;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::ArtifactVersion;

    fn test_sled() -> BaseboardId {
        BaseboardId {
            part_number: "913-0000019".to_string(),
            serial_number: "test-sled".to_string(),
        }
    }

    fn all_matching_report() -> ActiveVersionsReport {
        ActiveVersionsReport {
            sp: ActiveImageCheck::Matches,
            rot: ActiveImageCheck::Matches,
            rot_bootloader: ActiveImageCheck::Matches,
            host_phase_1: Some(ActiveImageCheck::Matches),
            rot_boot_preferences: RotBootPreferenceCheck::Settled,
        }
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
        let report = ActiveVersionsReport {
            sp: ActiveImageCheck::Differs {
                found: ArtifactVersion::new_const("1.0.0"),
                expected: ArtifactVersion::new_const("2.0.0"),
            },
            rot: ActiveImageCheck::Matches,
            rot_bootloader: ActiveImageCheck::Matches,
            host_phase_1: Some(ActiveImageCheck::Differs {
                found: ArtifactHash([1; 32]),
                expected: ArtifactHash([2; 32]),
            }),
            rot_boot_preferences: RotBootPreferenceCheck::Unsettled {
                active_slot: RotSlot::A,
                persistent_boot_preference: RotSlot::B,
                pending_persistent_boot_preference: None,
                transient_boot_preference: None,
            },
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
        assert_eq!(
            problems[1],
            SledSoftwareProblem::WrongImage {
                component: SledComponent::HostPhase1,
                found: ArtifactHash([1; 32]).to_string(),
                expected: ArtifactHash([2; 32]).to_string(),
            }
        );
        assert!(matches!(
            problems[2],
            SledSoftwareProblem::RotBootPreferenceUnsettled { .. }
        ));

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
        assert!(message.contains("host OS phase 1 is running"));
        assert!(message.contains("RoT boot preferences show an update"));
    }

    #[test]
    fn test_indeterminate_is_a_problem() {
        let report = ActiveVersionsReport {
            sp: ActiveImageCheck::Indeterminate(
                FailedSpUpdateReason::CabooseNotInInventory(
                    CabooseWhich::SpSlot0,
                ),
            ),
            rot_boot_preferences: RotBootPreferenceCheck::Indeterminate,
            ..all_matching_report()
        };
        let problems = sled_software_problems(&test_sled(), report)
            .expect("board is a sled");
        assert_eq!(
            problems,
            vec![
                SledSoftwareProblem::Indeterminate {
                    component: SledComponent::Sp,
                    reason: "caboose for SpSlot0 is not in inventory"
                        .to_string(),
                },
                SledSoftwareProblem::RotBootPreferenceIndeterminate,
            ]
        );
    }

    #[test]
    fn test_not_a_sled() {
        let report = ActiveVersionsReport {
            host_phase_1: None,
            ..all_matching_report()
        };
        let error = sled_software_problems(&test_sled(), report)
            .expect_err("board is not a sled");
        assert!(matches!(error, SledSoftwareError::NotASled(_)));
    }
}
