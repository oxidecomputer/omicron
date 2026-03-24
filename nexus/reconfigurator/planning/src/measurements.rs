// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_types::deployment::BlueprintArtifactMeasurements;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintSingleMeasurement;
use nexus_types::deployment::TargetReleaseDescription;
use omicron_common::update::TufRepoDescription;
use std::collections::BTreeSet;
use thiserror::Error;
use tufaceous_artifact::KnownArtifactTags;

#[derive(Debug, Error)]
pub(crate) enum MeasurementPlanError {
    #[error("TUF repo {0} contained no measurements")]
    EmptyMeasurementSet(semver::Version),
    #[error("Attempted to plan measurements on an initial blueprint")]
    PlanningInitial,
    #[error("Found initial blueprint when there should be a TUF repo")]
    InitialAfterRepo,
}

fn build_measurement_set(
    artifacts: &TufRepoDescription,
) -> BTreeSet<BlueprintSingleMeasurement> {
    artifacts
        .artifacts
        .get_all(KnownArtifactTags::MeasurementCorpus)
        .map(|artifact| BlueprintSingleMeasurement {
            version: BlueprintArtifactVersion::Available {
                version: artifact.version.clone(),
            },
            hash: artifact.hash,
        })
        .collect()
}

pub(crate) fn plan_measurement_updates(
    current_artifacts: &TargetReleaseDescription,
    previous_artifacts: &TargetReleaseDescription,
) -> Result<BlueprintArtifactMeasurements, MeasurementPlanError> {
    match (current_artifacts, previous_artifacts) {
        // Very first blueprint, we should never get here because planner logic
        // should prevent us from proceding here until we have a TUF repo
        (
            TargetReleaseDescription::Initial,
            TargetReleaseDescription::Initial,
        ) => Err(MeasurementPlanError::PlanningInitial),
        // Second blueprint. It's okay to just take the current set
        (
            TargetReleaseDescription::TufRepo(c),
            TargetReleaseDescription::Initial,
        ) => {
            let current = build_measurement_set(&c);
            let artifacts = BlueprintArtifactMeasurements::new(current)
                .ok_or_else(|| {
                    MeasurementPlanError::EmptyMeasurementSet(
                        c.system_version.clone(),
                    )
                })?;

            Ok(artifacts)
        }
        // Every other blueprint
        (
            TargetReleaseDescription::TufRepo(c),
            TargetReleaseDescription::TufRepo(p),
        ) => {
            let current = build_measurement_set(&c);
            if current.is_empty() {
                return Err(MeasurementPlanError::EmptyMeasurementSet(
                    c.system_version.clone(),
                ));
            }
            let previous = build_measurement_set(&p);
            if previous.is_empty() {
                return Err(MeasurementPlanError::EmptyMeasurementSet(
                    p.system_version.clone(),
                ));
            }
            let artifacts = BlueprintArtifactMeasurements::new(
                current.into_iter().chain(previous).collect(),
            )
            .expect("we already checked both sets were non-empty");

            Ok(artifacts)
        }
        // This should never happen
        (
            TargetReleaseDescription::Initial,
            TargetReleaseDescription::TufRepo(_),
        ) => Err(MeasurementPlanError::InitialAfterRepo),
    }
}
