// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_types::deployment::BlueprintArtifactMeasurements;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintSingleMeasurement;
use nexus_types::deployment::TargetReleaseDescription;
use omicron_common::api::external::TufRepoDescription;
use std::collections::BTreeSet;
use thiserror::Error;
use tufaceous_artifact::ArtifactKind;

#[derive(Debug, Error)]
pub(crate) enum MeasurementPlanError {
    #[error("TUF repo contained no measurements")]
    EmptyMeasurementSet,
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
        .iter()
        .filter_map(|artifact| {
            if artifact.id.kind == ArtifactKind::MEASUREMENT_CORPUS {
                Some(BlueprintSingleMeasurement {
                    version: BlueprintArtifactVersion::Available {
                        version: artifact.id.version.clone(),
                    },
                    hash: artifact.hash,
                })
            } else {
                None
            }
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
            let artifacts =
                BlueprintArtifactMeasurements::new(build_measurement_set(&c))
                    .ok_or(MeasurementPlanError::EmptyMeasurementSet)?;

            Ok(artifacts)
        }
        // Every other blueprint
        (
            TargetReleaseDescription::TufRepo(c),
            TargetReleaseDescription::TufRepo(p),
        ) => {
            let artifacts = BlueprintArtifactMeasurements::new(
                build_measurement_set(&c)
                    .into_iter()
                    .chain(build_measurement_set(&p).into_iter())
                    .collect(),
            )
            .ok_or(MeasurementPlanError::EmptyMeasurementSet)?;

            Ok(artifacts)
        }
        // This should never happen
        (
            TargetReleaseDescription::Initial,
            TargetReleaseDescription::TufRepo(_),
        ) => Err(MeasurementPlanError::InitialAfterRepo),
    }
}
