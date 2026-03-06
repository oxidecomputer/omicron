// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_types::deployment::BlueprintArtifactMeasurements;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintMeasurements;
use nexus_types::deployment::BlueprintSingleMeasurement;
use nexus_types::deployment::TargetReleaseDescription;
use omicron_common::api::external::TufRepoDescription;
use std::collections::BTreeSet;
use tufaceous_artifact::ArtifactKind;

pub(crate) enum MeasurementPlanError {
    EmptyMeasurementSet,
}

fn build_measurement_set(
    artifacts: &TufRepoDescription,
) -> BTreeSet<BlueprintSingleMeasurement> {
    let mut measurements = Vec::new();

    for artifact in &artifacts.artifacts {
        if artifact.id.kind == ArtifactKind::MEASUREMENT_CORPUS {
            measurements.push(artifact);
        }
    }

    measurements
        .into_iter()
        .map(|artifact| BlueprintSingleMeasurement {
            version: BlueprintArtifactVersion::Available {
                version: artifact.id.version.clone(),
            },
            hash: artifact.hash,
        })
        .collect()
}

pub(crate) fn plan_measurement_updates(
    current_artifacts: &TargetReleaseDescription,
    previous_artifacts: &TargetReleaseDescription,
) -> Result<BlueprintMeasurements, MeasurementPlanError> {
    let measurements = match (current_artifacts, previous_artifacts) {
        // Very first blueprint, we return an empty set indicating we are using the install dataset
        (
            TargetReleaseDescription::Initial,
            TargetReleaseDescription::Initial,
        ) => BlueprintMeasurements::InstallDataset,
        // Second blueprint. It's okay to just take the current set
        (
            TargetReleaseDescription::TufRepo(c),
            TargetReleaseDescription::Initial,
        ) => {
            let artifacts =
                BlueprintArtifactMeasurements::new(build_measurement_set(&c))
                    .ok_or(MeasurementPlanError::EmptyMeasurementSet)?;

            BlueprintMeasurements::Artifacts { artifacts }
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

            BlueprintMeasurements::Artifacts { artifacts }
        }
        // This should never happen
        (
            TargetReleaseDescription::Initial,
            TargetReleaseDescription::TufRepo(_),
        ) => {
            panic!("current blueprint is initial but previous is a TUF repo");
        }
    };

    Ok(measurements)
}
