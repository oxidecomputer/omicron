// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_types::deployment::BlueprintArtifactMeasurements;
/// Measurements??????
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintMeasurements;
use nexus_types::deployment::BlueprintSingleMeasurement;
use nexus_types::deployment::TargetReleaseDescription;
use omicron_common::api::external::TufRepoDescription;
use omicron_uuid_kinds::SledUuid;
use slog::error;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use tufaceous_artifact::ArtifactKind;

pub(crate) struct PendingMeasurements {
    by_sled: BTreeMap<SledUuid, BlueprintMeasurements>,
}

impl PendingMeasurements {
    pub(super) fn empty() -> Self {
        Self { by_sled: BTreeMap::new() }
    }

    pub(crate) fn into_iter(
        self,
    ) -> impl Iterator<Item = (SledUuid, BlueprintMeasurements)> {
        self.by_sled.into_iter()
    }

    // We need to insert both current and previous at the same time
    // to avoid triggering the multiple change check
    fn insert_all_measurements(
        &mut self,
        log: &slog::Logger,
        sled_id: SledUuid,
        current_artifacts: &TufRepoDescription,
        previous_artifacts: Option<&TufRepoDescription>,
    ) {
        let mut measurements = Vec::new();

        for artifact in &current_artifacts.artifacts {
            if artifact.id.kind == ArtifactKind::MEASUREMENT_CORPUS {
                measurements.push(artifact);
            }
        }

        if let Some(previous) = previous_artifacts {
            for artifact in &previous.artifacts {
                if artifact.id.kind == ArtifactKind::MEASUREMENT_CORPUS {
                    measurements.push(artifact);
                }
            }
        }

        let artifacts = measurements
            .into_iter()
            .map(|artifact| BlueprintSingleMeasurement {
                version: BlueprintArtifactVersion::Available {
                    version: artifact.id.version.clone(),
                },
                hash: artifact.hash,
            })
            .collect();

        let artifacts = match BlueprintArtifactMeasurements::new(artifacts) {
            None => {
                error!(log, "Found no measurement artifacts");
                return;
            }
            Some(m) => m,
        };

        let contents = BlueprintMeasurements::Artifacts { artifacts };
        let prev = self.by_sled.insert(sled_id, contents);
        assert!(prev.is_none(), "recorded multiple changes for sled {sled_id}");
    }
}

pub(crate) fn plan_measurement_updates(
    log: &slog::Logger,
    sled_id: &BTreeSet<SledUuid>,
    current_artifacts: &TargetReleaseDescription,
    previous_artifacts: &TargetReleaseDescription,
) -> PendingMeasurements {
    let mut pending = PendingMeasurements::empty();

    match (current_artifacts, previous_artifacts) {
        // Very first blueprint, we return an empty set indicating we are using the install dataset
        (
            TargetReleaseDescription::Initial,
            TargetReleaseDescription::Initial,
        ) => {}
        // Second blueprint. It's okay to just take the current set
        (
            TargetReleaseDescription::TufRepo(c),
            TargetReleaseDescription::Initial,
        ) => {
            for s in sled_id {
                pending.insert_all_measurements(log, *s, &c, None);
            }
        }
        // Every other blueprint
        (
            TargetReleaseDescription::TufRepo(c),
            TargetReleaseDescription::TufRepo(p),
        ) => {
            for s in sled_id {
                pending.insert_all_measurements(log, *s, &c, Some(&p));
            }
        }
        // This should never happen
        (
            TargetReleaseDescription::Initial,
            TargetReleaseDescription::TufRepo(_),
        ) => {
            panic!("current blueprint is initial but previous is a TUF repo");
        }
    };

    pending
}
