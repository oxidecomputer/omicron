// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// Measurements??????
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintMeasurementSetDesiredContents;
use nexus_types::deployment::BlueprintSingleMeasurement;
use nexus_types::deployment::TargetReleaseDescription;
use omicron_common::api::external::TufRepoDescription;
use omicron_uuid_kinds::SledUuid;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use tufaceous_artifact::ArtifactKind;

pub(crate) struct PendingMeasurements {
    by_sled: BTreeMap<SledUuid, BlueprintMeasurementSetDesiredContents>,
}

impl PendingMeasurements {
    pub(super) fn empty() -> Self {
        Self { by_sled: BTreeMap::new() }
    }

    pub(crate) fn into_iter(
        self,
    ) -> impl Iterator<Item = (SledUuid, BlueprintMeasurementSetDesiredContents)>
    {
        self.by_sled.into_iter()
    }

    fn insert_all_measurements(
        &mut self,
        sled_id: SledUuid,
        current_artifacts: &TufRepoDescription,
    ) {
        let mut measurements = Vec::new();

        for artifact in &current_artifacts.artifacts {
            // XXX
            if artifact.id.kind == ArtifactKind::TRAMPOLINE_PHASE_2 {
                measurements.push(artifact);
            }
        }

        let contents = BlueprintMeasurementSetDesiredContents::Artifacts {
            artifacts: measurements
                .into_iter()
                .map(|artifact| BlueprintSingleMeasurement {
                    version: BlueprintArtifactVersion::Available {
                        version: artifact.id.version.clone(),
                    },
                    hash: artifact.hash,
                })
                .collect(),
        };
        let prev = self.by_sled.insert(sled_id, contents);
        assert!(prev.is_none(), "recorded multiple changes for sled {sled_id}");
    }
}

pub(crate) fn plan_measurement_updates(
    _log: &slog::Logger,
    //inventory: &Collection,
    sled_id: &BTreeSet<SledUuid>,
    current_artifacts: &TargetReleaseDescription,
) -> PendingMeasurements {
    let mut pending = PendingMeasurements::empty();

    let current_artifacts = match current_artifacts {
        TargetReleaseDescription::Initial => todo!(),
        TargetReleaseDescription::TufRepo(d) => d,
    };

    for s in sled_id {
        pending.insert_all_measurements(*s, &current_artifacts);
    }
    pending
}
