// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::scalar::ScalarEditor;
use nexus_types::deployment::BlueprintMeasurementSetDesiredContents;
use nexus_types::deployment::BlueprintMeasurementsDesiredContents;
use nexus_types::deployment::BlueprintSingleMeasurement;

use std::collections::BTreeSet;

fn merge_sets(
    old: BTreeSet<BlueprintSingleMeasurement>,
    new: BTreeSet<BlueprintSingleMeasurement>,
) -> BTreeSet<BlueprintSingleMeasurement> {
    let merged = old
        .clone()
        .union(&new)
        .cloned()
        .collect::<BTreeSet<BlueprintSingleMeasurement>>();

    // If we've produced the same output after adding the new
    // measurements just return what we have
    if merged == old {
        return merged;
    }

    let merged = merged
        .into_iter()
        .filter(|x| x.prune == false)
        .collect::<BTreeSet<BlueprintSingleMeasurement>>();

    let mut measurements: BTreeSet<BlueprintSingleMeasurement> =
        BTreeSet::new();
    for mut m in merged.into_iter() {
        if !new.contains(&m) {
            m.prune = true;
        }
        measurements.insert(m.clone());
    }

    measurements
}

/// No no okay this is where the work needs to happen!
///
/// OKAY: so we start a new editor with our EXISTING SET of previous
/// and current contents.
///
/// By the time we `finalize` we need to DROP the current set of
#[derive(Debug)]
pub(super) struct MeasurementEditor {
    measurements: ScalarEditor<BTreeSet<BlueprintSingleMeasurement>>,
    pending_measurements: ScalarEditor<BTreeSet<BlueprintSingleMeasurement>>,
}

impl MeasurementEditor {
    pub fn new(measurements: BlueprintMeasurementsDesiredContents) -> Self {
        Self {
            measurements: ScalarEditor::new(measurements.measurements),
            pending_measurements: ScalarEditor::new(BTreeSet::new()),
        }
    }

    pub fn value(&self) -> BlueprintMeasurementsDesiredContents {
        BlueprintMeasurementsDesiredContents {
            measurements: merge_sets(
                self.measurements.value().clone(),
                self.pending_measurements.value().clone(),
            ),
        }
    }

    pub fn delete_pending_measurements(
        &mut self,
    ) -> BlueprintMeasurementsDesiredContents {
        let previous_set = self.value();
        // Hard reset! Nothing pending
        self.pending_measurements.set_value(BTreeSet::new());
        previous_set
    }

    pub fn replace_measurements(
        &mut self,
        measurements: BlueprintMeasurementsDesiredContents,
    ) -> BlueprintMeasurementsDesiredContents {
        let previous_set = self.value();
        self.measurements.set_value(measurements.measurements.into());
        // Hard reset! Nothing pending
        self.pending_measurements.set_value(BTreeSet::new());
        previous_set
    }

    pub fn merge_new_measurements(
        &mut self,
        new: BlueprintMeasurementSetDesiredContents,
    ) -> BlueprintMeasurementsDesiredContents {
        self.pending_measurements.set_value(new.into());
        self.value()
    }

    pub fn is_modified(&self) -> bool {
        let Self { measurements, pending_measurements } = self;
        measurements.is_modified() && pending_measurements.is_modified()
    }

    pub fn finalize(self) -> BlueprintMeasurementsDesiredContents {
        let Self { measurements, pending_measurements } = self;

        BlueprintMeasurementsDesiredContents {
            measurements: merge_sets(
                measurements.finalize(),
                pending_measurements.finalize(),
            ),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use nexus_types::deployment::BlueprintArtifactVersion;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::ArtifactVersion;

    macro_rules! fake_measurement {
        ($version: expr, $hv: expr, $prune: expr) => {
            BlueprintSingleMeasurement {
                version: BlueprintArtifactVersion::Available {
                    version: $version,
                },
                hash: ArtifactHash([$hv; 32]),
                prune: $prune,
            }
        };
    }

    #[test]
    fn basic_measurement_set_tests() {
        // Doing simple set operations like it's an undergraduate
        // math theory course
        let set_a_version =
            ArtifactVersion::new_static("0.0.1").expect("bad version");
        let set_a_entries = [
            fake_measurement!(set_a_version.clone(), 1, false),
            fake_measurement!(set_a_version.clone(), 2, false),
            fake_measurement!(set_a_version.clone(), 3, false),
        ];
        let set_a = BTreeSet::from(set_a_entries.clone());

        // Set b has 2 old measurements and one old one
        let set_b_version =
            ArtifactVersion::new_static("0.0.2").expect("bad version");
        let set_b_entries = [
            fake_measurement!(set_a_version.clone(), 2, false),
            fake_measurement!(set_a_version.clone(), 3, false),
            fake_measurement!(set_b_version.clone(), 4, false),
        ];
        let set_b = BTreeSet::from(set_b_entries.clone());
        let new_set = merge_sets(set_a.clone(), set_b.clone());

        let expected_entries = [
            fake_measurement!(set_a_version.clone(), 1, true),
            fake_measurement!(set_a_version.clone(), 2, false),
            fake_measurement!(set_a_version.clone(), 3, false),
            fake_measurement!(set_b_version.clone(), 4, false),
        ];
        let expected_set = BTreeSet::from(expected_entries.clone());

        assert!(new_set == expected_set);

        // run the merge again, we don't want to prune
        let new_set = merge_sets(new_set, set_b);

        assert!(new_set == expected_set);

        let set_c_version =
            ArtifactVersion::new_static("0.0.3").expect("bad version");
        let set_c_entries = [
            fake_measurement!(set_c_version.clone(), 5, false),
            fake_measurement!(set_c_version.clone(), 6, false),
            fake_measurement!(set_c_version.clone(), 7, false),
        ];
        let set_c = BTreeSet::from(set_c_entries.clone());
        let new_set = merge_sets(new_set.clone(), set_c.clone());

        // Third set, this time we should prune
        let expected_entries = [
            fake_measurement!(set_a_version.clone(), 2, true),
            fake_measurement!(set_a_version.clone(), 3, true),
            fake_measurement!(set_b_version.clone(), 4, true),
            fake_measurement!(set_c_version.clone(), 5, false),
            fake_measurement!(set_c_version.clone(), 6, false),
            fake_measurement!(set_c_version.clone(), 7, false),
        ];
        let expected_set = BTreeSet::from(expected_entries.clone());
        assert!(new_set == expected_set);
    }
}
