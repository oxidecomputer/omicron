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
) -> BlueprintMeasurementsDesiredContents {
    // Our goal is to have the new set contain what will be our current measurements
    // and only the the old entries that are immediately present

    let mut going_in = old.into_iter()
            // Step 1: grab anything that doesn't need to be pruned
            .filter(|x| x.prune == false)
            // Step 2: mark anything that isn't  in our new set as prunable
            .map(|mut x| { if !new.contains(&x) { x.prune = true; } x }).collect::<BTreeSet<BlueprintSingleMeasurement>>();

    // Step 3: Union
    //let mut going_in = going_in.union(&new).collect();

    let mut measurements : BTreeSet<BlueprintSingleMeasurement> = BTreeSet::new();
    //measurements.append(&mut going_in);
    for m in going_in.union(&new) {
        measurements.insert(m.clone());
    }

    BlueprintMeasurementsDesiredContents {
        measurements
    }
    
}

/// No no okay this is where the work needs to happen!
///
/// OKAY: so we start a new editor with our EXISTING SET of previous
/// and current contents.
///
/// By the time we `finalize` we need to DROP the current set of
#[derive(Debug)]
pub(super) struct MeasurementEditor {
    old_measurements: ScalarEditor<BTreeSet<BlueprintSingleMeasurement>>,
    new_measurements: ScalarEditor<BTreeSet<BlueprintSingleMeasurement>>,
}

impl MeasurementEditor {
    pub fn new(measurements: BlueprintMeasurementsDesiredContents) -> Self {
        Self {
            old_measurements: ScalarEditor::new(measurements.measurements),
            new_measurements: ScalarEditor::new(BTreeSet::new()),
        }
    }

    pub fn value(&self) -> BlueprintMeasurementsDesiredContents {
        merge_sets(
            self.old_measurements.value().clone(),
            self.new_measurements.value().clone(),
        )
    }

    /// Force overwrite our settings as if we called `new`
    pub fn set_value(
        &mut self,
        measurements: BlueprintMeasurementsDesiredContents,
    ) -> BlueprintMeasurementsDesiredContents {
        let previous_set = BlueprintMeasurementsDesiredContents {
            measurements: self
                .old_measurements
                .set_value(measurements.measurements)
                .into_owned()
                .into(),
        };
        self.new_measurements.set_value(BTreeSet::new());
        previous_set
    }

    pub fn set_current_measurements(
        &mut self,
        current: BlueprintMeasurementSetDesiredContents,
    ) -> BlueprintMeasurementsDesiredContents {
        self.new_measurements.set_value(current.into());
        self.value()
    }

    pub fn is_modified(&self) -> bool {
        let Self { old_measurements, new_measurements } = self;
        old_measurements.is_modified()
            || new_measurements.is_modified()
    }

    pub fn finalize(self) -> BlueprintMeasurementsDesiredContents {
        let Self { old_measurements, new_measurements } = self;

        merge_sets(
            old_measurements.finalize(),
            new_measurements.finalize(),
        )
    }
}
