// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::scalar::ScalarEditor;
use crate::blueprint_builder::EditCounts;
use nexus_types::deployment::BlueprintMeasurements;

#[derive(Debug)]
pub(super) struct MeasurementEditor {
    measurements: ScalarEditor<BlueprintMeasurements>,
}

impl MeasurementEditor {
    pub fn new(measurements: BlueprintMeasurements) -> Self {
        Self { measurements: ScalarEditor::new(measurements.clone()) }
    }

    pub fn edit_counts(&self) -> EditCounts {
        let mut counts = EditCounts::zeroes();
        if self.measurements.is_modified() {
            counts.updated += 1;
        }
        counts
    }

    pub fn value(&self) -> BlueprintMeasurements {
        self.measurements.value().clone()
    }

    pub fn reset_to_parent_blueprint_measurements(
        &mut self,
    ) -> BlueprintMeasurements {
        self.measurements.reset_to_original().into_owned()
    }

    pub fn set_measurements(
        &mut self,
        new: BlueprintMeasurements,
    ) -> BlueprintMeasurements {
        self.measurements.set_value_if_unchanged(new).into_owned()
    }

    pub fn finalize(self) -> (BlueprintMeasurements, EditCounts) {
        let counts = self.edit_counts();

        (self.measurements.finalize(), counts)
    }
}
