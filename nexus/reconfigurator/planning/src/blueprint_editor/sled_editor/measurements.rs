// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::scalar::ScalarEditor;
use crate::blueprint_builder::EditCounts;
use nexus_types::deployment::BlueprintMeasurements;

#[derive(Debug)]
pub(super) struct MeasurementEditor {
    // Measurements we started with in case we need to reset
    old_measurements: BlueprintMeasurements,
    // Measurements we're changing
    pending_measurements: ScalarEditor<BlueprintMeasurements>,
    counts: EditCounts,
}

impl MeasurementEditor {
    pub fn new(measurements: BlueprintMeasurements) -> Self {
        Self {
            old_measurements: measurements.clone(),
            pending_measurements: ScalarEditor::new(measurements.clone()),
            counts: EditCounts::zeroes(),
        }
    }

    pub fn edit_counts(&self) -> EditCounts {
        self.counts
    }

    pub fn value(&self) -> BlueprintMeasurements {
        self.pending_measurements.value().clone()
    }

    pub fn delete_pending_measurements(&mut self) -> BlueprintMeasurements {
        let Self { old_measurements, pending_measurements: _, counts: _ } =
            self;
        // Hard reset to old value
        self.counts.updated += 1;
        self.pending_measurements
            .set_value(old_measurements.clone())
            .into_owned()
    }

    pub fn set_install_dataset(&mut self) -> BlueprintMeasurements {
        self.counts.updated += 1;
        self.pending_measurements
            .set_value(BlueprintMeasurements::InstallDataset)
            .into_owned()
    }

    pub fn set_measurements(
        &mut self,
        new: BlueprintMeasurements,
    ) -> BlueprintMeasurements {
        let Self { old_measurements: _, pending_measurements, counts: _ } =
            self;
        if new == *pending_measurements.value() {
            return new;
        }
        self.counts.updated += 1;
        self.pending_measurements.set_value(new).into_owned()
    }

    pub fn finalize(self) -> (BlueprintMeasurements, EditCounts) {
        let Self { old_measurements: _, pending_measurements, counts } = self;

        (pending_measurements.finalize(), counts)
    }
}
