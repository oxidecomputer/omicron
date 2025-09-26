// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::scalar::ScalarEditor;
use nexus_types::deployment::BlueprintMeasurementSetDesiredContents;
use nexus_types::deployment::BlueprintMeasurementsDesiredContents;

fn generate_from_choices(
    old_previous: BlueprintMeasurementSetDesiredContents,
    old_current: BlueprintMeasurementSetDesiredContents,
    new_current: Option<BlueprintMeasurementSetDesiredContents>,
) -> BlueprintMeasurementsDesiredContents {
    match new_current {
        // This is the easy case, simply return what we have
        None => BlueprintMeasurementsDesiredContents {
            previous: old_previous,
            current: old_current,
        },
        // We drop our old previous, old current becomes our previuos, new becomes our current
        Some(new) => BlueprintMeasurementsDesiredContents {
            previous: old_current,
            current: new,
        },
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
    old_previous: ScalarEditor<BlueprintMeasurementSetDesiredContents>,
    old_current: ScalarEditor<BlueprintMeasurementSetDesiredContents>,
    new_current: ScalarEditor<Option<BlueprintMeasurementSetDesiredContents>>,
}

impl MeasurementEditor {
    pub fn new(measurements: BlueprintMeasurementsDesiredContents) -> Self {
        Self {
            old_previous: ScalarEditor::new(measurements.previous),
            old_current: ScalarEditor::new(measurements.current),
            new_current: ScalarEditor::new(None),
        }
    }

    pub fn value(&self) -> BlueprintMeasurementsDesiredContents {
        // XXX not sure this is correct
        generate_from_choices(
            self.old_previous.value().clone(),
            self.old_current.value().clone(),
            self.new_current.value().clone(),
        )
    }

    pub fn set_value(
        &mut self,
        measurements: BlueprintMeasurementsDesiredContents,
    ) -> BlueprintMeasurementsDesiredContents {
        // XXX I think this is wrong???
        let previous_set = BlueprintMeasurementsDesiredContents {
            previous: self
                .old_previous
                .set_value(measurements.previous)
                .into_owned(),
            current: self
                .old_current
                .set_value(measurements.current)
                .into_owned(),
        };
        previous_set
    }

    pub fn set_current_measurements(
        &mut self,
        current: BlueprintMeasurementSetDesiredContents,
    ) -> BlueprintMeasurementsDesiredContents {
        self.new_current.set_value(Some(current));
        self.value()
    }

    pub fn is_modified(&self) -> bool {
        let Self { old_previous, old_current, new_current } = self;
        old_previous.is_modified()
            || old_current.is_modified()
            || new_current.is_modified()
    }

    pub fn finalize(self) -> BlueprintMeasurementsDesiredContents {
        let Self { old_previous, old_current, new_current } = self;

        generate_from_choices(
            old_previous.finalize(),
            old_current.finalize(),
            new_current.finalize(),
        )
    }
}
