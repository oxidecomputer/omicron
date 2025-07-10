// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::scalar::ScalarEditor;
use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;

#[derive(Debug)]
pub(super) struct HostPhase2Editor {
    slot_a: ScalarEditor<BlueprintHostPhase2DesiredContents>,
    slot_b: ScalarEditor<BlueprintHostPhase2DesiredContents>,
}

impl HostPhase2Editor {
    pub fn new(host_phase_2: BlueprintHostPhase2DesiredSlots) -> Self {
        let BlueprintHostPhase2DesiredSlots { slot_a, slot_b } = host_phase_2;
        Self {
            slot_a: ScalarEditor::new(slot_a),
            slot_b: ScalarEditor::new(slot_b),
        }
    }

    pub fn set_value(&mut self, host_phase_2: BlueprintHostPhase2DesiredSlots) {
        let BlueprintHostPhase2DesiredSlots { slot_a, slot_b } = host_phase_2;
        self.slot_a.set_value(slot_a);
        self.slot_b.set_value(slot_b);
    }

    pub fn is_modified(&self) -> bool {
        let Self { slot_a, slot_b } = self;
        slot_a.is_modified() || slot_b.is_modified()
    }

    pub fn finalize(self) -> BlueprintHostPhase2DesiredSlots {
        let Self { slot_a, slot_b } = self;
        BlueprintHostPhase2DesiredSlots {
            slot_a: slot_a.finalize(),
            slot_b: slot_b.finalize(),
        }
    }
}
