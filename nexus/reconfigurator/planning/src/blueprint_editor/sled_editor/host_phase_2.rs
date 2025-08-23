// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::scalar::ScalarEditor;
use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;
use omicron_common::disk::M2Slot;

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

    pub fn value(&self) -> BlueprintHostPhase2DesiredSlots {
        BlueprintHostPhase2DesiredSlots {
            slot_a: self.slot_a.value().clone(),
            slot_b: self.slot_b.value().clone(),
        }
    }

    /// Set the host phase 2 information for this sled, returning the previous value.
    pub fn set_value(
        &mut self,
        host_phase_2: BlueprintHostPhase2DesiredSlots,
    ) -> BlueprintHostPhase2DesiredSlots {
        let BlueprintHostPhase2DesiredSlots { slot_a, slot_b } = host_phase_2;
        let previous = BlueprintHostPhase2DesiredSlots {
            slot_a: self.slot_a.set_value(slot_a).into_owned(),
            slot_b: self.slot_b.set_value(slot_b).into_owned(),
        };
        previous
    }

    pub fn set_slot(
        &mut self,
        slot: M2Slot,
        desired: BlueprintHostPhase2DesiredContents,
    ) {
        let target = match slot {
            M2Slot::A => &mut self.slot_a,
            M2Slot::B => &mut self.slot_b,
        };
        target.set_value(desired);
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
