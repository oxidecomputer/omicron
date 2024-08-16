// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use serde::{Deserialize, Serialize};
use wicket_common::update_events::UpdateComponent;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ForceUpdateState {
    pub force_update_rot_bootloader: bool,
    pub force_update_rot: bool,
    pub force_update_sp: bool,
    selected_component: UpdateComponent,
}

impl Default for ForceUpdateState {
    fn default() -> Self {
        Self {
            force_update_rot_bootloader: false,
            force_update_rot: false,
            force_update_sp: false,
            selected_component: UpdateComponent::RotBootloader,
        }
    }
}

impl ForceUpdateState {
    pub fn selected_component(&self) -> UpdateComponent {
        self.selected_component
    }

    pub fn next_component(&mut self) {
        self.selected_component = match self.selected_component {
            UpdateComponent::RotBootloader => UpdateComponent::Rot,
            UpdateComponent::Rot => UpdateComponent::Sp,
            UpdateComponent::Sp => UpdateComponent::RotBootloader,
            _ => unreachable!(),
        };
    }

    pub fn prev_component(&mut self) {
        self.selected_component = match self.selected_component {
            UpdateComponent::RotBootloader => UpdateComponent::Sp,
            UpdateComponent::Rot => UpdateComponent::RotBootloader,
            UpdateComponent::Sp => UpdateComponent::Rot,
            _ => unreachable!(),
        };
    }

    pub fn toggle(&mut self, component: UpdateComponent) {
        match component {
            UpdateComponent::RotBootloader => {
                self.force_update_rot_bootloader =
                    !self.force_update_rot_bootloader
            }
            UpdateComponent::Rot => {
                self.force_update_rot = !self.force_update_rot;
            }
            UpdateComponent::Sp => {
                self.force_update_sp = !self.force_update_sp;
            }
            UpdateComponent::Host => (),
        }
    }
}
