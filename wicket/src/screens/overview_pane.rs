// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{get_control_id, Control, ControlId, Pane};

/// The OverviewPane shows a rendering of the rack.
///
/// This is useful for getting a quick view of the state of the rack.
pub struct OverviewPane {
    control_id: ControlId,
}

impl OverviewPane {
    pub fn new() -> OverviewPane {
        OverviewPane { control_id: get_control_id() }
    }
}

impl Pane for OverviewPane {
    fn tabs(&self) -> &[&'static str] {
        &["Oxide Rack"]
    }

    fn selected_tab(&self) -> usize {
        // There's only one tab
        0
    }
}

impl Control for OverviewPane {
    fn control_id(&self) -> ControlId {
        self.control_id
    }

    fn on(
        &mut self,
        state: &mut crate::State,
        event: crate::Event,
    ) -> Option<crate::Action> {
        None
    }

    fn draw(
        &mut self,
        state: &crate::State,
        frame: &mut crate::Frame<'_>,
        rect: tui::layout::Rect,
    ) {
    }
}
