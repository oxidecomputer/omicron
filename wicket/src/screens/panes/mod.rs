// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod overview_pane;

pub use super::{get_control_id, Control, ControlId};
pub use overview_pane::OverviewPane;

/// A specific functionality such as `Update` or `Help` that is selectable
/// from the [`MainScreen`] navbar on the left.
pub trait Pane: Control {
    /// Return the tab names to be shown in the top bar of [`MainScreen`]
    fn tabs(&self) -> &[&'static str];

    /// Return the index of the selected tab
    fn selected_tab(&self) -> usize;
}

/// A placeholder pane used for development purposes
pub struct NullPane {
    control_id: ControlId,
}

impl NullPane {
    pub fn new() -> NullPane {
        NullPane { control_id: get_control_id() }
    }
}

impl Pane for NullPane {
    fn tabs(&self) -> &[&'static str] {
        &["NULL"]
    }

    fn selected_tab(&self) -> usize {
        // There's only one tab
        0
    }
}

impl Control for NullPane {
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
