// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod overview_pane;

pub use super::Control;
pub use overview_pane::OverviewPane;

pub struct Tab {
    pub title: &'static str,
    pub control: Box<dyn Control>,
}

/// A specific functionality such as `Update` or `Help` that is selectable
/// from the [`MainScreen`] navbar on the left.
pub trait Pane: Control {
    /// Return the tab names to be shown in the top bar of [`MainScreen`]
    fn tab_titles(&self) -> Vec<&'static str>;

    /// Return the index of the selected tab
    fn selected_tab(&self) -> usize;
}

/// A placeholder pane used for development purposes
#[derive(Default)]
pub struct NullPane {}

impl NullPane {
    pub fn new() -> NullPane {
        NullPane::default()
    }
}

impl Pane for NullPane {
    fn tab_titles(&self) -> Vec<&'static str> {
        vec!["NULL"]
    }

    fn selected_tab(&self) -> usize {
        // There's only one tab
        0
    }
}

impl Control for NullPane {
    fn on(
        &mut self,
        _: &mut crate::State,
        _: crate::Event,
    ) -> Option<crate::Action> {
        None
    }

    fn draw(
        &mut self,
        _: &crate::State,
        _: &mut crate::Frame<'_>,
        _: tui::layout::Rect,
        _active: bool,
    ) {
    }
}
