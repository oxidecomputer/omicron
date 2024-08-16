// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{Action, Cmd, State};
use ratatui::{layout::Rect, Frame};

/// A [`Control`] is the an item on a screen that can be selected and interacted with.
/// Control's render [`ratatui::widgets::Widget`]s when drawn.
///
///
/// Due to the rendering model of stateful widgets in `tui.rs`, `self` must
/// be mutable when `Control::draw` is called. However, global state is never
/// mutated when drawing, only visible state relevant to the Widget being
/// drawn.
///
/// To allow for distinctive styling on active/selected widgets we allow the
/// caller to indicate this via the `active` parameter.
pub trait Control: Send {
    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action>;
    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: Rect,
        active: bool,
    );

    /// Optional callback for [`Control`]s that must know the size and position
    /// of their area in order to compute visuals dynamically outside the
    /// render process. This is useful for situations when you want to size
    /// a widget to the the screen where the content of the widget may differ
    /// depending upon the size available, and you want to minimize the amount
    /// of times this computation is performed by memoizing the contents of the
    /// widget in the `Control`. It is also useful to allow mouse input event
    /// handling via rectangle intersection.
    ///
    /// This is a separate callback because not all [`Control`]s need the
    /// functionality provided.
    ///
    /// Additionally, the resize events from the terminal are insufficient, as
    /// they only give the total size of the screen, not the specific area of a
    /// `Control`. Only the parent of a [`Control`] knows the precise [`Rect`]
    /// of the child. The parent computes its own layout during resize and
    /// passes the appropriate `Rect` to the child.
    fn resize(&mut self, _: &mut State, _: Rect) {}

    /// Some [`Control`]s can launch modal popups.
    ///
    /// Return false by default so parent behavior does not change.
    ///
    /// If a control has a modal active, its parent should not switch away from
    /// it. For example, in `crate::ui::MainScreen`, the escape key should not allow
    /// transferring control from a pane's modal to the side bar.
    fn is_modal_active(&self) -> bool {
        false
    }
}
