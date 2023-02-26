// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    wizard::{Action, Event, State},
    Frame,
};
use tui::layout::Rect;

/// A [`Control`] is the an item on a screen that can be selected and interacted with.
/// Control's render [`tui::Widget`]s when drawn.
///
///
/// Due to the rendering model of stateful widgets in `tui.rs`, `self` must
/// be mutable when `Control::draw` is called. However, global state is never
/// mutated when drawing, only visible state relevant to the Widget being
/// drawn.
///
/// To allow for distinctive styling on active/selected widgets we allow the
/// caller to indicate this via the `active` parameter.
pub trait Control {
    fn on(&mut self, state: &mut State, event: Event) -> Option<Action>;
    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: Rect,
        active: bool,
    );
}
