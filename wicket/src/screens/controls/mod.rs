// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    wizard::{Action, Event, State, Term},
    Frame,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tui::layout::Rect;

/// A unique id for a [`Control`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ControlId(pub usize);

/// Return a unique id for a [`Control`]
pub fn get_control_id() -> ControlId {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    ControlId(COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// A [`Control`] is the an item on a screen that can be selected and interacted with.
/// Control's render [`tui::Widget`]s when drawn.
///
///
/// Due to the rendering model of stateful widgets in `tui.rs`, `self` must
/// be mutable when `Control::draw` is called. However, global state is never
/// mutated when drawing, only visible state relevant to the Widget being
/// drawn.
pub trait Control {
    fn control_id(&self) -> ControlId;
    fn on(&mut self, state: &mut State, event: Event) -> Option<Action>;
    fn draw(&mut self, state: &State, frame: &mut Frame<'_>, rect: Rect);
}
