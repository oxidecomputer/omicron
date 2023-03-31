// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::Control;
use crate::{Action, Cmd, Frame, State};
use tui::layout::Rect;
use tui::text::Spans;

/// An item in a menu
pub trait MenuItem: Control {
    /// Return the line to be shown whether the item has expanded contents or not
    ///
    /// We pass in whether the item is selected so that it may be rendered
    /// differently.
    fn title(&self, selected: bool) -> Spans<'_>;

    /// If the menu item contains contents then return a `Rect` of the contents
    /// given a bounding box that the contents must be drawn within.
    fn expanded_size(&self, bounding_box: Rect) -> Option<Rect>;

    /// Can the item be "entered", so that all Cmds go directly to the item,
    /// and will not manipualte the list of items until "exited".
    fn is_enterable(&self) -> bool;
}

/// A scrollable menu with collapsible contents
pub struct Menu {
    // The bounding box available for the menu
    rect: Rect,
    // All items in the menu
    items: Vec<Box<dyn MenuItem>>,
    // The index of the selected item
    selected: usize,
    // Whether the selected item has expanded contents
    expanded: bool,
    // The currently selected entry has been entered.
    // An entry can only be entered if it is expanded and enterable.
    // If an entry has not been expanded, it will automatically be expanded
    // when entered.
    entered: bool,
}

impl Menu {
    pub fn new(items: Vec<Box<dyn MenuItem>>) -> Menu {
        Menu {
            items,
            rect: Rect::default(),
            selected: 0,
            expanded: false,
            entered: false,
        }
    }

    /// Return the currently selected item if there is one
    fn selected_item(&mut self) -> &mut Box<dyn MenuItem> {
        self.items.get_mut(self.selected).unwrap()
    }
}

impl Control for Menu {
    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        if self.entered {
            // Forward all commands, except `Exit` to the selected item
            if cmd == Cmd::Exit {
                self.entered = false;
                return Some(Action::Redraw);
            }
            return self.selected_item().on(state, cmd);
        }

        match cmd {
            Cmd::Up => {
                if self.selected == 0 {
                    self.selected = self.items.len() - 1;
                } else {
                    self.selected -= 1;
                }
                Some(Action::Redraw)
            }
            Cmd::Down => {
                self.selected = (self.selected + 1) % self.items.len();
                Some(Action::Redraw)
            }
            Cmd::Collapse | Cmd::Left => {
                self.expanded = false;
                Some(Action::Redraw)
            }
            Cmd::Expand | Cmd::Right => {
                self.expanded = true;
                Some(Action::Redraw)
            }
            Cmd::Enter => {
                if self.selected_item().is_enterable() {
                    self.entered = true;
                    self.expanded = true;
                    Some(Action::Redraw)
                } else {
                    self.selected_item().on(state, Cmd::Enter)
                }
            }

            c => self.selected_item().on(state, c),
        }
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: Rect,
        active: bool,
    ) {
    }
}
