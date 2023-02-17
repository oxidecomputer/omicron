// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! List widget, specifically designed for use with wicket

use super::{get_control_id, Control, ControlId, HoverResult};
use crate::Point;
use std::fmt::Display;
use tui::layout::Rect;
use tui::style::Style;
use tui::text::{Span, Spans, Text};
use tui::widgets::{Block, BorderType, Borders, Paragraph, Widget};

/// An indicator is a character that represents some facet of an item in
/// a list.
///
/// For example, a list of components being updated may have a symbol which is
/// styled grey for no update, yellow for partial update, green for updated.
#[derive(Debug)]
pub struct Indicator {
    pub style: Style,
    pub symbol: &'static str,
}

/// An entry in a list along with a styled indicator
#[derive(Debug)]
pub struct ListEntry<T> {
    pub item: T,
    pub indicator: Indicator,
}

/// The state of a scrollable list of items, with an optional indicator.
///
/// The amount of items visible in the list changes depending upon the height
/// of the rect.
///
/// The list itself is structure like the following, with borders:
///
/// -------------------
/// <Indicator> item a
/// <Indicator> item b
/// <Indicator> item c
/// -------------------

#[derive(Debug)]
pub struct ListState<T> {
    control_id: ControlId,
    rect: Rect,
    items: Vec<ListEntry<T>>,

    // Offset from `visible_start`
    // We do it this way because, then if we scroll while hovering, the relative
    // position doesn't change.
    item_hovered_offset: Option<usize>,
    // If a border or whitespace is hovered, then non_item_hovered should be selected.
    // This is mutually exclusive with item_hovered.
    // TODO: This seems like a good use for `Either` to encode the mutual
    // exclusion.
    non_item_hovered: bool,
    selected: usize,

    // The first item to be shown in the visible area. `hovered` must always
    // remain visible, while selected lives in the  selection box at the top of
    // the list.
    visible_start: usize,
    mouse: Point,
}

impl<T: Display> ListState<T> {
    pub fn new() -> ListState<T> {
        ListState {
            control_id: get_control_id(),
            rect: Rect::default(),
            items: vec![],
            item_hovered_offset: None,
            non_item_hovered: false,
            selected: 0,
            visible_start: 0,
            mouse: Point { x: 0, y: 0 },
        }
    }

    pub fn init(&mut self, items: Vec<ListEntry<T>>) {
        self.items = items;
    }

    pub fn resize(&mut self, rect: Rect) {
        assert!(rect.height >= 3);
        self.rect = rect;
        if self.selected >= self.num_rows() {
            self.visible_start = self.selected - (self.num_rows() - 1);
        } else {
            self.visible_start = 0;
        }

        // Check to see if the hover offset has moved into or out of range
        // The easiest way to do this is to pretend the mouse moved and not
        // the list.
        self.on_mouse_move(self.mouse.x, self.mouse.y);
    }

    pub fn selection(&self) -> &ListEntry<T> {
        &self.items[self.selected]
    }

    pub fn selection_mut(&mut self) -> &mut ListEntry<T> {
        &mut self.items[self.selected]
    }

    pub fn items(&self) -> impl Iterator<Item = &T> {
        self.items.iter().map(|entry| &entry.item)
    }

    pub fn on_mouse_move(&mut self, x: u16, y: u16) -> HoverResult {
        self.mouse = Point { x, y };
        if self.intersects_point(x, y) {
            // Are we intersecting any selectables?
            let row = y - self.rect.y;
            let redraw = if row == 0 // top border
                // bottom border
                || row == self.rect.height -1
                // whitespace (offsets are 0 based)
                || row as usize >= self.items.len()
            {
                // We're on a border or in whitespace before the bottom border
                if self.non_item_hovered {
                    // No change
                    false
                } else {
                    self.item_hovered_offset = None;
                    self.non_item_hovered = true;
                    true
                }
            } else {
                //  offsets are zero based
                let offset = Some(row as usize - 1);
                if offset == self.item_hovered_offset {
                    // No change
                    false
                } else {
                    self.non_item_hovered = false;
                    self.item_hovered_offset = offset;
                    true
                }
            };
            HoverResult { redraw, hovered: true }
        } else {
            let redraw = if self.item_hovered_offset.is_none()
                && !self.non_item_hovered
            {
                false
            } else {
                self.item_hovered_offset = None;
                self.non_item_hovered = false;
                true
            };
            HoverResult { redraw, hovered: false }
        }
    }

    /// Return true if the selection changed, false otherwise
    pub fn on_mouse_click(&mut self) -> bool {
        match self.item_hovered_offset {
            Some(offset) => {
                let idx = self.visible_start + offset;
                if idx == self.selected {
                    false
                } else {
                    self.selected = idx;
                    true
                }
            }
            None => false,
        }
    }

    pub fn scroll_down(&mut self) {
        if self.selected == self.items.len() - 1 {
            self.selected = 0;
            self.visible_start = 0;
        } else {
            self.selected += 1;
            if self.selected - self.visible_start == self.num_rows() {
                self.visible_start += 1;
            }
        }
    }

    pub fn scroll_up(&mut self) {
        if self.selected == 0 {
            self.selected = self.items.len() - 1;
            // self.num_rows() can be greater than self.items.len()
            self.visible_start =
                self.selected.saturating_sub(self.num_rows() - 1);
        } else {
            self.selected -= 1;
            if self.selected < self.visible_start {
                self.visible_start -= 1;
            }
        }
    }

    fn num_rows(&self) -> usize {
        // account for borders
        (self.rect.height - 2) as usize
    }
}

impl<T> Control for ListState<T> {
    fn id(&self) -> ControlId {
        self.control_id
    }

    fn rect(&self) -> Rect {
        self.rect
    }
}

pub struct List<'a, T> {
    pub state: &'a ListState<T>,
    pub style: Style,
    pub hover_style: Style,
    pub selection_style: Style,
    pub border_style: Style,
}

impl<'a, T: Display> Widget for List<'a, T> {
    fn render(self, _: Rect, buf: &mut tui::buffer::Buffer) {
        let selected = self.state.selected;
        let hovered = self
            .state
            .item_hovered_offset
            .map(|offset| self.state.visible_start + offset);

        let lines: Vec<_> = self
            .state
            .items
            .iter()
            .enumerate()
            .skip(self.state.visible_start)
            .take(self.state.num_rows())
            .map(|(index, entry)| {
                let mut spans = vec![Span::styled(
                    entry.indicator.symbol,
                    entry.indicator.style,
                )];
                if index == selected {
                    spans.push(Span::styled(
                        entry.item.to_string(),
                        self.selection_style,
                    ));
                } else if Some(index) == hovered {
                    spans.push(Span::styled(
                        entry.item.to_string(),
                        self.hover_style,
                    ));
                } else {
                    spans
                        .push(Span::styled(entry.item.to_string(), self.style));
                }
                Spans::from(spans)
            })
            .collect();
        let text = Text::from(lines);
        let list = Paragraph::new(text).block(
            Block::default()
                .style(self.border_style)
                .borders(Borders::ALL)
                .border_type(BorderType::Plain),
        );
        list.render(self.state.rect, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const MAX_ITEMS: u16 = 36;

    // One item row, and top and bottom borders
    const MIN_LIST_HEIGHT: u16 = 3;
    const MAX_LIST_HEIGHT: u16 = 80;

    // We just fix the list width, as it's not important to test, since no
    // scrolling  occurs horizontally.
    const LIST_WIDTH: u16 = 14;

    // Fix the position of the list for simplicity
    const LIST_X: u16 = 8;
    const LIST_Y: u16 = 10;

    // Bounding box only matters for mouse movements
    // We detect when the mouse is inside or outside the list.
    const BOUNDING_BOX_HEIGHT: u16 = 100;
    const BOUNDING_BOX_WIDTH: u16 = 30;

    #[derive(Debug, Clone)]
    pub enum UserAction {
        ScrollUp(u16),
        ScrollDown(u16),
        MouseMovement(u16, u16),
        MouseClick,
        // The height is the only thing that matters. We fix the other
        // parameters for simplicity.
        Resize(u16),
    }

    // Create an arbitrary `UserAction` using proptest generation
    fn arb_user_action() -> impl Strategy<Value = UserAction> {
        prop_oneof![
            // We want to allow wrap-around
            (1..MAX_LIST_HEIGHT).prop_map(UserAction::ScrollUp),
            (1..MAX_LIST_HEIGHT).prop_map(UserAction::ScrollDown),
            ((0..BOUNDING_BOX_WIDTH - 1), (0..BOUNDING_BOX_HEIGHT - 1))
                .prop_map(|(x, y)| UserAction::MouseMovement(x, y)),
            Just(UserAction::MouseClick),
            (MIN_LIST_HEIGHT..MAX_LIST_HEIGHT).prop_map(UserAction::Resize)
        ]
    }

    // Validate the invariants of the list
    fn prop_invariants(state: &ListState<u16>) -> Result<(), TestCaseError> {
        let visible_range =
            state.visible_start..state.visible_start + state.num_rows();
        prop_assert!(visible_range.contains(&state.selected));

        // We can only be hovering over a non-item or an item, not both
        if state.item_hovered_offset.is_some() {
            prop_assert!(!state.non_item_hovered);
        }
        Ok(())
    }

    fn rect(height: u16) -> Rect {
        Rect { x: LIST_X, y: LIST_Y, width: LIST_WIDTH, height }
    }

    proptest! {
        /// We always start in the initial state, then perform a bunch of list
        /// operations simulating user interaction and ensure that invariants
        /// hold.
        #[test]
        fn test_list_operations(
            actions in prop::collection::vec(arb_user_action(), 1..100),
            initial_height in 3..MAX_LIST_HEIGHT)
        {
            let mut state = ListState::<u16>::new();
            // Init and resize are pre-requisites to start the test
            state.init(
                (0..MAX_ITEMS)
                    .map(|item| ListEntry {
                        item,
                        indicator: Indicator {
                            style: Style::default(),
                            symbol: ". ",
                        },
                    })
                    .collect(),
            );
            state.resize(rect(initial_height));

            for action in actions {
                match action {
                    UserAction::ScrollUp(i) => {
                        for _ in 0..i {
                            state.scroll_up();
                            prop_invariants(&state)?;
                        }
                    }
                    UserAction::ScrollDown(i) => {
                        for _ in 0..i {
                            state.scroll_down();
                            prop_invariants(&state)?;
                        }
                    }
                    UserAction::MouseMovement(x, y) => {
                        let _ = state.on_mouse_move(x, y);
                        prop_invariants(&state)?;
                    }
                    UserAction::MouseClick => {
                        let _ = state.on_mouse_click();
                        prop_invariants(&state)?;

                        // After a mouse click, if the mouse is hovered within
                        // bounds then `state.selected` should match the item
                        // that was hovered over.
                        if let Some(offset) = state.item_hovered_offset {
                            let item = state.visible_start + offset;
                            prop_assert_eq!(item, state.selected);
                        }
                    }
                    UserAction::Resize(height) => {
                        state.resize(rect(height));
                        prop_invariants(&state)?;
                    }
                }
            }
        }
    }
}
