// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A Modal for displaying rack inventory

use crate::inventory::{Component, ComponentId, Inventory};
use slog::Logger;
use std::marker::PhantomData;
use tui::buffer::Buffer;
use tui::layout::Alignment;
use tui::layout::Rect;
use tui::style::{Modifier, Style};
use tui::text::{Span, Spans, Text};
use tui::widgets::Block;
use tui::widgets::Paragraph;
use tui::widgets::StatefulWidget;
use tui::widgets::Widget;

pub struct ComponentModalState<'a> {
    pub log: Logger,
    pub prev: ComponentId,
    pub next: ComponentId,
    pub current: ComponentId,
    pub current_component: Option<Component>,
    pub inventory: &'a Inventory,
}

#[derive(Debug, Clone, Default)]
pub struct ComponentModal<'a> {
    style: Style,
    status_bar_style: Style,
    status_bar_selected_style: Style,
    inventory_style: Style,
    phantom: PhantomData<&'a usize>,
}

impl<'a> ComponentModal<'a> {
    pub fn style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }

    pub fn status_bar_style(mut self, style: Style) -> Self {
        self.status_bar_style = style;
        self
    }

    pub fn status_bar_selected_style(mut self, style: Style) -> Self {
        self.status_bar_selected_style = style;
        self
    }

    pub fn inventory_style(mut self, style: Style) -> Self {
        self.inventory_style = style;
        self
    }
}

impl<'a> ComponentModal<'a> {
    fn draw_status_bar(
        &self,
        area: Rect,
        buf: &mut Buffer,
        state: &mut ComponentModalState,
    ) {
        let mut rect = area.clone();
        rect.height = 5;

        let status_bar_block = Block::default().style(self.status_bar_style);

        status_bar_block.render(rect, buf);

        // Draw the components list
        // TODO: Some sliding style animation?
        let title = Spans::from(vec![
            Span::styled(state.prev.name(), self.status_bar_style),
            Span::raw("   "),
            Span::styled(state.current.name(), self.status_bar_selected_style),
            Span::raw("   "),
            Span::styled(state.next.name(), self.status_bar_style),
        ]);

        let mut rect = area.clone();
        rect.height = 1;
        rect.y = area.y + 1;
        let title_block = Block::default()
            .style(self.status_bar_style)
            .title(title)
            .title_alignment(Alignment::Center);
        title_block.render(rect, buf);

        // Draw the power state
        let title = match state.inventory.get_power_state(&state.current) {
            Some(s) => {
                format!(
                    "⌁ Power State: {}",
                    s.description().to_ascii_uppercase()
                )
            }
            None => "⌁ Power State: UNKNOWN".to_string(),
        };

        let mut rect = area.clone();
        rect.height = 1;
        rect.y = area.y + 3;
        let power_state_block = Block::default()
            .style(self.status_bar_selected_style)
            .title(title)
            .title_alignment(Alignment::Center);

        power_state_block.render(rect, buf);
    }

    fn draw_background(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::default().style(self.style);
        clear_buf(area.clone(), buf, self.style);
        block.render(area, buf);
    }

    fn draw_inventory(
        &self,
        mut area: Rect,
        buf: &mut Buffer,
        state: &mut ComponentModalState,
    ) {
        // Draw the header
        let mut header_style = self.status_bar_selected_style;
        header_style =
            header_style.add_modifier(Modifier::UNDERLINED | Modifier::BOLD);

        let text = Text::styled("INVENTORY\n\n", header_style);
        let mut rect = area.clone();
        rect.y = area.y + 6;
        rect.height = area.height - 6;
        let center = (area.width - text.width() as u16) / 2;
        rect.x = area.x + center;
        rect.width = area.width - center;
        let header = Paragraph::new(text);
        header.render(rect, buf);

        // Draw the contents
        let text = match state.inventory.get_inventory(&state.current) {
            Some(inventory) => {
                Text::styled(format!("{:#?}", inventory), self.inventory_style)
            }
            None => Text::styled("UNKNOWN", self.inventory_style),
        };

        area.y = area.y + 9;
        area.height = area.height - 9;

        let center = (area.width - text.width() as u16) / 2;
        area.x = area.x + center;
        area.width = area.width - center;

        let inventory = Paragraph::new(text);

        inventory.render(area, buf);
    }
}

impl<'a> StatefulWidget for ComponentModal<'a> {
    type State = ComponentModalState<'a>;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        self.draw_background(area, buf);
        self.draw_status_bar(area, buf, state);
        self.draw_inventory(area, buf, state);
    }
}

// Set the buf area to the bg color
fn clear_buf(area: Rect, buf: &mut Buffer, style: Style) {
    for x in area.left()..area.right() {
        for y in area.top()..area.bottom() {
            buf.get_mut(x, y).set_style(style).set_symbol(" ");
        }
    }
}
