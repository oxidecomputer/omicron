// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::help_text;
use super::ComputedScrollOffset;
use crate::ui::defaults::colors::OX_OFF_WHITE;
use crate::ui::defaults::style;
use crate::ui::widgets::BoxConnector;
use crate::ui::widgets::BoxConnectorKind;
use crate::Action;
use crate::Cmd;
use crate::Control;
use crate::State;
use tui::layout::Constraint;
use tui::layout::Direction;
use tui::layout::Layout;
use tui::style::Style;
use tui::text::Span;
use tui::text::Spans;
use tui::text::Text;
use tui::widgets::Block;
use tui::widgets::BorderType;
use tui::widgets::Borders;
use tui::widgets::Paragraph;

/// `RackSetupPane` shows the current rack setup configuration and allows
/// triggering the rack setup process.
///
/// Currently, it does not allow input: configuration must be provided
/// externally by running `wicket setup ...`. This should change in the future.
pub struct RackSetupPane {
    help: Vec<(&'static str, &'static str)>,
    scroll_offset: usize,
}

impl Default for RackSetupPane {
    fn default() -> Self {
        Self { help: vec![("Scroll", "<UP/DOWN>")], scroll_offset: 0 }
    }
}

impl Control for RackSetupPane {
    fn on(&mut self, _state: &mut State, cmd: Cmd) -> Option<Action> {
        match cmd {
            Cmd::Up => {
                self.scroll_offset = self.scroll_offset.saturating_sub(1);
                Some(Action::Redraw)
            }
            Cmd::Down => {
                self.scroll_offset += 1;
                Some(Action::Redraw)
            }
            _ => None,
        }
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut crate::Frame<'_>,
        rect: tui::layout::Rect,
        active: bool,
    ) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    Constraint::Length(3),
                    Constraint::Min(0),
                    Constraint::Length(3),
                ]
                .as_ref(),
            )
            .split(rect);

        let border_style =
            if active { style::selected_line() } else { style::deselected() };

        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .style(border_style);

        // Draw the screen title (subview look)
        let title_bar = Paragraph::new(Spans::from(vec![Span::styled(
            "Oxide Rack Setup",
            border_style,
        )]))
        .block(block.clone());
        frame.render_widget(title_bar, chunks[0]);

        // Draw the contents
        let contents_block = block
            .clone()
            .borders(Borders::LEFT | Borders::RIGHT | Borders::TOP);
        let contents_style = Style::default().fg(OX_OFF_WHITE);
        let text = match state.rss_config.as_ref() {
            Some(config) => {
                Text::styled(format!("{config:#?}"), contents_style)
            }
            None => Text::styled("Rack Setup Unavailable", contents_style),
        };

        let y_offset = ComputedScrollOffset::new(
            self.scroll_offset,
            text.height(),
            chunks[1].height as usize,
        )
        .into_offset();
        self.scroll_offset = y_offset as usize;

        let inventory = Paragraph::new(text)
            .block(contents_block.clone())
            .scroll((y_offset, 0));
        frame.render_widget(inventory, chunks[1]);

        // Draw the help bar
        let help = help_text(&self.help).block(block.clone());
        frame.render_widget(help, chunks[2]);

        // Make sure the top and bottom bars connect
        frame.render_widget(
            BoxConnector::new(BoxConnectorKind::Bottom),
            chunks[1],
        );
    }
}
