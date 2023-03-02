// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{align_by, help_text, Control};
use crate::state::{ComponentId, ALL_COMPONENT_IDS};
use crate::ui::defaults::style;
use crate::ui::widgets::{BoxConnector, BoxConnectorKind};
use crate::{Action, Event, Frame, State};
use crossterm::event::Event as TermEvent;
use crossterm::event::KeyCode;
use tui::layout::{Constraint, Direction, Layout, Rect};
use tui::style::Style;
use tui::text::{Span, Spans, Text};
use tui::widgets::{Block, BorderType, Borders, Paragraph};
use tui_tree_widget::{Tree, TreeItem, TreeState};

/// Overview of update status and ability to install updates
/// from a single TUF repo uploaded to wicketd via wicket.
pub struct UpdatePane {
    tree_state: TreeState,
    items: Vec<TreeItem<'static>>,
    help: Vec<(&'static str, &'static str)>,
}

impl UpdatePane {
    pub fn new() -> UpdatePane {
        UpdatePane {
            tree_state: Default::default(),
            items: ALL_COMPONENT_IDS
                .iter()
                .map(|id| TreeItem::new(*id, vec![]))
                .collect(),
            help: vec![
                ("OPEN", "<RIGHT>"),
                ("CLOSE", "<LEFT>"),
                ("SELECT", "<UP/DOWN>"),
            ],
        }
    }
}

impl Control for UpdatePane {
    fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        match event {
            Event::Term(TermEvent::Key(e)) => match e.code {
                KeyCode::Up => {
                    // Keep the rack selection in sync across panes
                    state.rack_state.prev();
                    self.tree_state.key_up(&self.items);
                    Some(Action::Redraw)
                }
                KeyCode::Down => {
                    // Keep the rack selection in sync across panes
                    state.rack_state.next();
                    self.tree_state.key_down(&self.items);
                    Some(Action::Redraw)
                }
                KeyCode::Left => {
                    self.tree_state.key_left();
                    Some(Action::Redraw)
                }
                KeyCode::Right => {
                    self.tree_state.key_right();
                    Some(Action::Redraw)
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: Rect,
        active: bool,
    ) {
        let (border_style, component_style) = if active {
            (style::selected_line(), style::selected())
        } else {
            (style::deselected(), style::deselected())
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    Constraint::Length(3),
                    Constraint::Length(3),
                    Constraint::Min(0),
                    Constraint::Length(3),
                ]
                .as_ref(),
            )
            .split(rect);

        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .style(border_style);

        // Draw the title/tab bar
        let title_bar = Paragraph::new(Spans::from(vec![Span::styled(
            "UPDATE STATUS",
            style::selected(),
        )]))
        .block(block.clone().title("<ENTER>"));
        frame.render_widget(title_bar, chunks[0]);

        // Draw the table headers
        let mut line_rect = chunks[1];
        line_rect.x += 2;
        line_rect.width -= 2;
        let headers = Paragraph::new(align_by(
            2,
            25,
            line_rect,
            vec![
                Span::styled("COMPONENT", style::selected()),
                Span::styled("VERSION", style::selected()),
                Span::styled("TARGET", style::selected()),
                Span::styled("STATUS", style::selected()),
            ],
        ))
        .block(block.clone());
        frame.render_widget(headers, chunks[1]);

        // Populate the contents
        // TODO: Put this in a function and use real data
        let items: Vec<_> = ALL_COMPONENT_IDS
            .iter()
            .map(|id| TreeItem::new(*id, vec![]))
            .collect();

        // Draw the contents
        let tree = Tree::new(items)
            .block(block.clone().borders(Borders::LEFT | Borders::RIGHT))
            .style(style::plain_text())
            .highlight_style(style::highlighted());
        frame.render_stateful_widget(tree, chunks[2], &mut self.tree_state);

        // Draw the help bar
        let help = help_text(&self.help).block(block.clone());
        frame.render_widget(help, chunks[3]);

        // Ensure the contents is connected to the table headers and help bar
        frame.render_widget(
            BoxConnector::new(BoxConnectorKind::Both),
            chunks[2],
        );
    }
}
