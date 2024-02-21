// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use ratatui::{
    layout::{Alignment, Rect},
    text::Text,
    widgets::{Block, Borders, List, Paragraph, StatefulWidget, Widget},
    Frame,
};

use super::{BoxConnector, BoxConnectorKind};

/// A displayer for the status view.
///
/// This isn't a widget itself but is a wrapper around a widget that adds:
/// * a line of status text at the top
/// * a line of help text at the bottom
/// * borders as necessary.
#[derive(Debug, Default)]
pub struct StatusView<'a, W> {
    /// Rectangle that covers the status text and the widget, but not the help
    /// text underneath.
    ///
    /// If there is no help text, this will be made larger.
    pub status_view_rect: Rect,

    /// The help text rectangle at the bottom.
    pub help_rect: Rect,

    /// An optional title to display at the top.
    pub title: String,

    /// Status text to display at the top: should be a single line of text.
    pub status_text: Text<'a>,

    /// The widget to display.
    pub widget: W,

    /// Optional help text to display at the bottom.
    pub help_text: Option<Paragraph<'a>>,

    /// Block decoration used.
    pub block: Block<'a>,
}

impl<'a, W: Widget + HasBlock<'a>> StatusView<'a, W> {
    pub fn render(self, frame: &mut Frame<'_>) {
        let status_text_rect = self.status_text_rect();
        let widget_rect = self.widget_rect();
        let widget_borders = self.widget_borders();

        // Render the status text at the top.
        let status_paragraph = Paragraph::new(self.status_text)
            .block(self.block.clone().title(self.title))
            .alignment(Alignment::Center);
        frame.render_widget(status_paragraph, status_text_rect);

        // Render the widget.
        frame.render_widget(
            self.widget.block(self.block.clone().borders(widget_borders)),
            widget_rect,
        );

        // If there's help text at the bottom, render it.
        if let Some(help_text) = self.help_text {
            frame.render_widget(
                help_text.block(self.block.clone()),
                self.help_rect,
            );
            frame.render_widget(
                BoxConnector::new(BoxConnectorKind::Both),
                widget_rect,
            );
        } else {
            frame.render_widget(
                BoxConnector::new(BoxConnectorKind::Top),
                widget_rect,
            );
        }
    }

    pub fn render_stateful(self, frame: &mut Frame<'_>, state: &mut W::State)
    where
        W: StatefulWidget,
    {
        let status_text_rect = self.status_text_rect();
        let widget_rect = self.widget_rect();
        let widget_borders = self.widget_borders();

        // Render the status text at the top.
        let status_paragraph = Paragraph::new(self.status_text)
            .block(self.block.clone().title(self.title))
            .alignment(Alignment::Center);
        frame.render_widget(status_paragraph, status_text_rect);

        // Render the widget.
        frame.render_stateful_widget(
            self.widget.block(self.block.clone().borders(widget_borders)),
            widget_rect,
            state,
        );

        // If there's help text at the bottom, render it.
        if let Some(help_text) = self.help_text {
            frame.render_widget(
                help_text.block(self.block.clone()),
                self.help_rect,
            );
            frame.render_widget(
                BoxConnector::new(BoxConnectorKind::Both),
                widget_rect,
            );
        } else {
            frame.render_widget(
                BoxConnector::new(BoxConnectorKind::Top),
                widget_rect,
            );
        }
    }

    fn status_text_rect(&self) -> Rect {
        let mut status_text_rect = self.status_view_rect;
        status_text_rect.height = 3;
        status_text_rect
    }

    fn widget_rect(&self) -> Rect {
        let mut widget_rect = self.status_view_rect;
        widget_rect.y += 3;
        if self.help_text.is_some() {
            // widget_rect.height needs to be subtracted by 3 to
            // account for the box being moved downwards (widget_rect.y
            // += 3).
            widget_rect.height = widget_rect.height.saturating_sub(3);
        } else {
            // As above, widget_rect.height needs to be subtracted by 3 to
            // account for the box being moved downwards. *However*, the height
            // needs to be incremented by 3 since there's no help text at the
            // bottom. This means that there is no change in height.
        }

        widget_rect
    }

    fn widget_borders(&self) -> Borders {
        // In both cases, the top border is formed by the bottom border of the
        // status text.
        if self.help_text.is_some() {
            // The bottom border will be formed by the top border of the help
            // text.
            Borders::LEFT | Borders::RIGHT
        } else {
            Borders::LEFT | Borders::RIGHT | Borders::BOTTOM
        }
    }
}

/// Trait that exposes `block` in a generic fashion. Implement for more widgets
/// as needed.
pub trait HasBlock<'a> {
    fn block(self, block: Block<'a>) -> Self;
}

impl<'a> HasBlock<'a> for Paragraph<'a> {
    fn block(self, block: Block<'a>) -> Self {
        self.block(block)
    }
}

impl<'a> HasBlock<'a> for List<'a> {
    fn block(self, block: Block<'a>) -> Self {
        self.block(block)
    }
}
