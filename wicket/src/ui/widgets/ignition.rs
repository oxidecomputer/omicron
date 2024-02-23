// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A popup dialog box widget for ignition control

use super::ButtonText;
use super::PopupBuilder;
use crate::state::ComponentId;
use crate::ui::defaults::style;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::text::Text;
use wicketd_client::types::IgnitionCommand;

pub struct IgnitionPopup {
    selected_command: IgnitionCommand,
}

impl Default for IgnitionPopup {
    fn default() -> Self {
        Self { selected_command: IgnitionCommand::PowerOn }
    }
}

impl IgnitionPopup {
    pub fn selected_command(&self) -> IgnitionCommand {
        self.selected_command
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    pub fn key_up(&mut self) {
        self.selected_command = match self.selected_command {
            IgnitionCommand::PowerOn => IgnitionCommand::PowerReset,
            IgnitionCommand::PowerOff => IgnitionCommand::PowerOn,
            IgnitionCommand::PowerReset => IgnitionCommand::PowerOff,
        };
    }

    pub fn key_down(&mut self) {
        self.selected_command = match self.selected_command {
            IgnitionCommand::PowerOn => IgnitionCommand::PowerOff,
            IgnitionCommand::PowerOff => IgnitionCommand::PowerReset,
            IgnitionCommand::PowerReset => IgnitionCommand::PowerOn,
        };
    }

    /// Return the `PopupBuilder` for this popup -- the header, body and button
    /// text.
    ///
    /// Can't return a `Popup` here due to lifetime issues.
    pub fn to_popup_builder(
        &self,
        component: ComponentId,
    ) -> PopupBuilder<'static> {
        PopupBuilder {
            header: Line::from(vec![Span::styled(
                format!("IGNITION: {}", component.to_string_uppercase()),
                style::header(true),
            )]),
            body: Text::from(vec![
                Line::from(vec![Span::styled(
                    "Power On",
                    style::line(
                        self.selected_command == IgnitionCommand::PowerOn,
                    ),
                )]),
                Line::from(vec![Span::styled(
                    "Power Off",
                    style::line(
                        self.selected_command == IgnitionCommand::PowerOff,
                    ),
                )]),
                Line::from(vec![Span::styled(
                    "Power Reset",
                    style::line(
                        self.selected_command == IgnitionCommand::PowerReset,
                    ),
                )]),
            ]),
            buttons: vec![ButtonText::new("Close", "Esc")],
        }
    }
}
