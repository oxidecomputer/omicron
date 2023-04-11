// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A popup dialog box widget for ignition control

use super::ButtonText;
use super::Popup;
use crate::state::ComponentId;
use crate::ui::defaults::style;
use tui::text::Span;
use tui::text::Spans;
use tui::text::Text;
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

    pub fn popup(&self, component: ComponentId) -> Popup<'static> {
        Popup {
            header: Text::from(vec![Spans::from(vec![Span::styled(
                format!(" IGNITION: {}", component),
                style::header(true),
            )])]),
            body: Text {
                lines: vec![
                    Spans::from(vec![Span::styled(
                        "Power On",
                        style::line(
                            self.selected_command == IgnitionCommand::PowerOn,
                        ),
                    )]),
                    Spans::from(vec![Span::styled(
                        "Power Off",
                        style::line(
                            self.selected_command == IgnitionCommand::PowerOff,
                        ),
                    )]),
                    Spans::from(vec![Span::styled(
                        "Power Reset",
                        style::line(
                            self.selected_command
                                == IgnitionCommand::PowerReset,
                        ),
                    )]),
                ],
            },
            buttons: vec![ButtonText { instruction: "CLOSE", key: "ESC" }],
        }
    }
}
