// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::help_text;
use super::push_text_lines;
use super::ComputedScrollOffset;
use crate::keymap::ShowPopupCmd;
use crate::state::RackSetupState;
use crate::ui::defaults::colors;
use crate::ui::defaults::style;
use crate::ui::widgets::BoxConnector;
use crate::ui::widgets::BoxConnectorKind;
use crate::ui::widgets::ButtonText;
use crate::ui::widgets::PopupBuilder;
use crate::ui::widgets::PopupScrollKind;
use crate::Action;
use crate::Cmd;
use crate::Control;
use crate::Frame;
use crate::State;
use std::borrow::Cow;
use tui::layout::Constraint;
use tui::layout::Direction;
use tui::layout::Layout;
use tui::layout::Rect;
use tui::style::Style;
use tui::text::Span;
use tui::text::Spans;
use tui::text::Text;
use tui::widgets::Block;
use tui::widgets::BorderType;
use tui::widgets::Borders;
use tui::widgets::Paragraph;
use wicketd_client::types::Baseboard;
use wicketd_client::types::CurrentRssUserConfig;
use wicketd_client::types::IpRange;

#[derive(Debug)]
enum Popup {
    RackSetup(PopupKind),
    RackReset(PopupKind),
}

impl Popup {
    fn kind_mut(&mut self) -> &mut PopupKind {
        match self {
            Self::RackSetup(kind) | Self::RackReset(kind) => kind,
        }
    }
}

#[derive(Debug)]
enum PopupKind {
    Prompting,
    Waiting,
    Failed { message: String, scroll_kind: PopupScrollKind },
}

impl PopupKind {
    fn is_scrollable(&self) -> bool {
        match self {
            PopupKind::Prompting | PopupKind::Waiting => false,
            PopupKind::Failed { .. } => true,
        }
    }

    fn scroll_up(&mut self) {
        match self {
            PopupKind::Prompting | PopupKind::Waiting => (),
            PopupKind::Failed { scroll_kind, .. } => scroll_kind.scroll_up(),
        }
    }

    fn scroll_down(&mut self) {
        match self {
            PopupKind::Prompting | PopupKind::Waiting => (),
            PopupKind::Failed { scroll_kind, .. } => scroll_kind.scroll_down(),
        }
    }
}

/// `RackSetupPane` shows the current rack setup configuration and allows
/// triggering the rack setup process.
///
/// Currently, it does not allow input: configuration must be provided
/// externally by running `wicket setup ...`. This should change in the future.
pub struct RackSetupPane {
    help: Vec<(&'static str, &'static str)>,
    rack_uninitialized_help: Vec<(&'static str, &'static str)>,
    rack_initialized_help: Vec<(&'static str, &'static str)>,
    scroll_offset: usize,

    popup: Option<Popup>,
}

impl Default for RackSetupPane {
    fn default() -> Self {
        Self {
            help: vec![("Scroll", "<UP/DOWN>")],
            rack_uninitialized_help: vec![
                ("Scoll", "<UP/DOWN>"),
                ("Start Rack Setup", "<Ctrl-Alt-S>"),
            ],
            rack_initialized_help: vec![
                ("Scoll", "<UP/DOWN>"),
                ("Start Rack Reset", "<Ctrl-Alt-R>"),
            ],
            scroll_offset: 0,
            popup: None,
        }
    }
}

impl RackSetupPane {
    fn handle_cmd_in_popup(
        &mut self,
        _state: &mut State,
        cmd: Cmd,
    ) -> Option<Action> {
        let popup = self.popup.as_mut().unwrap();

        // Handle up or down commands here.
        let kind = popup.kind_mut();
        if kind.is_scrollable() {
            match cmd {
                Cmd::Up => {
                    kind.scroll_up();
                    return Some(Action::Redraw);
                }
                Cmd::Down => {
                    kind.scroll_down();
                    return Some(Action::Redraw);
                }
                _ => {}
            }
        }

        match (kind, cmd) {
            (PopupKind::Prompting, Cmd::No)
            | (PopupKind::Failed { .. }, Cmd::Exit) => {
                self.popup = None;
                Some(Action::Redraw)
            }
            (PopupKind::Prompting, Cmd::Yes) => {
                // TODO: actually trigger RSS or reset
                *popup.kind_mut() = PopupKind::Waiting;
                Some(Action::Redraw)
            }
            (
                PopupKind::Waiting,
                Cmd::ShowPopup(ShowPopupCmd::StartRackSetupResponse(response)),
            ) => {
                // We should be blocked in `Waiting` until this shows up, so we
                // should only be able to see this command if we're in the "rack
                // setup" popup. Confirm that.
                assert!(matches!(popup, Popup::RackSetup(_)));
                match response {
                    Ok(()) => {
                        self.popup = None;
                    }
                    Err(message) => {
                        *popup.kind_mut() = PopupKind::Failed {
                            message,
                            scroll_kind: PopupScrollKind::Enabled { offset: 0 },
                        };
                    }
                }
                Some(Action::Redraw)
            }
            (
                PopupKind::Waiting,
                Cmd::ShowPopup(ShowPopupCmd::StartRackResetResponse(response)),
            ) => {
                // We should be blocked in `Waiting` until this shows up, so we
                // should only be able to see this command if we're in the "rack
                // reset" popup. Confirm that.
                assert!(matches!(popup, Popup::RackReset(_)));
                match response {
                    Ok(()) => {
                        self.popup = None;
                    }
                    Err(message) => {
                        *popup.kind_mut() = PopupKind::Failed {
                            message,
                            scroll_kind: PopupScrollKind::Enabled { offset: 0 },
                        };
                    }
                }
                Some(Action::Redraw)
            }
            _ => None,
        }
    }
}

fn draw_rack_setup_popup(
    state: &State,
    frame: &mut Frame<'_>,
    kind: &mut PopupKind,
) {
    let header;
    let body;
    let buttons;
    let current_scroll_kind;

    match kind {
        PopupKind::Prompting => {
            header = Spans::from(vec![Span::styled(
                "Start Rack Setup",
                style::header(true),
            )]);
            body = Text::from(vec![Spans::from(vec![Span::styled(
                "Would you like to begin rack setup?",
                style::plain_text(),
            )])]);
            buttons =
                vec![ButtonText::new("Yes", "Y"), ButtonText::new("No", "N")];
            current_scroll_kind = None;
        }
        PopupKind::Waiting => {
            header = Spans::from(vec![Span::styled(
                "Start Rack Setup",
                style::header(true),
            )]);
            body = Text::from(vec![Spans::from(vec![Span::styled(
                "Waiting for rack setup to start",
                style::plain_text(),
            )])]);
            buttons = vec![];
            current_scroll_kind = None;
        }
        PopupKind::Failed { message, scroll_kind } => {
            header = Spans::from(vec![Span::styled(
                "Start Rack Setup Failed",
                style::failed_update(),
            )]);
            let mut failed_body = Text::default();
            let prefix = vec![Span::styled("Message: ", style::selected())];
            push_text_lines(message, prefix, &mut failed_body.lines);
            body = failed_body;
            buttons = vec![ButtonText::new("Close", "Esc")];
            current_scroll_kind = Some(scroll_kind);
        }
    }

    let full_screen = Rect {
        width: state.screen_width,
        height: state.screen_height,
        x: 0,
        y: 0,
    };

    let popup_builder = PopupBuilder { header, body, buttons };
    let popup = popup_builder.build(
        full_screen,
        current_scroll_kind
            .as_ref()
            .map(|k| **k)
            .unwrap_or(PopupScrollKind::Disabled),
    );
    let actual_scroll_kind = popup.actual_scroll_kind();
    frame.render_widget(popup, full_screen);

    if let Some(current_scroll_kind) = current_scroll_kind {
        *current_scroll_kind = actual_scroll_kind;
    }
}

fn draw_rack_reset_popup(
    state: &State,
    frame: &mut Frame<'_>,
    kind: &mut PopupKind,
) {
    let header;
    let body;
    let buttons;
    let current_scroll_kind;

    match kind {
        PopupKind::Prompting => {
            header = Spans::from(vec![Span::styled(
                "Rack Reset (DESTRUCTIVE!)",
                style::header(true),
            )]);
            body = Text::from(vec![Spans::from(vec![Span::styled(
                "Would you like to reset the rack to an uninitialized state?",
                style::plain_text(),
            )])]);
            buttons =
                vec![ButtonText::new("Yes", "Y"), ButtonText::new("No", "N")];
            current_scroll_kind = None;
        }
        PopupKind::Waiting => {
            header = Spans::from(vec![Span::styled(
                "Rack Reset",
                style::header(true),
            )]);
            body = Text::from(vec![Spans::from(vec![Span::styled(
                "Waiting for rack reset to start",
                style::plain_text(),
            )])]);
            buttons = vec![];
            current_scroll_kind = None;
        }
        PopupKind::Failed { message, scroll_kind } => {
            header = Spans::from(vec![Span::styled(
                "Rack Reset Failed",
                style::failed_update(),
            )]);
            let mut failed_body = Text::default();
            let prefix = vec![Span::styled("Message: ", style::selected())];
            push_text_lines(message, prefix, &mut failed_body.lines);
            body = failed_body;
            buttons = vec![ButtonText::new("Close", "Esc")];
            current_scroll_kind = Some(scroll_kind);
        }
    }

    let full_screen = Rect {
        width: state.screen_width,
        height: state.screen_height,
        x: 0,
        y: 0,
    };

    let popup_builder = PopupBuilder { header, body, buttons };
    let popup = popup_builder.build(
        full_screen,
        current_scroll_kind
            .as_ref()
            .map(|k| **k)
            .unwrap_or(PopupScrollKind::Disabled),
    );
    let actual_scroll_kind = popup.actual_scroll_kind();
    frame.render_widget(popup, full_screen);

    if let Some(current_scroll_kind) = current_scroll_kind {
        *current_scroll_kind = actual_scroll_kind;
    }
}

impl Control for RackSetupPane {
    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        if self.popup.is_some() {
            return self.handle_cmd_in_popup(state, cmd);
        }
        match cmd {
            Cmd::Up => {
                self.scroll_offset = self.scroll_offset.saturating_sub(1);
                Some(Action::Redraw)
            }
            Cmd::Down => {
                self.scroll_offset += 1;
                Some(Action::Redraw)
            }
            Cmd::StartRackSetup => match state.rack_setup_state.as_ref() {
                Some(RackSetupState::Uninitialized) => {
                    self.popup = Some(Popup::RackSetup(PopupKind::Prompting));
                    Some(Action::Redraw)
                }
                Some(RackSetupState::Initialized) | None => None,
            },
            Cmd::ResetState => match state.rack_setup_state.as_ref() {
                Some(RackSetupState::Initialized) => {
                    self.popup = Some(Popup::RackReset(PopupKind::Prompting));
                    Some(Action::Redraw)
                }
                Some(RackSetupState::Uninitialized) | None => None,
            },
            _ => None,
        }
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
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
        let text = rss_config_text(state.rss_config.as_ref());
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
        let help = match state.rack_setup_state.as_ref() {
            Some(RackSetupState::Uninitialized) => {
                &self.rack_uninitialized_help
            }
            Some(RackSetupState::Initialized) => &self.rack_initialized_help,
            None => &self.help,
        };
        let help = help_text(help).block(block.clone());
        frame.render_widget(help, chunks[2]);

        // Make sure the top and bottom bars connect
        frame.render_widget(
            BoxConnector::new(BoxConnectorKind::Bottom),
            chunks[1],
        );

        if let Some(popup) = self.popup.as_mut() {
            match popup {
                Popup::RackSetup(kind) => {
                    draw_rack_setup_popup(state, frame, kind);
                }
                Popup::RackReset(kind) => {
                    draw_rack_reset_popup(state, frame, kind);
                }
            }
        }
    }
}

fn rss_config_text(config: Option<&CurrentRssUserConfig>) -> Text {
    fn dyn_span<'a>(
        ok: bool,
        ok_contents: impl Into<Cow<'a, str>>,
        bad_contents: &'static str,
    ) -> Span<'a> {
        let ok_style = Style::default().fg(colors::OX_GREEN_LIGHT);
        let bad_style = Style::default().fg(colors::OX_RED);
        if ok {
            Span::styled(ok_contents, ok_style)
        } else {
            Span::styled(bad_contents, bad_style)
        }
    }

    let label_style = Style::default().fg(colors::OX_OFF_WHITE);
    let ok_style = Style::default().fg(colors::OX_GREEN_LIGHT);
    let bad_style = Style::default().fg(colors::OX_RED);
    let dyn_style = |ok| if ok { ok_style } else { bad_style };

    let Some(config) = config else {
        return Text::styled("Rack Setup Unavailable", label_style);
    };

    let sensitive = &config.sensitive;
    let insensitive = &config.insensitive;

    // Special single-line values, where we convert some kind of condition into
    // a user-appropriate string.
    let mut spans = vec![
        Spans::from(vec![
            Span::styled("Uploaded cert/key pairs: ", label_style),
            Span::styled(
                sensitive.num_external_certificates.to_string(),
                dyn_style(sensitive.num_external_certificates > 0),
            ),
        ]),
        Spans::from(vec![
            Span::styled("Recovery password set: ", label_style),
            dyn_span(sensitive.recovery_silo_password_set, "Yes", "No"),
        ]),
    ];

    let net_config = insensitive.rack_network_config.as_ref();
    let uplink_port_speed = net_config
        .map_or(Cow::default(), |c| c.uplink_port_speed.to_string().into());
    let uplink_port_fec = net_config
        .map_or(Cow::default(), |c| c.uplink_port_fec.to_string().into());

    // List of single-line values, each of which may or may not be set; if it's
    // set we show its value, and if not we show "Not set" in bad_style.
    for (label, contents) in [
        (
            "External DNS zone name: ",
            Cow::from(insensitive.external_dns_zone_name.as_str()),
        ),
        ("Gateway IP: ", net_config.map_or("", |c| &c.gateway_ip).into()),
        (
            "Infrastructure first IP: ",
            net_config.map_or("", |c| &c.infra_ip_first).into(),
        ),
        (
            "Infrastructure last IP: ",
            net_config.map_or("", |c| &c.infra_ip_last).into(),
        ),
        ("Uplink port: ", net_config.map_or("", |c| &c.uplink_port).into()),
        ("Uplink port speed: ", uplink_port_speed),
        ("Uplink port FEC: ", uplink_port_fec),
        ("Uplink IP: ", net_config.map_or("", |c| &c.uplink_ip).into()),
    ] {
        spans.push(Spans::from(vec![
            Span::styled(label, label_style),
            dyn_span(!contents.is_empty(), contents, "Not set"),
        ]));
    }

    // Helper function for multivalued items: we either print "None" (in
    // bad_style) if there are no items, or return a the list of spans.
    let append_list =
        |spans: &mut Vec<Spans>, label_str, items: Vec<Vec<Span<'static>>>| {
            let label = Span::styled(label_str, label_style);
            if items.is_empty() {
                spans.push(Spans::from(vec![
                    label,
                    Span::styled("None", bad_style),
                ]));
            } else {
                spans.push(label.into());
                for item in items {
                    spans.push(Spans::from(item));
                }
            }
        };

    // Helper function to created a bulleted item for a single value.
    let plain_list_item = |item: String| {
        vec![Span::styled("  • ", label_style), Span::styled(item, ok_style)]
    };

    append_list(
        &mut spans,
        "NTP servers: ",
        insensitive.ntp_servers.iter().cloned().map(plain_list_item).collect(),
    );
    append_list(
        &mut spans,
        "DNS servers: ",
        insensitive.dns_servers.iter().cloned().map(plain_list_item).collect(),
    );
    append_list(
        &mut spans,
        "Internal services IP pool ranges: ",
        insensitive
            .internal_services_ip_pool_ranges
            .iter()
            .map(|r| {
                let s = match r {
                    IpRange::V4(r) => format!("{} - {}", r.first, r.last),
                    IpRange::V6(r) => format!("{} - {}", r.first, r.last),
                };
                plain_list_item(s)
            })
            .collect(),
    );
    append_list(
        &mut spans,
        "Sleds: ",
        insensitive
            .bootstrap_sleds
            .iter()
            .map(|desc| {
                let identifier = match &desc.baseboard {
                    Baseboard::Gimlet { identifier, .. } => identifier,
                    Baseboard::Pc { identifier, .. } => identifier,
                    Baseboard::Unknown => "unknown",
                };
                let mut spans = vec![
                    Span::styled("  • ", label_style),
                    Span::styled(format!("Cubby {}", desc.id.slot), ok_style),
                    Span::styled(
                        format!(" ({identifier}, bootstrap address "),
                        label_style,
                    ),
                ];
                if let Some(addr) = desc.bootstrap_ip {
                    spans.push(Span::styled(addr.to_string(), label_style));
                } else {
                    spans.push(Span::styled("UNKNOWN", bad_style));
                }
                spans.push(Span::styled(")", label_style));
                spans
            })
            .collect(),
    );

    // Add a "trailing newline" for scrolling to work correctly.
    spans.push(Spans::default());

    Text::from(spans)
}
