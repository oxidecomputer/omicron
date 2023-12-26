// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::help_text;
use super::push_text_lines;
use super::ComputedScrollOffset;
use super::PendingScroll;
use crate::keymap::ShowPopupCmd;
use crate::ui::defaults::style;
use crate::ui::widgets::BoxConnector;
use crate::ui::widgets::BoxConnectorKind;
use crate::ui::widgets::ButtonText;
use crate::ui::widgets::PopupBuilder;
use crate::ui::widgets::PopupScrollOffset;
use crate::Action;
use crate::Cmd;
use crate::Control;
use crate::Frame;
use crate::State;
use ratatui::layout::Constraint;
use ratatui::layout::Direction;
use ratatui::layout::Layout;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::text::Text;
use ratatui::widgets::Block;
use ratatui::widgets::BorderType;
use ratatui::widgets::Borders;
use ratatui::widgets::Paragraph;
use std::borrow::Cow;
use wicketd_client::types::Baseboard;
use wicketd_client::types::CurrentRssUserConfig;
use wicketd_client::types::IpRange;
use wicketd_client::types::RackOperationStatus;

#[derive(Debug)]
enum Popup {
    RackSetup(PopupKind),
    RackReset(PopupKind),
    RackStatusDetails(PopupScrollOffset),
}

impl Popup {
    fn new_rack_setup() -> Self {
        Self::RackSetup(PopupKind::Prompting)
    }

    fn new_rack_reset() -> Self {
        Self::RackReset(PopupKind::Prompting)
    }

    fn new_rack_status_details() -> Self {
        Self::RackStatusDetails(PopupScrollOffset::default())
    }

    fn scroll_offset_mut(&mut self) -> Option<&mut PopupScrollOffset> {
        match self {
            Popup::RackSetup(kind) | Popup::RackReset(kind) => match kind {
                PopupKind::Prompting | PopupKind::Waiting => None,
                PopupKind::Failed { scroll_offset, .. } => Some(scroll_offset),
            },
            Popup::RackStatusDetails(scroll_offset) => Some(scroll_offset),
        }
    }
}

#[derive(Debug)]
enum PopupKind {
    Prompting,
    Waiting,
    Failed { message: String, scroll_offset: PopupScrollOffset },
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
    pending_scroll: Option<PendingScroll>,

    popup: Option<Popup>,
}

impl Default for RackSetupPane {
    fn default() -> Self {
        Self {
            help: vec![
                ("Scroll", "<Up/Down>"),
                ("Current Status Details", "<D>"),
            ],
            rack_uninitialized_help: vec![
                ("Scroll", "<Up/Down>"),
                ("Current Status Details", "<D>"),
                ("Start Rack Setup", "<Ctrl-K>"),
            ],
            rack_initialized_help: vec![
                ("Scroll", "<Up/Down>"),
                ("Current Status Details", "<D>"),
                ("Start Rack Reset", "<Ctrl-R Ctrl-R>"),
            ],
            scroll_offset: 0,
            pending_scroll: None,
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

        // Handle scroll commands here.
        if let Some(offset) = popup.scroll_offset_mut() {
            if let Some(pending_scroll) = PendingScroll::from_cmd(&cmd) {
                offset.set_pending_scroll(pending_scroll);
                return Some(Action::Redraw);
            }
        }

        match (popup, cmd) {
            (
                Popup::RackSetup(PopupKind::Prompting)
                | Popup::RackReset(PopupKind::Prompting),
                Cmd::No,
            )
            | (
                Popup::RackSetup(PopupKind::Failed { .. })
                | Popup::RackReset(PopupKind::Failed { .. })
                | Popup::RackStatusDetails(_),
                Cmd::Exit,
            ) => {
                self.popup = None;
                Some(Action::Redraw)
            }
            (Popup::RackSetup(kind @ PopupKind::Prompting), Cmd::Yes) => {
                *kind = PopupKind::Waiting;
                Some(Action::StartRackSetup)
            }
            (Popup::RackReset(kind @ PopupKind::Prompting), Cmd::Yes) => {
                *kind = PopupKind::Waiting;
                Some(Action::StartRackReset)
            }
            (
                Popup::RackSetup(kind @ PopupKind::Waiting),
                Cmd::ShowPopup(ShowPopupCmd::StartRackSetupResponse(response)),
            ) => {
                match response {
                    Ok(()) => {
                        self.popup = None;
                    }
                    Err(message) => {
                        *kind = PopupKind::Failed {
                            message,
                            scroll_offset: PopupScrollOffset::default(),
                        };
                    }
                }
                Some(Action::Redraw)
            }
            (
                Popup::RackReset(kind @ PopupKind::Waiting),
                Cmd::ShowPopup(ShowPopupCmd::StartRackResetResponse(response)),
            ) => {
                match response {
                    Ok(()) => {
                        self.popup = None;
                    }
                    Err(message) => {
                        *kind = PopupKind::Failed {
                            message,
                            scroll_offset: PopupScrollOffset::default(),
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
    let full_screen = Rect {
        width: state.screen_width,
        height: state.screen_height,
        x: 0,
        y: 0,
    };

    match kind {
        PopupKind::Prompting => {
            let header = Line::from(vec![Span::styled(
                "Start Rack Setup",
                style::header(true),
            )]);
            let body = Text::from(vec![Line::from(vec![Span::styled(
                "Would you like to begin rack setup?",
                style::plain_text(),
            )])]);
            let buttons =
                vec![ButtonText::new("Yes", "Y"), ButtonText::new("No", "N")];

            let popup_builder = PopupBuilder { header, body, buttons };
            let popup = popup_builder.build(full_screen);
            frame.render_widget(popup, full_screen);
        }
        PopupKind::Waiting => {
            let header = Line::from(vec![Span::styled(
                "Start Rack Setup",
                style::header(true),
            )]);
            let body = Text::from(vec![Line::from(vec![Span::styled(
                "Waiting for rack setup to start",
                style::plain_text(),
            )])]);
            let buttons = vec![];

            let popup_builder = PopupBuilder { header, body, buttons };
            let popup = popup_builder.build(full_screen);
            frame.render_widget(popup, full_screen);
        }
        PopupKind::Failed { message, scroll_offset } => {
            let header = Line::from(vec![Span::styled(
                "Start Rack Setup Failed",
                style::failed_update(),
            )]);
            let mut failed_body = Text::default();
            let prefix = vec![Span::styled("Message: ", style::selected())];
            push_text_lines(message, prefix, &mut failed_body.lines);
            let body = failed_body;
            let buttons = vec![ButtonText::new("Close", "Esc")];

            let popup_builder = PopupBuilder { header, body, buttons };
            let popup =
                popup_builder.build_scrollable(full_screen, *scroll_offset);
            *scroll_offset = popup.actual_scroll_offset();
            frame.render_widget(popup, full_screen);
        }
    }
}

fn draw_rack_reset_popup(
    state: &State,
    frame: &mut Frame<'_>,
    kind: &mut PopupKind,
) {
    let full_screen = Rect {
        width: state.screen_width,
        height: state.screen_height,
        x: 0,
        y: 0,
    };

    match kind {
        PopupKind::Prompting => {
            let header = Line::from(vec![Span::styled(
                "Rack Reset (DESTRUCTIVE!)",
                style::header(true),
            )]);
            let body = Text::from(vec![Line::from(vec![Span::styled(
                "Would you like to reset the rack to an uninitialized state?",
                style::plain_text(),
            )])]);
            let buttons =
                vec![ButtonText::new("Yes", "Y"), ButtonText::new("No", "N")];

            let popup_builder = PopupBuilder { header, body, buttons };
            let popup = popup_builder.build(full_screen);
            frame.render_widget(popup, full_screen);
        }
        PopupKind::Waiting => {
            let header = Line::from(vec![Span::styled(
                "Rack Reset",
                style::header(true),
            )]);
            let body = Text::from(vec![Line::from(vec![Span::styled(
                "Waiting for rack reset to start",
                style::plain_text(),
            )])]);
            let buttons = vec![];

            let popup_builder = PopupBuilder { header, body, buttons };
            let popup = popup_builder.build(full_screen);
            frame.render_widget(popup, full_screen);
        }
        PopupKind::Failed { message, scroll_offset } => {
            let header = Line::from(vec![Span::styled(
                "Rack Reset Failed",
                style::failed_update(),
            )]);
            let mut failed_body = Text::default();
            let prefix = vec![Span::styled("Message: ", style::selected())];
            push_text_lines(message, prefix, &mut failed_body.lines);
            let body = failed_body;
            let buttons = vec![ButtonText::new("Close", "Esc")];

            let popup_builder = PopupBuilder { header, body, buttons };
            let popup =
                popup_builder.build_scrollable(full_screen, *scroll_offset);
            *scroll_offset = popup.actual_scroll_offset();
            frame.render_widget(popup, full_screen);
        }
    }
}

fn draw_rack_status_details_popup(
    state: &State,
    frame: &mut Frame<'_>,
    scroll_offset: &mut PopupScrollOffset,
) {
    let header = Line::from(vec![Span::styled(
        "Current Rack Setup Status",
        style::header(true),
    )]);
    let buttons = vec![ButtonText::new("Close", "Esc")];
    let mut body = Text::default();

    let status = Span::styled("Status: ", style::selected());
    let prefix = vec![Span::styled("Message: ", style::selected())];
    match state.rack_setup_state.as_ref() {
        Ok(RackOperationStatus::Uninitialized { reset_id }) => {
            body.lines.push(Line::from(vec![
                status,
                Span::styled("Uninitialized", style::plain_text()),
            ]));
            if let Some(id) = reset_id {
                body.lines.push(Line::from(vec![Span::styled(
                    format!("Last reset operation ID: {}", id.0),
                    style::plain_text(),
                )]));
            }
        }
        Ok(RackOperationStatus::Initialized { id }) => {
            body.lines.push(Line::from(vec![
                status,
                Span::styled("Initialized", style::plain_text()),
            ]));
            if let Some(id) = id {
                body.lines.push(Line::from(vec![Span::styled(
                    format!("Last initialization operation ID: {}", id.0),
                    style::plain_text(),
                )]));
            }
        }
        Ok(RackOperationStatus::InitializationFailed { id, message }) => {
            body.lines.push(Line::from(vec![
                status,
                Span::styled("Initialization Failed", style::plain_text()),
            ]));
            body.lines.push(Line::from(vec![Span::styled(
                format!("Last initialization operation ID: {}", id.0),
                style::plain_text(),
            )]));
            push_text_lines(message, prefix, &mut body.lines);
        }
        Ok(RackOperationStatus::InitializationPanicked { id }) => {
            body.lines.push(Line::from(vec![
                status,
                Span::styled("Initialization Panicked", style::plain_text()),
            ]));
            body.lines.push(Line::from(vec![Span::styled(
                format!("Last initialization operation ID: {}", id.0),
                style::plain_text(),
            )]));
        }
        Ok(RackOperationStatus::ResetFailed { id, message }) => {
            body.lines.push(Line::from(vec![
                status,
                Span::styled("Reset Failed", style::plain_text()),
            ]));
            body.lines.push(Line::from(vec![Span::styled(
                format!("Last reset operation ID: {}", id.0),
                style::plain_text(),
            )]));
            push_text_lines(message, prefix, &mut body.lines);
        }
        Ok(RackOperationStatus::ResetPanicked { id }) => {
            body.lines.push(Line::from(vec![
                status,
                Span::styled("Reset Panicked", style::plain_text()),
            ]));
            body.lines.push(Line::from(vec![Span::styled(
                format!("Last reset operation ID: {}", id.0),
                style::plain_text(),
            )]));
        }
        Ok(RackOperationStatus::Initializing { id }) => {
            body.lines.push(Line::from(vec![
                status,
                Span::styled("Initializing", style::plain_text()),
            ]));
            body.lines.push(Line::from(vec![Span::styled(
                format!("Current operation ID: {}", id.0),
                style::plain_text(),
            )]));
        }
        Ok(RackOperationStatus::Resetting { id }) => {
            body.lines.push(Line::from(vec![
                status,
                Span::styled("Resetting", style::plain_text()),
            ]));
            body.lines.push(Line::from(vec![Span::styled(
                format!("Current operation ID: {}", id.0),
                style::plain_text(),
            )]));
        }
        Err(message) => {
            body.lines.push(Line::from(vec![
                status,
                Span::styled(
                    "Unknown (request to wicketd failed)",
                    style::plain_text(),
                ),
            ]));
            push_text_lines(message, prefix, &mut body.lines);
        }
    }

    let full_screen = Rect {
        width: state.screen_width,
        height: state.screen_height,
        x: 0,
        y: 0,
    };

    let popup_builder = PopupBuilder { header, body, buttons };
    let popup = popup_builder.build_scrollable(full_screen, *scroll_offset);
    *scroll_offset = popup.actual_scroll_offset();
    frame.render_widget(popup, full_screen);
}

impl Control for RackSetupPane {
    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        if self.popup.is_some() {
            return self.handle_cmd_in_popup(state, cmd);
        }
        if let Some(pending_scroll) = PendingScroll::from_cmd(&cmd) {
            self.pending_scroll = Some(pending_scroll);
            return Some(Action::Redraw);
        }

        match cmd {
            Cmd::Details => {
                self.popup = Some(Popup::new_rack_status_details());
                Some(Action::Redraw)
            }
            Cmd::StartRackSetup => match state.rack_setup_state.as_ref() {
                Ok(RackOperationStatus::Uninitialized { .. }) => {
                    self.popup = Some(Popup::new_rack_setup());
                    Some(Action::Redraw)
                }
                _ => None,
            },
            Cmd::ResetState => match state.rack_setup_state.as_ref() {
                Ok(RackOperationStatus::Initialized { .. }) => {
                    self.popup = Some(Popup::new_rack_reset());
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
        rect: ratatui::layout::Rect,
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
        let title_bar = Paragraph::new(Line::from(vec![Span::styled(
            "Oxide Rack Setup",
            border_style,
        )]))
        .block(block.clone());
        frame.render_widget(title_bar, chunks[0]);

        // Draw the contents
        let contents_block = block
            .clone()
            .borders(Borders::LEFT | Borders::RIGHT | Borders::TOP);
        let text = rss_config_text(
            state.rack_setup_state.as_ref(),
            state.rss_config.as_ref(),
        );
        let y_offset = ComputedScrollOffset::new(
            self.scroll_offset,
            text.height(),
            chunks[1].height as usize,
            self.pending_scroll.take(),
        )
        .into_offset();
        self.scroll_offset = y_offset as usize;

        let inventory = Paragraph::new(text)
            .block(contents_block.clone())
            .scroll((y_offset, 0));
        frame.render_widget(inventory, chunks[1]);

        // Draw the help bar
        let help = match state.rack_setup_state.as_ref() {
            Ok(RackOperationStatus::Uninitialized { .. }) => {
                &self.rack_uninitialized_help
            }
            Ok(RackOperationStatus::Initialized { .. }) => {
                &self.rack_initialized_help
            }
            _ => &self.help,
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
                Popup::RackStatusDetails(scroll_offset) => {
                    draw_rack_status_details_popup(state, frame, scroll_offset);
                }
            }
        }
    }
}

fn rss_config_text<'a>(
    setup_state: Result<&RackOperationStatus, &String>,
    config: Option<&'a CurrentRssUserConfig>,
) -> Text<'a> {
    fn dyn_span<'a>(
        ok: bool,
        ok_contents: impl Into<Cow<'a, str>>,
        bad_contents: &'static str,
    ) -> Span<'a> {
        let ok_style = style::text_success();
        let bad_style = style::text_failure();
        if ok {
            Span::styled(ok_contents, ok_style)
        } else {
            Span::styled(bad_contents, bad_style)
        }
    }

    let label_style = style::text_label();
    let ok_style = style::text_success();
    let bad_style = style::text_failure();
    let warn_style = style::text_warning();
    let dyn_style = |ok| if ok { ok_style } else { bad_style };

    let setup_description = match setup_state {
        Ok(RackOperationStatus::Uninitialized { .. }) => {
            Span::styled("Uninitialized", ok_style)
        }
        Ok(RackOperationStatus::Initialized { .. }) => {
            Span::styled("Initialized", ok_style)
        }
        Ok(RackOperationStatus::Initializing { .. }) => {
            Span::styled("Initializing", warn_style)
        }
        Ok(RackOperationStatus::Resetting { .. }) => {
            Span::styled("Resetting", warn_style)
        }
        Ok(
            RackOperationStatus::InitializationFailed { .. }
            | RackOperationStatus::InitializationPanicked { .. },
        ) => Span::styled("Initialization Failed", bad_style),
        Ok(
            RackOperationStatus::ResetFailed { .. }
            | RackOperationStatus::ResetPanicked { .. },
        ) => Span::styled("Resetting Failed", bad_style),
        Err(_) => Span::styled("Unknown", bad_style),
    };

    let mut spans = vec![
        Line::from(vec![
            Span::styled("Current rack status: ", label_style),
            setup_description,
        ]),
        Line::default(),
    ];

    let Some(config) = config else {
        return Text::styled("Rack Setup Unavailable", label_style);
    };

    let sensitive = &config.sensitive;
    let insensitive = &config.insensitive;

    // Special single-line values, where we convert some kind of condition into
    // a user-appropriate string.
    spans.push(Line::from(vec![
        Span::styled("Uploaded cert/key pairs: ", label_style),
        Span::styled(
            sensitive.num_external_certificates.to_string(),
            dyn_style(sensitive.num_external_certificates > 0),
        ),
    ]));
    spans.push(Line::from(vec![
        Span::styled("Recovery password set: ", label_style),
        dyn_span(sensitive.recovery_silo_password_set, "Yes", "No"),
    ]));

    let net_config = insensitive.rack_network_config.as_ref();

    // List of single-line values, each of which may or may not be set; if it's
    // set we show its value, and if not we show "Not set" in bad_style.
    for (label, contents) in [
        (
            "External DNS zone name: ",
            Cow::from(insensitive.external_dns_zone_name.as_str()),
        ),
        (
            "Infrastructure first IP: ",
            net_config
                .map_or("".into(), |c| c.infra_ip_first.to_string().into()),
        ),
        (
            "Infrastructure last IP: ",
            net_config
                .map_or("".into(), |c| c.infra_ip_last.to_string().into()),
        ),
    ] {
        spans.push(Line::from(vec![
            Span::styled(label, label_style),
            dyn_span(!contents.is_empty(), contents, "Not set"),
        ]));
    }

    // Helper function for multivalued items: we either print "None" (in
    // bad_style) if there are no items, or return a the list of spans.
    let append_list = |spans: &mut Vec<Line>,
                       label_str,
                       items: Vec<Vec<Span<'static>>>| {
        let label = Span::styled(label_str, label_style);
        if items.is_empty() {
            spans
                .push(Line::from(vec![label, Span::styled("None", bad_style)]));
        } else {
            spans.push(label.into());
            for item in items {
                spans.push(Line::from(item));
            }
        }
    };

    // Helper function to created a bulleted item for a single value.
    let plain_list_item = |item: String| {
        vec![Span::styled("  • ", label_style), Span::styled(item, ok_style)]
    };

    if let Some(cfg) = insensitive.rack_network_config.as_ref() {
        for (i, uplink) in cfg.ports.iter().enumerate() {
            let mut items = vec![
                vec![
                    Span::styled("  • Switch    : ", label_style),
                    Span::styled(uplink.switch.to_string(), ok_style),
                ],
                vec![
                    Span::styled("  • Speed     : ", label_style),
                    Span::styled(
                        uplink.uplink_port_speed.to_string(),
                        ok_style,
                    ),
                ],
                vec![
                    Span::styled("  • FEC       : ", label_style),
                    Span::styled(uplink.uplink_port_fec.to_string(), ok_style),
                ],
            ];

            let routes = uplink.routes.iter().map(|r| {
                vec![
                    Span::styled("  • Route     : ", label_style),
                    Span::styled(
                        format!("{} -> {}", r.destination, r.nexthop),
                        ok_style,
                    ),
                ]
            });

            let addresses = uplink.addresses.iter().map(|a| {
                vec![
                    Span::styled("  • Address   : ", label_style),
                    Span::styled(a.to_string(), ok_style),
                ]
            });

            let peers = uplink.bgp_peers.iter().map(|p| {
                vec![
                    Span::styled("  • BGP peer  : ", label_style),
                    Span::styled(format!("{} ASN={}", p.addr, p.asn), ok_style),
                ]
            });

            items.extend(routes);
            items.extend(addresses);
            items.extend(peers);

            append_list(
                &mut spans,
                Cow::from(format!("Port {}: ", i + 1)),
                items,
            );
        }
    } else {
        append_list(&mut spans, "Ports: ".into(), vec![]);
    }

    append_list(
        &mut spans,
        "NTP servers: ".into(),
        insensitive.ntp_servers.iter().cloned().map(plain_list_item).collect(),
    );
    append_list(
        &mut spans,
        "DNS servers: ".into(),
        insensitive
            .dns_servers
            .iter()
            .map(|s| plain_list_item(s.to_string()))
            .collect(),
    );
    append_list(
        &mut spans,
        "Internal services IP pool ranges: ".into(),
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
        "External DNS IPs: ".into(),
        insensitive
            .external_dns_ips
            .iter()
            .cloned()
            .map(|ip| plain_list_item(ip.to_string()))
            .collect(),
    );
    append_list(
        &mut spans,
        "Sleds: ".into(),
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
    spans.push(Line::default());

    Text::from(spans)
}
