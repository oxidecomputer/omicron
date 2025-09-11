// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ComputedScrollOffset;
use super::PendingScroll;
use super::help_text;
use super::push_text_lines;
use crate::Action;
use crate::Cmd;
use crate::Control;
use crate::State;
use crate::keymap::ShowPopupCmd;
use crate::ui::defaults::style;
use crate::ui::widgets::BoxConnector;
use crate::ui::widgets::BoxConnectorKind;
use crate::ui::widgets::ButtonText;
use crate::ui::widgets::PopupBuilder;
use crate::ui::widgets::PopupScrollOffset;
use itertools::Itertools;
use omicron_common::address::IpRange;
use omicron_common::api::internal::shared::AllowedSourceIps;
use omicron_common::api::internal::shared::BgpConfig;
use omicron_common::api::internal::shared::LldpPortConfig;
use omicron_common::api::internal::shared::RouteConfig;
use ratatui::Frame;
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
use wicket_common::rack_setup::BgpAuthKeyInfo;
use wicket_common::rack_setup::BgpAuthKeyStatus;
use wicket_common::rack_setup::CurrentRssUserConfigInsensitive;
use wicket_common::rack_setup::UserSpecifiedBgpPeerConfig;
use wicket_common::rack_setup::UserSpecifiedImportExportPolicy;
use wicket_common::rack_setup::UserSpecifiedPortConfig;
use wicket_common::rack_setup::UserSpecifiedRackNetworkConfig;
use wicketd_client::types::CurrentRssUserConfig;
use wicketd_client::types::CurrentRssUserConfigSensitive;
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
            let mut body = Text::from(vec![Line::from(vec![Span::styled(
                "Would you like to reset the rack to an uninitialized state?",
                style::plain_text(),
            )])]);
            // One might see this warning and ask "why is this feature even
            // here, then?" We do eventually want "rack reset" to work as a
            // sort of factory reset, and the current implementation is a good
            // starting point, so there's no sense in removing it (this is
            // certainly not the only feature currently in this state).
            //
            // The warning is intended to remove the speed bump where someone
            // has to find out the hard way that this doesn't work, without
            // removing the speed bump where we're reminded of the feature that
            // doesn't work yet.
            body.lines.push(Line::from(""));
            body.lines.push(Line::from(vec![
                Span::styled("WARNING: ", style::warning()),
                Span::styled(
                    "This does not work yet and will leave the rack \
                     in an unknown state (see omicron#3820)",
                    style::plain_text(),
                ),
            ]));
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
                    format!("Last reset operation ID: {}", id),
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
                    format!("Last initialization operation ID: {}", id),
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
                format!("Last initialization operation ID: {}", id),
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
                format!("Last initialization operation ID: {}", id),
                style::plain_text(),
            )]));
        }
        Ok(RackOperationStatus::ResetFailed { id, message }) => {
            body.lines.push(Line::from(vec![
                status,
                Span::styled("Reset Failed", style::plain_text()),
            ]));
            body.lines.push(Line::from(vec![Span::styled(
                format!("Last reset operation ID: {}", id),
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
                format!("Last reset operation ID: {}", id),
                style::plain_text(),
            )]));
        }
        Ok(RackOperationStatus::Initializing { id, step }) => {
            body.lines.push(Line::from(vec![
                status,
                Span::styled("Initializing", style::plain_text()),
            ]));
            let max = step.max_step();
            let index = step.index();
            body.lines.push(Line::from(vec![Span::styled(
                format!("Current step: {}/{}", index, max),
                style::plain_text(),
            )]));
            body.lines.push(Line::from(vec![Span::styled(
                format!("Current operation: {:?}", step),
                style::plain_text(),
            )]));
            body.lines.push(Line::from(vec![Span::styled(
                format!("Current operation ID: {}", id),
                style::plain_text(),
            )]));
        }
        Ok(RackOperationStatus::Resetting { id }) => {
            body.lines.push(Line::from(vec![
                status,
                Span::styled("Resetting", style::plain_text()),
            ]));
            body.lines.push(Line::from(vec![Span::styled(
                format!("Current operation ID: {}", id),
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
        Ok(RackOperationStatus::Initializing { step, .. }) => {
            let max = step.max_step();
            let index = step.index();
            let msg = format!("Initializing: Step {}/{}", index, max);
            Span::styled(msg, warn_style)
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

    let CurrentRssUserConfigSensitive {
        bgp_auth_keys,
        num_external_certificates,
        recovery_silo_password_set,
    } = &config.sensitive;
    let CurrentRssUserConfigInsensitive {
        bootstrap_sleds,
        ntp_servers,
        dns_servers,
        internal_services_ip_pool_ranges,
        external_dns_ips,
        external_dns_zone_name,
        rack_network_config,
        allowed_source_ips,
    } = &config.insensitive;

    // Special single-line values, where we convert some kind of condition into
    // a user-appropriate string.
    spans.push(Line::from(vec![
        Span::styled("Uploaded cert/key pairs: ", label_style),
        Span::styled(
            num_external_certificates.to_string(),
            dyn_style(*num_external_certificates > 0),
        ),
    ]));
    spans.push(Line::from(vec![
        Span::styled("Recovery password set: ", label_style),
        dyn_span(*recovery_silo_password_set, "Yes", "No"),
    ]));

    // List of single-line values, each of which may or may not be set; if it's
    // set we show its value, and if not we show "Not set" in bad_style.
    for (label, contents) in [
        (
            "External DNS zone name: ",
            Cow::from(external_dns_zone_name.as_str()),
        ),
        (
            "Infrastructure first IP: ",
            rack_network_config
                .as_ref()
                .map_or("".into(), |c| c.infra_ip_first.to_string().into()),
        ),
        (
            "Infrastructure last IP: ",
            rack_network_config
                .as_ref()
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

    if let Some(cfg) = rack_network_config.as_ref() {
        // This style ensures that if a new field is added to the struct, it
        // fails to compile.
        let UserSpecifiedRackNetworkConfig {
            // infra_ip_first and infra_ip_last have already been handled above.
            infra_ip_first: _,
            infra_ip_last: _,
            // switch0 and switch1 re handled via the iter_uplinks iterator.
            switch0: _,
            switch1: _,
            bgp,
        } = cfg;

        for (i, (switch, port, uplink)) in cfg.iter_uplinks().enumerate() {
            let UserSpecifiedPortConfig {
                routes,
                addresses,
                uplink_port_speed,
                uplink_port_fec,
                autoneg,
                bgp_peers,
                lldp,
                tx_eq,
            } = uplink;

            let mut items = vec![
                vec![
                    Span::styled("  • Port          : ", label_style),
                    Span::styled(port.to_string(), ok_style),
                    Span::styled(" on switch ", label_style),
                    Span::styled(switch.to_string(), ok_style),
                ],
                vec![
                    Span::styled("  • Speed         : ", label_style),
                    Span::styled(uplink_port_speed.to_string(), ok_style),
                ],
                vec![
                    Span::styled("  • FEC           : ", label_style),
                    Span::styled(
                        match uplink_port_fec {
                            Some(fec) => fec.to_string(),
                            None => "unspecified".to_string(),
                        },
                        ok_style,
                    ),
                ],
                vec![
                    Span::styled("  • Autoneg       : ", label_style),
                    if *autoneg {
                        Span::styled("enabled", ok_style)
                    } else {
                        // bad_style isn't right here because there's no
                        // necessary action item, but green/ok isn't also
                        // right. So use warn_style.
                        Span::styled("disabled", warn_style)
                    },
                ],
            ];

            let routes =
                routes.iter().map(|r| {
                    let RouteConfig {
                        destination,
                        nexthop,
                        vlan_id,
                        rib_priority,
                    } = r;

                    let mut items = vec![
                        Span::styled("  • Route         : ", label_style),
                        Span::styled(
                            format!("{} -> {}", destination, nexthop),
                            ok_style,
                        ),
                    ];
                    if let Some(vlan_id) = vlan_id {
                        items.extend([
                            Span::styled(" (vlan_id=", label_style),
                            Span::styled(vlan_id.to_string(), ok_style),
                            Span::styled(")", label_style),
                        ]);
                    }
                    if let Some(rib_priority) = rib_priority {
                        items.extend([
                            Span::styled(" (rib_priority=", label_style),
                            Span::styled(rib_priority.to_string(), ok_style),
                            Span::styled(")", label_style),
                        ]);
                    }

                    items
                });

            let addresses = addresses.iter().map(|a| {
                let mut items = vec![
                    Span::styled("  • Address       : ", label_style),
                    Span::styled(a.address.to_string(), ok_style),
                ];
                if let Some(vlan_id) = a.vlan_id {
                    items.extend([
                        Span::styled(" (vlan_id=", label_style),
                        Span::styled(vlan_id.to_string(), ok_style),
                        Span::styled(")", label_style),
                    ]);
                }

                items
            });

            let peers = bgp_peers.iter().flat_map(|p| {
                let UserSpecifiedBgpPeerConfig {
                    asn,
                    port,
                    addr,

                    // These values are accessed via methods, since they have
                    // defaults defined by the methods.
                    hold_time: _,
                    idle_hold_time: _,
                    delay_open: _,
                    connect_retry: _,
                    keepalive: _,

                    remote_asn,
                    min_ttl,
                    auth_key_id,
                    multi_exit_discriminator,
                    communities,
                    local_pref,
                    enforce_first_as,
                    allowed_import,
                    allowed_export,
                    vlan_id,
                } = p;

                let mut lines = vec![
                    vec![
                        Span::styled("  • BGP peer      : ", label_style),
                        Span::styled(addr.to_string(), ok_style),
                        Span::styled(" asn=", label_style),
                        Span::styled(asn.to_string(), ok_style),
                        Span::styled(" port=", label_style),
                        Span::styled(port.clone(), ok_style),
                    ],
                    vec![
                        Span::styled("    Intervals     :", label_style),
                        Span::styled(" hold=", label_style),
                        Span::styled(format!("{}s", p.hold_time()), ok_style),
                        Span::styled(" idle_hold=", label_style),
                        Span::styled(
                            format!("{}s", p.idle_hold_time()),
                            ok_style,
                        ),
                        Span::styled(" delay_open=", label_style),
                        Span::styled(format!("{}s", p.delay_open()), ok_style),
                        Span::styled(" connect_retry=", label_style),
                        Span::styled(
                            format!("{}s", p.connect_retry()),
                            ok_style,
                        ),
                        Span::styled(" keepalive=", label_style),
                        Span::styled(format!("{}s", p.keepalive()), ok_style),
                    ],
                ];
                {
                    // These are all optional settings.
                    let mut settings =
                        vec![Span::styled("    Settings      :", label_style)];

                    if let Some(remote_asn) = remote_asn {
                        settings.extend([
                            Span::styled(" remote_asn=", label_style),
                            Span::styled(remote_asn.to_string(), ok_style),
                        ]);
                    }
                    if let Some(min_ttl) = min_ttl {
                        settings.extend([
                            Span::styled(" min_ttl=", label_style),
                            Span::styled(min_ttl.to_string(), ok_style),
                        ]);
                    }
                    if let Some(multi_exit_discriminator) =
                        multi_exit_discriminator
                    {
                        settings.extend([
                            Span::styled(" med=", label_style),
                            Span::styled(
                                multi_exit_discriminator.to_string(),
                                ok_style,
                            ),
                        ]);
                    }
                    if let Some(local_pref) = local_pref {
                        settings.extend([
                            Span::styled(" local_pref=", label_style),
                            Span::styled(local_pref.to_string(), ok_style),
                        ]);
                    }
                    if *enforce_first_as {
                        settings.extend([
                            Span::styled(" enforce_first_as=", label_style),
                            Span::styled("true", ok_style),
                        ]);
                    }
                    if !communities.is_empty() {
                        settings.extend([
                            Span::styled(" communities=", label_style),
                            Span::styled(
                                communities.iter().join(","),
                                ok_style,
                            ),
                        ]);
                    }
                    if let Some(vlan_id) = vlan_id {
                        settings.extend([
                            Span::styled(" vlan_id=", label_style),
                            Span::styled(vlan_id.to_string(), ok_style),
                        ]);
                    }

                    // We always push one element in -- check if any other
                    // elements were pushed.
                    if settings.len() > 1 {
                        lines.push(settings);
                    }
                }

                if let Some(auth_key_id) = auth_key_id {
                    let mut auth_key_line =
                        vec![Span::styled("    Auth key      : ", label_style)];
                    match bgp_auth_keys.data.get(auth_key_id) {
                        Some(BgpAuthKeyStatus::Unset) => {
                            auth_key_line.extend([
                                Span::styled(
                                    auth_key_id.to_string(),
                                    bad_style,
                                ),
                                Span::styled(": ", label_style),
                                Span::styled("unset", bad_style),
                            ]);
                        }

                        // This matches the format defined in
                        // BgpAuthKeyInfo::to_string_styled.
                        Some(BgpAuthKeyStatus::Set {
                            info: BgpAuthKeyInfo::TcpMd5 { sha256 },
                        }) => {
                            auth_key_line.extend([
                                Span::styled(auth_key_id.to_string(), ok_style),
                                Span::styled(": ", label_style),
                                Span::styled("TCP-MD5", ok_style),
                                Span::styled(" (SHA-256: ", label_style),
                                Span::styled(sha256.to_string(), ok_style),
                                Span::styled(")", label_style),
                            ]);
                        }

                        None => {
                            // This shouldn't happen -- all auth keys should be
                            // known.
                            auth_key_line.extend([
                                Span::styled(
                                    auth_key_id.to_string(),
                                    bad_style,
                                ),
                                Span::styled(
                                    "unknown (internal error)",
                                    bad_style,
                                ),
                            ]);
                        }
                    }
                    lines.push(auth_key_line);
                }

                let import_export_policy_line =
                    |label: &'static str,
                     policy: &UserSpecifiedImportExportPolicy|
                     -> Option<Vec<Span<'static>>> {
                        match policy {
                            UserSpecifiedImportExportPolicy::NoFiltering => {
                                None
                            }
                            UserSpecifiedImportExportPolicy::Allow(
                                prefixes,
                            ) => {
                                let mut line =
                                    vec![Span::styled(label, label_style)];
                                if prefixes.is_empty() {
                                    line.push(Span::styled(
                                        "no prefixes allowed",
                                        warn_style,
                                    ));
                                } else {
                                    line.push(Span::styled(
                                        "allowed=",
                                        label_style,
                                    ));
                                    line.push(Span::styled(
                                        prefixes.iter().join(","),
                                        ok_style,
                                    ));
                                }

                                Some(line)
                            }
                        }
                    };

                if let Some(allowed_import) = import_export_policy_line(
                    "    Import policy : ",
                    allowed_import,
                ) {
                    lines.push(allowed_import);
                }
                if let Some(allowed_export) = import_export_policy_line(
                    "    Export policy : ",
                    allowed_export,
                ) {
                    lines.push(allowed_export);
                }

                lines
            });

            items.extend(routes);
            items.extend(addresses);
            items.extend(peers);

            if let Some(lp) = lldp {
                let LldpPortConfig {
                    status,
                    chassis_id,
                    port_id,
                    system_name,
                    system_description,
                    port_description,
                    management_addrs,
                } = lp;

                let mut lldp = vec![
                    vec![Span::styled("  • LLDP port settings: ", label_style)],
                    vec![
                        Span::styled("    • Admin status      : ", label_style),
                        Span::styled(status.to_string(), ok_style),
                    ],
                ];

                if let Some(c) = chassis_id {
                    lldp.push(vec![
                        Span::styled("    • Chassis ID        : ", label_style),
                        Span::styled(c.to_string(), ok_style),
                    ])
                }
                if let Some(s) = system_name {
                    lldp.push(vec![
                        Span::styled("    • System name       : ", label_style),
                        Span::styled(s.to_string(), ok_style),
                    ])
                }
                if let Some(s) = system_description {
                    lldp.push(vec![
                        Span::styled("    • System description: ", label_style),
                        Span::styled(s.to_string(), ok_style),
                    ])
                }
                if let Some(p) = port_id {
                    lldp.push(vec![
                        Span::styled("    • Port ID           : ", label_style),
                        Span::styled(p.to_string(), ok_style),
                    ])
                }
                if let Some(p) = port_description {
                    lldp.push(vec![
                        Span::styled("    • Port description  : ", label_style),
                        Span::styled(p.to_string(), ok_style),
                    ])
                }
                if let Some(addrs) = management_addrs {
                    let mut label = "    • Management addrs  : ";
                    for a in addrs {
                        lldp.push(vec![
                            Span::styled(label, label_style),
                            Span::styled(a.to_string(), ok_style),
                        ]);
                        label = "                        : ";
                    }
                }
                items.extend(lldp);
            }

            if let Some(t) = tx_eq {
                let mut tx_eq = vec![vec![Span::styled(
                    "  • TxEq port settings: ",
                    label_style,
                )]];

                if let Some(x) = t.pre1 {
                    tx_eq.push(vec![
                        Span::styled("    • Precursor 1:  ", label_style),
                        Span::styled(x.to_string(), ok_style),
                    ])
                }
                if let Some(x) = t.pre2 {
                    tx_eq.push(vec![
                        Span::styled("    • Precursor 2:  ", label_style),
                        Span::styled(x.to_string(), ok_style),
                    ])
                }
                if let Some(x) = t.main {
                    tx_eq.push(vec![
                        Span::styled("    • Main cursor:  ", label_style),
                        Span::styled(x.to_string(), ok_style),
                    ])
                }
                if let Some(x) = t.post2 {
                    tx_eq.push(vec![
                        Span::styled("    • Postcursor 2: ", label_style),
                        Span::styled(x.to_string(), ok_style),
                    ])
                }
                if let Some(x) = t.post1 {
                    tx_eq.push(vec![
                        Span::styled("    • Postcursor 1: ", label_style),
                        Span::styled(x.to_string(), ok_style),
                    ])
                }
                items.extend(tx_eq);
            }

            append_list(
                &mut spans,
                Cow::from(format!("Uplink {}: ", i + 1)),
                items,
            );
        }

        // Show BGP configuration.
        for cfg in bgp {
            let BgpConfig {
                asn,
                originate,
                // The shaper and checker are not currently used.
                shaper: _,
                checker: _,
            } = cfg;
            let mut items = vec![
                Span::styled("  • BGP config    :", label_style),
                Span::styled(" asn=", label_style),
                Span::styled(asn.to_string(), ok_style),
                Span::styled(" originate=", label_style),
            ];
            if originate.is_empty() {
                items.push(Span::styled("None", warn_style));
            } else {
                items.push(Span::styled(originate.iter().join(","), ok_style));
            }
            spans.push(Line::from(items));
        }
    } else {
        append_list(&mut spans, "Uplinks: ".into(), vec![]);
    }

    append_list(
        &mut spans,
        "NTP servers: ".into(),
        ntp_servers.iter().cloned().map(plain_list_item).collect(),
    );
    append_list(
        &mut spans,
        "DNS servers: ".into(),
        dns_servers.iter().map(|s| plain_list_item(s.to_string())).collect(),
    );
    append_list(
        &mut spans,
        "Internal services IP pool ranges: ".into(),
        internal_services_ip_pool_ranges
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
        external_dns_ips
            .iter()
            .cloned()
            .map(|ip| plain_list_item(ip.to_string()))
            .collect(),
    );

    // Add the allowlist for connecting to user-facing rack services.
    let allowed_source_ip_spans = match &allowed_source_ips {
        None | Some(AllowedSourceIps::Any) => {
            vec![plain_list_item(String::from("Any"))]
        }
        Some(AllowedSourceIps::List(list)) => list
            .iter()
            .map(|net| {
                let as_str = if net.is_host_net() {
                    net.addr().to_string()
                } else {
                    net.to_string()
                };
                plain_list_item(as_str)
            })
            .collect(),
    };
    append_list(
        &mut spans,
        "Allowed source IPs for user-facing services: ".into(),
        allowed_source_ip_spans,
    );

    append_list(
        &mut spans,
        "Sleds: ".into(),
        bootstrap_sleds
            .iter()
            .map(|desc| {
                let identifier = desc.baseboard.identifier();
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
