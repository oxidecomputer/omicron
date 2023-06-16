// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::help_text;
use super::ComputedScrollOffset;
use crate::ui::defaults::colors;
use crate::ui::defaults::style;
use crate::ui::widgets::BoxConnector;
use crate::ui::widgets::BoxConnectorKind;
use crate::Action;
use crate::Cmd;
use crate::Control;
use crate::State;
use std::borrow::Cow;
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
use wicketd_client::types::Baseboard;
use wicketd_client::types::CurrentRssUserConfig;
use wicketd_client::types::IpRange;

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
        let help = help_text(&self.help).block(block.clone());
        frame.render_widget(help, chunks[2]);

        // Make sure the top and bottom bars connect
        frame.render_widget(
            BoxConnector::new(BoxConnectorKind::Bottom),
            chunks[1],
        );
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
