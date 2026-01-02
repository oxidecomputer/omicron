// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::borrow::Cow;
use std::collections::BTreeMap;

use super::ComputedScrollOffset;
use super::Control;
use super::PendingScroll;
use super::help_text;
use crate::state::Component;
use crate::state::{ALL_COMPONENT_IDS, ComponentId};
use crate::ui::defaults::colors::*;
use crate::ui::defaults::style;
use crate::ui::widgets::IgnitionPopup;
use crate::ui::widgets::{BoxConnector, BoxConnectorKind, Rack};
use crate::ui::wrap::wrap_text;
use crate::{Action, Cmd, State};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph};
use transceiver_controller::ApplicationDescriptor;
use transceiver_controller::CmisDatapath;
use transceiver_controller::Datapath;
use transceiver_controller::PowerMode;
use transceiver_controller::PowerState;
use transceiver_controller::ReceiverPower;
use transceiver_controller::SffComplianceCode;
use transceiver_controller::VendorInfo;
use transceiver_controller::message::ExtendedStatus;
use wicket_common::inventory::RotState;
use wicket_common::inventory::SpComponentCaboose;
use wicket_common::inventory::SpComponentInfo;
use wicket_common::inventory::SpComponentPresence;
use wicket_common::inventory::SpIgnition;
use wicket_common::inventory::SpState;
use wicket_common::inventory::Transceiver;

enum PopupKind {
    Ignition,
}

/// The OverviewPane shows a rendering of the rack.
///
/// This is useful for getting a quick view of the state of the rack.
///
// TODO: Move this state into global `State` so `Recorder` snapshots work
pub struct OverviewPane {
    rack_view: RackView,
    inventory_view: InventoryView,
    rack_view_selected: bool,
}

impl OverviewPane {
    pub fn new() -> OverviewPane {
        OverviewPane {
            rack_view: RackView::default(),
            inventory_view: InventoryView::new(),
            rack_view_selected: true,
        }
    }

    pub fn dispatch(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        if self.rack_view_selected {
            self.rack_view.on(state, cmd)
        } else {
            self.inventory_view.on(state, cmd)
        }
    }
}

impl Control for OverviewPane {
    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        match cmd {
            Cmd::Enter => {
                // Transition to the inventory view `Enter` makes sense here
                // because we are entering a view of the given component.
                if self.rack_view_selected {
                    self.rack_view_selected = false;
                    Some(Action::Redraw)
                } else {
                    self.inventory_view.on(state, cmd)
                }
            }
            Cmd::Exit => {
                if self.inventory_view.popup.is_some() {
                    // If we're showing a popup, pass this event through so we
                    // can close it.
                    self.inventory_view.on(state, cmd)
                } else if !self.rack_view_selected {
                    // Otherwise, transition to the rack view. `Exit` makes
                    // sense here because we are exiting a subview of the rack.
                    self.rack_view_selected = true;
                    Some(Action::Redraw)
                } else {
                    // We're already on the rack view - there's nowhere to exit
                    // to, so this is a no-op.
                    None
                }
            }
            _ => self.dispatch(state, cmd),
        }
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: Rect,
        active: bool,
    ) {
        if self.rack_view_selected {
            self.rack_view.draw(state, frame, rect, active);
        } else {
            self.inventory_view.draw(state, frame, rect, active);
        }
    }
}

#[derive(Default)]
pub struct RackView {}

impl Control for RackView {
    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        match cmd {
            Cmd::Up => {
                state.rack_state.up();
                Some(Action::Redraw)
            }
            Cmd::Down => {
                state.rack_state.down();
                Some(Action::Redraw)
            }
            Cmd::KnightRiderMode => {
                state.rack_state.toggle_knight_rider_mode();
                Some(Action::Redraw)
            }
            Cmd::Left | Cmd::Right => {
                state.rack_state.left_or_right();
                Some(Action::Redraw)
            }
            Cmd::Tick => {
                // TODO: This only animates when the pane is active. Should we move the
                // tick into the wizard instead?
                if let Some(k) = state.rack_state.knight_rider_mode.as_mut() {
                    k.step();
                    Some(Action::Redraw)
                } else {
                    None
                }
            }
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
            .constraints([Constraint::Length(3), Constraint::Min(0)].as_ref())
            .split(rect);

        let border = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .style(border_style);

        // Draw the sled title (subview look)
        let title_bar = Paragraph::new(Line::from(vec![Span::styled(
            "OXIDE RACK",
            component_style,
        )]))
        .block(border.clone());
        frame.render_widget(title_bar, chunks[0]);

        // Draw the pane border
        let border = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .style(border_style);
        let inner = border.inner(chunks[1]);
        frame.render_widget(border, chunks[1]);

        // Draw the rack
        let rack = Rack {
            inventory: &state.inventory,
            state: &state.rack_state,
            not_present_style: Style::default()
                .bg(OX_GRAY_DARK)
                .fg(OX_OFF_WHITE),
            suspicious_style: Style::default().bg(OX_RED).fg(OX_WHITE),
            switch_style: Style::default().bg(OX_GRAY_DARK).fg(OX_WHITE),
            power_shelf_style: Style::default().bg(OX_GRAY).fg(OX_OFF_WHITE),
            sled_style: Style::default().bg(OX_GREEN_LIGHT).fg(TUI_BLACK),
            sled_selected_style: Style::default()
                .fg(TUI_BLACK)
                .bg(TUI_PURPLE_DIM),

            border_style: Style::default().fg(OX_GRAY).bg(TUI_BLACK),
            border_selected_style: Style::default()
                .fg(TUI_BLACK)
                .bg(TUI_PURPLE),

            switch_selected_style: Style::default()
                .bg(TUI_PURPLE_DIM)
                .fg(TUI_PURPLE),
            power_shelf_selected_style: Style::default()
                .bg(TUI_PURPLE_DIM)
                .fg(TUI_PURPLE),
        };

        frame.render_widget(rack, inner);
    }
}

pub struct InventoryView {
    help: Vec<(&'static str, &'static str)>,
    // Vertical offset used for scrolling
    scroll_offsets: BTreeMap<ComponentId, usize>,
    pending_scroll: Option<PendingScroll>,
    ignition: IgnitionPopup,
    popup: Option<PopupKind>,
}

impl InventoryView {
    pub fn new() -> InventoryView {
        InventoryView {
            help: vec![
                ("Rack View", "<Esc>"),
                ("Switch Component", "<Left/Right>"),
                ("Scroll", "<Up/Down>"),
                ("Ignition", "<I>"),
            ],
            scroll_offsets: ALL_COMPONENT_IDS
                .iter()
                .map(|id| (*id, 0))
                .collect(),
            pending_scroll: None,
            ignition: IgnitionPopup::default(),
            popup: None,
        }
    }

    /// Returns the wrap options that should be used in most cases for popups.
    fn default_wrap_options(width: usize) -> crate::ui::wrap::Options<'static> {
        crate::ui::wrap::Options {
            width,
            // The indent here is to add 1 character of padding.
            initial_indent: Span::raw(" "),
            subsequent_indent: Span::raw(" "),
            break_words: true,
        }
    }

    pub fn draw_ignition_popup(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
    ) {
        let full_screen = Rect {
            width: state.screen_width,
            height: state.screen_height,
            x: 0,
            y: 0,
        };

        let popup_builder =
            self.ignition.to_popup_builder(state.rack_state.selected);
        let popup = popup_builder.build(full_screen);
        frame.render_widget(popup, full_screen);
    }

    fn handle_cmd_in_popup(
        &mut self,
        state: &mut State,
        cmd: Cmd,
    ) -> Option<Action> {
        if cmd == Cmd::Exit {
            self.popup = None;
            return Some(Action::Redraw);
        }
        match self.popup.as_ref().unwrap() {
            PopupKind::Ignition => match cmd {
                Cmd::Up => {
                    self.ignition.key_up();
                    Some(Action::Redraw)
                }
                Cmd::Down => {
                    self.ignition.key_down();
                    Some(Action::Redraw)
                }
                Cmd::Enter => {
                    // Note: If making changes here, consider making them to the
                    // same arm of `UpdatePane::handle_cmd_in_popup()` in the
                    // `update` pane.
                    let command = self.ignition.selected_command();
                    let selected = state.rack_state.selected;
                    self.popup = None;
                    Some(Action::Ignition(selected, command))
                }
                _ => None,
            },
        }
    }
}

impl Control for InventoryView {
    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: Rect,
        active: bool,
    ) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    Constraint::Length(3),
                    // This includes a top border but not the bottom border.
                    Constraint::Min(0),
                    Constraint::Length(3),
                ]
                .as_ref(),
            )
            .split(rect);

        let (border_style, component_style) = if active {
            (style::selected_line(), style::selected())
        } else {
            (style::deselected(), style::deselected())
        };

        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .style(border_style);

        // Draw the sled title (subview look)
        let title_bar = Paragraph::new(Line::from(vec![
            Span::styled("OXIDE RACK / ", border_style),
            Span::styled(
                state.rack_state.selected.to_string_uppercase(),
                component_style,
            ),
        ]))
        .block(block.clone());
        frame.render_widget(title_bar, chunks[0]);

        // Draw the contents
        let contents_block = block
            .clone()
            .borders(Borders::LEFT | Borders::RIGHT | Borders::TOP);
        let inventory_style = Style::default().fg(OX_OFF_WHITE);
        let component_id = state.rack_state.selected;
        let text = match state.inventory.get_inventory(&component_id) {
            Some(inventory) => inventory_description(inventory),
            None => Text::styled("Inventory Unavailable", inventory_style),
        };
        let text = wrap_text(
            &text,
            // -2 each for borders and padding
            Self::default_wrap_options(
                chunks[1].width.saturating_sub(4).into(),
            ),
        );

        // chunks[1].height includes the top border, which means that the number
        // of lines displayed is height - 1.
        let num_lines = (chunks[1].height as usize).saturating_sub(1);

        let scroll_offset = self.scroll_offsets.get_mut(&component_id).unwrap();

        let y_offset = ComputedScrollOffset::new(
            *scroll_offset,
            text.height(),
            num_lines,
            self.pending_scroll.take(),
        )
        .into_offset();
        *scroll_offset = y_offset as usize;

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

        match self.popup {
            Some(PopupKind::Ignition) => self.draw_ignition_popup(state, frame),
            None => (),
        }
    }

    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        if self.popup.is_some() {
            return self.handle_cmd_in_popup(state, cmd);
        }

        // For Up, Down, PageUp, PageDown, GotoTop and GotoBottom, a
        // previous version of this code set the scroll offset directly.
        // Sadly that doesn't work with page up/page down because we don't
        // know the height of the viewport here.
        //
        // Instead, we now set a pending_scroll variable that is operated on
        // while the view is drawn.
        //
        // The old scheme does work with the other commands (up, down, goto
        // top and goto bottom), but use pending_scroll for all of these
        // commands for uniformity.
        if let Some(pending_scroll) = PendingScroll::from_cmd(&cmd) {
            self.pending_scroll = Some(pending_scroll);
            return Some(Action::Redraw);
        }

        match cmd {
            Cmd::Left => {
                state.rack_state.prev();
                Some(Action::Redraw)
            }
            Cmd::Right => {
                state.rack_state.next();
                Some(Action::Redraw)
            }
            Cmd::Tick => {
                // TODO: This only animates when the pane is active. Should we move the
                // tick into the [`Runner`] instead?
                if let Some(k) = state.rack_state.knight_rider_mode.as_mut() {
                    k.step();
                    Some(Action::Redraw)
                } else {
                    None
                }
            }
            Cmd::Ignition => {
                self.ignition.reset();
                self.popup = Some(PopupKind::Ignition);
                Some(Action::Redraw)
            }
            _ => None,
        }
    }
}

fn inventory_description(component: &Component) -> Text<'_> {
    let sp = component.sp();

    let label_style = style::text_label();
    let ok_style = style::text_success();
    let bad_style = style::text_failure();
    let warn_style = style::text_warning();
    let dyn_style = |ok| if ok { ok_style } else { bad_style };
    let yes_no = |val| if val { "Yes" } else { "No" };
    let bullet = || Span::styled("  • ", label_style);
    let nest_bullet = || Span::styled("      • ", label_style);

    let mut spans: Vec<Line> = Vec::new();

    // Describe ignition.
    let mut label = vec![Span::styled("Ignition: ", label_style)];
    if let Some(ignition) = sp.ignition() {
        match ignition {
            SpIgnition::No => {
                label.push(Span::styled("Not present", warn_style));
                spans.push(label.into());
            }
            SpIgnition::Yes {
                ctrl_detect_0,
                ctrl_detect_1,
                flt_a2,
                flt_a3,
                flt_rot,
                flt_sp,
                id,
                power,
            } => {
                spans.push(label.into());
                spans.push(
                    vec![
                        bullet(),
                        Span::styled("Identity: ", label_style),
                        Span::styled(format!("{id:?}"), ok_style),
                    ]
                    .into(),
                );
                spans.push(
                    vec![
                        bullet(),
                        Span::styled("Power: ", label_style),
                        Span::styled(yes_no(*power), dyn_style(*power)),
                    ]
                    .into(),
                );
                if *flt_a2 || *flt_a3 || *flt_rot || *flt_sp {
                    let mut faults =
                        vec![bullet(), Span::styled("Faults:", label_style)];
                    if *flt_a2 {
                        faults.push(Span::styled(" A2", bad_style));
                    }
                    if *flt_a3 {
                        faults.push(Span::styled(" A3", bad_style));
                    }
                    if *flt_rot {
                        faults.push(Span::styled(" RoT", bad_style));
                    }
                    if *flt_sp {
                        faults.push(Span::styled(" SP", bad_style));
                    }
                    spans.push(faults.into());
                } else {
                    spans.push(
                        vec![
                            bullet(),
                            Span::styled("Faults: ", label_style),
                            Span::styled("None", ok_style),
                        ]
                        .into(),
                    );
                }
                spans.push(
                    vec![
                        bullet(),
                        Span::styled("Controller 0 detected: ", label_style),
                        Span::styled(
                            yes_no(*ctrl_detect_0),
                            dyn_style(*ctrl_detect_0),
                        ),
                    ]
                    .into(),
                );
                spans.push(
                    vec![
                        bullet(),
                        Span::styled("Controller 1 detected: ", label_style),
                        Span::styled(
                            yes_no(*ctrl_detect_1),
                            dyn_style(*ctrl_detect_1),
                        ),
                    ]
                    .into(),
                );
            }
        }
    } else {
        label.push(Span::styled("Not available", bad_style));
        spans.push(label.into());
    }

    // blank line separator
    spans.push(Line::default());

    // Describe the SP.
    let mut label = vec![Span::styled("Service Processor: ", label_style)];
    if let Some(state) = sp.state() {
        let SpState {
            base_mac_address,
            hubris_archive_id,
            model,
            power_state,
            revision,
            serial_number,
            // We give the rot its own section below.
            rot: _,
        } = state;
        spans.push(label.into());
        spans.push(
            vec![
                bullet(),
                Span::styled("Serial number: ", label_style),
                Span::styled(serial_number.clone(), ok_style),
            ]
            .into(),
        );
        spans.push(
            vec![
                bullet(),
                Span::styled("Model: ", label_style),
                Span::styled(model.clone(), ok_style),
            ]
            .into(),
        );
        spans.push(
            vec![
                bullet(),
                Span::styled("Revision: ", label_style),
                Span::styled(revision.to_string(), ok_style),
            ]
            .into(),
        );
        spans.push(
            vec![
                bullet(),
                Span::styled("Base MAC Address: ", label_style),
                Span::styled(
                    format!(
                        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                        base_mac_address[0],
                        base_mac_address[1],
                        base_mac_address[2],
                        base_mac_address[3],
                        base_mac_address[4],
                        base_mac_address[5],
                    ),
                    ok_style,
                ),
            ]
            .into(),
        );
        spans.push(
            vec![
                bullet(),
                Span::styled("Power State: ", label_style),
                Span::styled(format!("{:?}", power_state), ok_style),
            ]
            .into(),
        );

        spans.push(
            vec![bullet(), Span::styled("Active Slot:", label_style)].into(),
        );
        spans.push(
            vec![
                nest_bullet(),
                Span::styled("Hubris Archive ID: ", label_style),
                Span::styled(hubris_archive_id.clone(), ok_style),
            ]
            .into(),
        );

        if let Some(caboose) = sp.caboose_active() {
            append_caboose(&mut spans, nest_bullet(), caboose);
        } else {
            spans.push(
                vec![
                    nest_bullet(),
                    Span::styled("No further information", warn_style),
                ]
                .into(),
            );
        }

        spans.push(
            vec![bullet(), Span::styled("Inactive Slot:", label_style)].into(),
        );
        if let Some(caboose) = sp.caboose_inactive() {
            append_caboose(&mut spans, nest_bullet(), caboose);
        } else {
            spans.push(
                vec![nest_bullet(), Span::styled("No information", warn_style)]
                    .into(),
            );
        }
    } else {
        label.push(Span::styled("Not available", bad_style));
        spans.push(label.into());
    }

    // blank line separator
    spans.push(Line::default());

    // Describe the RoT.
    let mut label = vec![Span::styled("Root of Trust: ", label_style)];
    if let Some(rot) = sp.state().map(|sp| &sp.rot) {
        match rot {
            RotState::V3 {
                active,
                pending_persistent_boot_preference,
                persistent_boot_preference,
                slot_a_fwid,
                slot_b_fwid,
                stage0_fwid,
                stage0next_fwid,
                transient_boot_preference,
                slot_a_error,
                slot_b_error,
                stage0_error,
                stage0next_error,
            } => {
                spans.push(label.into());
                spans.push(
                    vec![
                        bullet(),
                        Span::styled("Active Slot: ", label_style),
                        Span::styled(format!("{active:?}"), ok_style),
                    ]
                    .into(),
                );
                spans.push(
                    vec![
                        bullet(),
                        Span::styled(
                            "Persistent Boot Preference: ",
                            label_style,
                        ),
                        Span::styled(
                            format!("{persistent_boot_preference:?}"),
                            ok_style,
                        ),
                    ]
                    .into(),
                );
                spans.push(
                    vec![
                        bullet(),
                        Span::styled(
                            "Pending Persistent Boot Preference: ",
                            label_style,
                        ),
                        Span::styled(
                            match pending_persistent_boot_preference.as_ref() {
                                Some(pref) => Cow::from(format!("{pref:?}")),
                                None => Cow::from("None"),
                            },
                            ok_style,
                        ),
                    ]
                    .into(),
                );
                spans.push(
                    vec![
                        bullet(),
                        Span::styled(
                            "Transient Boot Preference: ",
                            label_style,
                        ),
                        Span::styled(
                            match transient_boot_preference.as_ref() {
                                Some(pref) => Cow::from(format!("{pref:?}")),
                                None => Cow::from("None"),
                            },
                            ok_style,
                        ),
                    ]
                    .into(),
                );
                spans.push(
                    vec![bullet(), Span::styled("Slot A:", label_style)].into(),
                );
                spans.push(
                    vec![
                        nest_bullet(),
                        Span::styled("Image SHA3-256: ", label_style),
                        Span::styled(slot_a_fwid.clone(), ok_style),
                    ]
                    .into(),
                );
                if let Some(caboose) =
                    sp.rot().and_then(|r| r.caboose_a.as_ref())
                {
                    append_caboose(&mut spans, nest_bullet(), caboose);
                } else {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("No further information", warn_style),
                        ]
                        .into(),
                    );
                }
                if let Some(e) = slot_a_error {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("Image status: ", label_style),
                            Span::styled(format!("Error: {e:?}"), bad_style),
                        ]
                        .into(),
                    );
                } else {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("Image status: ", label_style),
                            Span::styled("Status Good", ok_style),
                        ]
                        .into(),
                    );
                }
                spans.push(
                    vec![bullet(), Span::styled("Slot B:", label_style)].into(),
                );
                spans.push(
                    vec![
                        nest_bullet(),
                        Span::styled("Image SHA3-256: ", label_style),
                        Span::styled(slot_b_fwid.clone(), ok_style),
                    ]
                    .into(),
                );
                if let Some(caboose) =
                    sp.rot().and_then(|r| r.caboose_b.as_ref())
                {
                    append_caboose(&mut spans, nest_bullet(), caboose);
                } else {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("No further information", warn_style),
                        ]
                        .into(),
                    );
                }
                if let Some(e) = slot_b_error {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("Image status: ", label_style),
                            Span::styled(format!("Error: {e:?}"), bad_style),
                        ]
                        .into(),
                    );
                } else {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("Image status: ", label_style),
                            Span::styled("Status Good", ok_style),
                        ]
                        .into(),
                    );
                }

                spans.push(
                    vec![bullet(), Span::styled("Stage0:", label_style)].into(),
                );
                spans.push(
                    vec![
                        nest_bullet(),
                        Span::styled("Image SHA3-256: ", label_style),
                        Span::styled(stage0_fwid.clone(), ok_style),
                    ]
                    .into(),
                );
                if let Some(caboose) = sp.rot().and_then(|r| {
                    r.caboose_stage0.as_ref().map_or(None, |x| x.as_ref())
                }) {
                    append_caboose(&mut spans, nest_bullet(), caboose);
                } else {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("No further information", warn_style),
                        ]
                        .into(),
                    );
                }
                if let Some(e) = stage0_error {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("Image status: ", label_style),
                            Span::styled(format!("Error: {e:?}"), bad_style),
                        ]
                        .into(),
                    );
                } else {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("Image status: ", label_style),
                            Span::styled("Status Good", ok_style),
                        ]
                        .into(),
                    );
                }

                spans.push(
                    vec![bullet(), Span::styled("Stage0Next:", label_style)]
                        .into(),
                );
                spans.push(
                    vec![
                        nest_bullet(),
                        Span::styled("Image SHA3-256: ", label_style),
                        Span::styled(stage0next_fwid.clone(), ok_style),
                    ]
                    .into(),
                );
                if let Some(caboose) = sp.rot().and_then(|r| {
                    r.caboose_stage0next.as_ref().map_or(None, |x| x.as_ref())
                }) {
                    append_caboose(&mut spans, nest_bullet(), caboose);
                } else {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("No further information", warn_style),
                        ]
                        .into(),
                    );
                }
                if let Some(e) = stage0next_error {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("Image status: ", label_style),
                            Span::styled(format!("Error: {e:?}"), bad_style),
                        ]
                        .into(),
                    );
                } else {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("Image status: ", label_style),
                            Span::styled("Status Good", ok_style),
                        ]
                        .into(),
                    );
                }
            }

            RotState::V2 {
                active,
                pending_persistent_boot_preference,
                persistent_boot_preference,
                slot_a_sha3_256_digest,
                slot_b_sha3_256_digest,
                transient_boot_preference,
            } => {
                spans.push(label.into());
                spans.push(
                    vec![
                        bullet(),
                        Span::styled("Active Slot: ", label_style),
                        Span::styled(format!("{active:?}"), ok_style),
                    ]
                    .into(),
                );
                spans.push(
                    vec![
                        bullet(),
                        Span::styled(
                            "Persistent Boot Preference: ",
                            label_style,
                        ),
                        Span::styled(
                            format!("{persistent_boot_preference:?}"),
                            ok_style,
                        ),
                    ]
                    .into(),
                );
                spans.push(
                    vec![
                        bullet(),
                        Span::styled(
                            "Pending Persistent Boot Preference: ",
                            label_style,
                        ),
                        Span::styled(
                            match pending_persistent_boot_preference.as_ref() {
                                Some(pref) => Cow::from(format!("{pref:?}")),
                                None => Cow::from("None"),
                            },
                            ok_style,
                        ),
                    ]
                    .into(),
                );
                spans.push(
                    vec![
                        bullet(),
                        Span::styled(
                            "Transient Boot Preference: ",
                            label_style,
                        ),
                        Span::styled(
                            match transient_boot_preference.as_ref() {
                                Some(pref) => Cow::from(format!("{pref:?}")),
                                None => Cow::from("None"),
                            },
                            ok_style,
                        ),
                    ]
                    .into(),
                );
                spans.push(
                    vec![bullet(), Span::styled("Slot A:", label_style)].into(),
                );
                spans.push(
                    vec![
                        nest_bullet(),
                        Span::styled("Image SHA3-256: ", label_style),
                        match slot_a_sha3_256_digest.as_ref() {
                            Some(digest) => {
                                Span::styled(digest.clone(), ok_style)
                            }
                            None => Span::styled("Unknown", warn_style),
                        },
                    ]
                    .into(),
                );
                if let Some(caboose) =
                    sp.rot().and_then(|r| r.caboose_a.as_ref())
                {
                    append_caboose(&mut spans, nest_bullet(), caboose);
                } else {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("No further information", warn_style),
                        ]
                        .into(),
                    );
                }
                spans.push(
                    vec![bullet(), Span::styled("Slot B:", label_style)].into(),
                );
                spans.push(
                    vec![
                        nest_bullet(),
                        Span::styled("Image SHA3-256: ", label_style),
                        match slot_b_sha3_256_digest.as_ref() {
                            Some(digest) => {
                                Span::styled(digest.clone(), ok_style)
                            }
                            None => Span::styled("Unknown", warn_style),
                        },
                    ]
                    .into(),
                );
                if let Some(caboose) =
                    sp.rot().and_then(|r| r.caboose_b.as_ref())
                {
                    append_caboose(&mut spans, nest_bullet(), caboose);
                } else {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled("No further information", warn_style),
                        ]
                        .into(),
                    );
                }
            }
            RotState::CommunicationFailed { message } => {
                spans.push(label.into());
                spans.push(
                    vec![
                        bullet(),
                        Span::styled(
                            format!("Communication Failure: {message}"),
                            bad_style,
                        ),
                    ]
                    .into(),
                );
            }
        }
    } else {
        label.push(Span::styled("Not available", bad_style));
        spans.push(label.into());
    }

    // blank line separator
    spans.push(Line::default());

    // Describe all components.
    // TODO-correctness: component information will change with the IPCC /
    // topology work that is ongoing; show our current stand-in for now.
    let mut label = vec![Span::styled("Components: ", label_style)];
    let components = sp.components();
    if components.is_empty() {
        label.push(Span::styled("Not available", bad_style));
        spans.push(label.into());
    } else {
        spans.push(label.into());
        for SpComponentInfo {
            description,
            device,
            presence,
            // `cababilities` and `component` are internal-use only and not
            // meaningful to an end user
            capabilities: _,
            component: _,
            // serial number is currently always unpopulated
            serial_number: _,
        } in components
        {
            spans.push(
                vec![
                    bullet(),
                    Span::styled(
                        format!("{:?}", presence),
                        match presence {
                            SpComponentPresence::Present => ok_style,
                            SpComponentPresence::Timeout
                            | SpComponentPresence::Unavailable => warn_style,
                            SpComponentPresence::NotPresent
                            | SpComponentPresence::Failed
                            | SpComponentPresence::Error => bad_style,
                        },
                    ),
                    Span::styled(
                        format!(" {} ({})", description, device),
                        label_style,
                    ),
                ]
                .into(),
            );
        }
    }

    // If this is a switch, describe any transceivers.
    if let Component::Switch { transceivers, .. } = component {
        // blank line separator
        spans.push(Line::default());

        let mut label = vec![Span::styled("Transceivers: ", label_style)];
        if transceivers.is_empty() {
            label.push(Span::styled("None", warn_style));
            spans.push(label.into());
        } else {
            spans.push(label.into());
            for transceiver in transceivers {
                // Top-level bullet for the port itself. We're not sure what
                // details we can print about the transceiver yet.
                spans.push(
                    vec![
                        bullet(),
                        Span::styled(&transceiver.port, label_style),
                    ]
                    .into(),
                );

                // Now print as much of the details for this transceiver as we
                // can, starting with the vendor name.
                let vendor_details =
                    extract_vendor_details(&transceiver.vendor);
                for detail in vendor_details {
                    spans.push(
                        vec![
                            nest_bullet(),
                            Span::styled(detail.label, label_style),
                            Span::styled(detail.detail, detail.style),
                        ]
                        .into(),
                    );
                }

                let (media_type_style, media_type) =
                    extract_media_type(&transceiver.datapath);
                spans.push(
                    vec![
                        nest_bullet(),
                        Span::styled("Media type: ", label_style),
                        Span::styled(media_type, media_type_style),
                    ]
                    .into(),
                );

                let (fpga_power_style, fpga_power) =
                    format_transceiver_status(&transceiver.status);
                spans.push(
                    vec![
                        nest_bullet(),
                        Span::styled("Power at FPGA: ", label_style),
                        Span::styled(fpga_power, fpga_power_style),
                    ]
                    .into(),
                );

                let (module_power_style, module_power) =
                    format_transceiver_power_state(&transceiver.power);
                spans.push(
                    vec![
                        nest_bullet(),
                        Span::styled("Power at module: ", label_style),
                        Span::styled(module_power, module_power_style),
                    ]
                    .into(),
                );

                let (temp_style, temp) = format_transceiver_temperature(
                    transceiver.monitors.as_ref().map(|m| &m.temperature),
                );
                spans.push(
                    vec![
                        nest_bullet(),
                        Span::styled("Temperature: ", label_style),
                        Span::styled(temp, temp_style),
                    ]
                    .into(),
                );

                let (voltage_style, voltage) = format_transceiver_voltage(
                    transceiver.monitors.as_ref().map(|m| &m.supply_voltage),
                );
                spans.push(
                    vec![
                        nest_bullet(),
                        Span::styled("Voltage: ", label_style),
                        Span::styled(voltage, voltage_style),
                    ]
                    .into(),
                );

                let n_lanes = n_expected_lanes(transceiver);
                let mut line = vec![nest_bullet()];
                line.extend(format_transceiver_receive_power(
                    n_lanes,
                    transceiver
                        .monitors
                        .as_ref()
                        .map(|m| m.receiver_power.as_deref()),
                ));
                spans.push(line.into());

                let mut line = vec![nest_bullet()];
                line.extend(format_transceiver_transmit_power(
                    n_lanes,
                    transceiver
                        .monitors
                        .as_ref()
                        .map(|m| m.transmitter_power.as_deref()),
                ));
                spans.push(line.into());
            }
        }
    };

    Text::from(spans)
}

struct VendorLine {
    label: String,
    detail: String,
    style: Style,
}

fn extract_vendor_details(
    vendor: &Result<VendorInfo, String>,
) -> Vec<VendorLine> {
    let mut out = Vec::with_capacity(4);
    match vendor {
        Ok(vendor) => {
            out.push(VendorLine {
                label: "Vendor: ".to_string(),
                detail: vendor.vendor.name.clone(),
                style: style::text_success(),
            });
            out.push(VendorLine {
                label: "Model: ".to_string(),
                detail: vendor.vendor.part.clone(),
                style: style::text_success(),
            });
            out.push(VendorLine {
                label: "Serial: ".to_string(),
                detail: vendor.vendor.serial.clone(),
                style: style::text_success(),
            });
            out.push(VendorLine {
                label: "Management interface: ".to_string(),
                detail: vendor.identifier.to_string(),
                style: style::text_success(),
            });
        }
        Err(e) => {
            for label in
                ["Vendor: ", "Model: ", "Serial: ", "Management interface: "]
            {
                out.push(VendorLine {
                    label: label.to_string(),
                    detail: e.clone(),
                    style: style::text_failure(),
                })
            }
        }
    }
    out
}

// Helper function for appending caboose details to a section of the
// inventory (used for both SP and RoT above).
fn append_caboose(
    spans: &mut Vec<Line>,
    prefix: Span<'static>,
    caboose: &SpComponentCaboose,
) {
    let SpComponentCaboose { board, git_commit, name, sign, version, epoch } =
        caboose;
    let label_style = style::text_label();
    let ok_style = style::text_success();

    spans.push(
        vec![
            prefix.clone(),
            Span::styled("Git Commit: ", label_style),
            Span::styled(git_commit.clone(), ok_style),
        ]
        .into(),
    );
    spans.push(
        vec![
            prefix.clone(),
            Span::styled("Board: ", label_style),
            Span::styled(board.clone(), ok_style),
        ]
        .into(),
    );
    spans.push(
        vec![
            prefix.clone(),
            Span::styled("Name: ", label_style),
            Span::styled(name.clone(), ok_style),
        ]
        .into(),
    );
    if let Some(s) = sign {
        spans.push(
            vec![
                prefix.clone(),
                Span::styled("Sign Hash: ", label_style),
                Span::styled(s.clone(), ok_style),
            ]
            .into(),
        );
    }
    if let Some(s) = epoch {
        spans.push(
            vec![
                prefix.clone(),
                Span::styled("Epoch: ", label_style),
                Span::styled(s.clone(), ok_style),
            ]
            .into(),
        );
    }

    let mut version_spans =
        vec![prefix.clone(), Span::styled("Version: ", label_style)];
    version_spans.push(Span::styled(version, ok_style));
}

fn unsupported_ui_elements() -> (Style, String) {
    (style::text_dim(), String::from("Unsupported"))
}

// Print relevant transceiver status bits.
//
// We know the transceiver is present by construction, so print the power state
// and any faults.
fn format_transceiver_status(
    status: &Result<ExtendedStatus, String>,
) -> (Style, String) {
    match status {
        Ok(status) => {
            if status.contains(ExtendedStatus::ENABLED) {
                if status.contains(ExtendedStatus::POWER_GOOD) {
                    return (style::text_success(), String::from("Enabled"));
                }
                let message = if status
                    .contains(ExtendedStatus::FAULT_POWER_TIMEOUT)
                {
                    String::from("Timeout fault")
                } else if status.contains(ExtendedStatus::FAULT_POWER_LOST) {
                    String::from("Power lost")
                } else if status.contains(ExtendedStatus::DISABLED_BY_SP) {
                    String::from("Disabled by SP")
                } else {
                    format!("Unknown: {}", status)
                };
                (style::text_failure(), message)
            } else {
                (style::text_warning(), String::from("Disabled"))
            }
        }
        Err(e) => (style::text_failure(), e.clone()),
    }
}

fn format_transceiver_power_state(
    power: &Result<PowerMode, String>,
) -> (Style, String) {
    match power {
        Ok(power) => {
            let style = match power.state {
                PowerState::Off => style::text_warning(),
                PowerState::Low => style::text_warning(),
                PowerState::High => style::text_success(),
            };
            (
                style,
                format!(
                    "{}{}",
                    power.state,
                    // "Software override" means that the module's power is controlled
                    // by writing to a specific register, rather than through a hardware
                    // signal / pin.
                    if matches!(power.software_override, Some(true)) {
                        " (Software control)"
                    } else {
                        ""
                    }
                ),
            )
        }
        Err(e) => (style::text_failure(), e.clone()),
    }
}

/// Format the transceiver temperature.
fn format_transceiver_temperature(
    maybe_temp: Result<&Option<f32>, &String>,
) -> (Style, String) {
    let t = match maybe_temp {
        Ok(Some(t)) => *t,
        Ok(None) => return unsupported_ui_elements(),
        Err(e) => return (style::text_failure(), e.clone()),
    };
    const MIN_WARNING_TEMP: f32 = 15.0;
    const MAX_WARNING_TEMP: f32 = 50.0;
    let temp = format!("{t:0.2} °C");
    let style = if t < MIN_WARNING_TEMP || t > MAX_WARNING_TEMP {
        style::text_warning()
    } else {
        style::text_success()
    };
    (style, temp)
}

/// Format the transceiver voltage.
fn format_transceiver_voltage(
    maybe_voltage: Result<&Option<f32>, &String>,
) -> (Style, String) {
    let v = match maybe_voltage {
        Ok(Some(v)) => *v,
        Ok(None) => return unsupported_ui_elements(),
        Err(e) => return (style::text_failure(), e.clone()),
    };
    const MIN_WARNING_VOLTAGE: f32 = 3.0;
    const MAX_WARNING_VOLTAGE: f32 = 3.7;
    let voltage = format!("{v:0.2} V");
    let style = if v < MIN_WARNING_VOLTAGE || v > MAX_WARNING_VOLTAGE {
        style::text_warning()
    } else {
        style::text_success()
    };
    (style, voltage)
}

/// Format the transceiver received optical power.
fn format_transceiver_receive_power<'a>(
    n_lanes: Option<usize>,
    receiver_power: Result<Option<&'a [ReceiverPower]>, &'a String>,
) -> Vec<Span<'a>> {
    let pow = match receiver_power {
        Ok(Some(p)) if !p.is_empty() => p,
        // Either not supported at all, or list of power is empty.
        Ok(None) | Ok(Some(_)) => {
            let elems = unsupported_ui_elements();
            return vec![
                Span::styled("Rx power: ", style::text_label()),
                Span::styled(elems.1, elems.0),
            ];
        }
        // Failed to read entirely
        Err(e) => {
            return vec![
                Span::styled("Rx power: ", style::text_label()),
                Span::styled(e.clone(), style::text_failure()),
            ];
        }
    };

    const MIN_WARNING_POWER: f32 = 0.5;
    const MAX_WARNING_POWER: f32 = 2.5;
    assert!(!pow.is_empty());
    let mut out = Vec::with_capacity(pow.len() + 2);

    // Push the label itself.
    let kind = if matches!(&pow[0], ReceiverPower::Average(_)) {
        "Avg"
    } else {
        "Peak-to-peak"
    };
    let label = format!("Rx power (mW, {}): [", kind);
    out.push(Span::styled(label, style::text_label()));

    // Push each Rx power measurement, styling it if it's above the
    // limit.
    let n_lanes = n_lanes.unwrap_or(pow.len());
    for (lane, meas) in pow[..n_lanes].iter().enumerate() {
        let style = if meas.value() < MIN_WARNING_POWER
            || meas.value() > MAX_WARNING_POWER
        {
            style::text_warning()
        } else {
            style::text_success()
        };
        let measurement = format!("{:0.3}", meas.value());
        out.push(Span::styled(measurement, style));
        if lane < n_lanes - 1 {
            out.push(Span::styled(", ", style::text_label()));
        }
    }
    out.push(Span::styled("]", style::text_label()));
    out
}

/// Format the transceiver transmitted optical power.
fn format_transceiver_transmit_power<'a>(
    n_lanes: Option<usize>,
    transmitter_power: Result<Option<&'a [f32]>, &'a String>,
) -> Vec<Span<'a>> {
    let pow = match transmitter_power {
        Ok(Some(p)) if !p.is_empty() => p,
        // Either not supported at all, or list of power is empty.
        Ok(None) | Ok(Some(_)) => {
            let elems = unsupported_ui_elements();
            return vec![
                Span::styled("Tx power: ", style::text_label()),
                Span::styled(elems.1, elems.0),
            ];
        }
        // Failed to read entirely
        Err(e) => {
            return vec![
                Span::styled("Tx power: ", style::text_label()),
                Span::styled(e.clone(), style::text_failure()),
            ];
        }
    };

    const MIN_WARNING_POWER: f32 = 0.5;
    const MAX_WARNING_POWER: f32 = 2.5;
    assert!(!pow.is_empty());
    let mut out = Vec::with_capacity(pow.len() + 2);
    out.push(Span::styled("Tx power (mW): [", style::text_label()));
    let n_lanes = n_lanes.unwrap_or(pow.len());
    for (lane, meas) in pow[..n_lanes].iter().enumerate() {
        let style = if *meas < MIN_WARNING_POWER || *meas > MAX_WARNING_POWER {
            style::text_warning()
        } else {
            style::text_success()
        };
        let measurement = format!("{:0.3}", meas);
        out.push(Span::styled(measurement, style));
        if lane < n_lanes - 1 {
            out.push(Span::styled(", ", style::text_label()));
        }
    }
    out.push(Span::styled("]", style::text_label()));
    out
}

fn extract_media_type(datapath: &Result<Datapath, String>) -> (Style, String) {
    match datapath {
        Ok(datapath) => match datapath {
            Datapath::Cmis { datapaths, .. } => {
                let Some(media_type) = datapaths.values().next().map(|p| {
                    let CmisDatapath { application, .. } = p;
                    let ApplicationDescriptor { media_id, .. } = application;
                    media_id.to_string()
                }) else {
                    return (style::text_warning(), String::from("Unknown"));
                };
                (style::text_success(), media_type)
            }
            Datapath::Sff8636 { specification, .. } => {
                (style::text_success(), specification.to_string())
            }
        },
        Err(e) => (style::text_failure(), e.clone()),
    }
}

/// Return the number of expected media lanes in the transceiver.
///
/// If we aren't sure, return `None`.
fn n_expected_lanes(tr: &Transceiver) -> Option<usize> {
    let Ok(datapath) = &tr.datapath else {
        return None;
    };
    match datapath {
        Datapath::Cmis { datapaths, .. } => datapaths
            .values()
            .next()
            .map(|CmisDatapath { lane_status, .. }| lane_status.len())
            .or(Some(4)),
        Datapath::Sff8636 { specification, .. } => match specification {
            SffComplianceCode::Extended(code) => {
                use transceiver_controller::ExtendedSpecificationComplianceCode::*;
                match code {
                    Id100GBaseSr4 => Some(4),
                    Id100GBaseLr4 => Some(4),
                    Id100GBCwdm4 => Some(4),
                    Id100GBaseCr4 => Some(4),
                    Id100GSwdm4 => Some(4),
                    Id100GBaseFr1 => Some(1),
                    Id100GBaseLr1 => Some(1),
                    Id200GBaseFr4 => Some(4),
                    Id200GBaseLr4 => Some(4),
                    Id400GBaseDr4 => Some(4),
                    Id100GPam4BiDi => Some(2),
                    Unspecified | Id100GAoc5en5 | Id100GBaseEr4
                    | Id100GBaseSr10 | Id100GPsm4 | Id100GAcc | Obsolete
                    | Id25GBaseCrS | Id25GBaseCrN | Id10MbEth
                    | Id40GBaseEr4 | Id4x10GBaseSr | Id40GPsm4
                    | IdG959p1i12d1 | IdG959p1s12d2 | IdG9592p1l1d1
                    | Id10GBaseT | Id100GClr4 | Id100GAoc10en12
                    | Id100GAcc10en12 | Id100GeDwdm2 | Id100GWdm
                    | Id10GBaseTSr | Id5GBaseT | Id2p5GBaseT | Id40GSwdm4
                    | Id10GBaseBr | Id25GBaseBr | Id50GBaseBr | Id4wdm10
                    | Id4wdm20 | Id4wdm40 | Id100GBaseDr | Id100GFr
                    | Id100GLr | Id100GBaseSr1 | Id100GBaseVr1
                    | Id100GBaseSr12 | Id100GBaseVr12 | Id100GLr120Caui4
                    | Id100GLr130Caui4 | Id100GLr140Caui4 | Id100GLr120
                    | Id100GLr130 | Id100GLr140 | IdAcc50GAUI10en6
                    | IdAcc50GAUI10en62 | IdAcc50GAUI2p6en4
                    | IdAcc50GAUI2p6en41 | Id100GBaseCr1 | Id50GBaseCr
                    | Id50GBaseSr | Id50GBaseFr | Id50GBaseEr | Id200GPsm4
                    | Id50GBaseLr | Id400GBaseFr4 | Id400GBaseLr4
                    | Id400GGLr410 | Id400GBaseZr | Id256GfcSw4 | Id64Gfc
                    | Id128Gfc | Reserved(_) => None,
                }
            }
            SffComplianceCode::Ethernet(_) => None,
        },
    }
}
