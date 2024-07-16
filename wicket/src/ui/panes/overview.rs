// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::borrow::Cow;
use std::collections::BTreeMap;

use super::help_text;
use super::ComputedScrollOffset;
use super::Control;
use super::PendingScroll;
use crate::state::Component;
use crate::state::{ComponentId, ALL_COMPONENT_IDS};
use crate::ui::defaults::colors::*;
use crate::ui::defaults::style;
use crate::ui::widgets::IgnitionPopup;
use crate::ui::widgets::{BoxConnector, BoxConnectorKind, Rack};
use crate::ui::wrap::wrap_text;
use crate::{Action, Cmd, State};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph};
use ratatui::Frame;
use wicket_common::inventory::RotState;
use wicket_common::inventory::SpComponentCaboose;
use wicket_common::inventory::SpComponentInfo;
use wicket_common::inventory::SpComponentPresence;
use wicket_common::inventory::SpIgnition;
use wicket_common::inventory::SpState;

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

fn inventory_description(component: &Component) -> Text {
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

    Text::from(spans)
}

// Helper function for appending caboose details to a section of the
// inventory (used for both SP and RoT above).
fn append_caboose(
    spans: &mut Vec<Line>,
    prefix: Span<'static>,
    caboose: &SpComponentCaboose,
) {
    let SpComponentCaboose {
        board,
        git_commit,
        // Currently `name` is always the same as `board`, so we'll skip it.
        name: _,
        version,
    } = caboose;
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
    let mut version_spans =
        vec![prefix.clone(), Span::styled("Version: ", label_style)];
    version_spans.push(Span::styled(version, ok_style));
}
