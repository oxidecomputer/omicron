// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use super::{align_by, help_text, Control};
use crate::state::{artifact_title, ComponentId, Inventory, ALL_COMPONENT_IDS};
use crate::ui::defaults::style;
use crate::ui::widgets::{BoxConnector, BoxConnectorKind, ButtonText, Popup};
use crate::{Action, Cmd, Frame, State};
use omicron_common::api::internal::nexus::KnownArtifactKind;
use slog::{info, o, Logger};
use tui::layout::{Constraint, Direction, Layout, Rect};
use tui::text::{Span, Spans, Text};
use tui::widgets::{Block, BorderType, Borders, Paragraph};
use tui_tree_widget::{Tree, TreeItem, TreeState};
use wicketd_client::types::{SemverVersion, UpdateState};

const MAX_COLUMN_WIDTH: u16 = 25;

enum PopupKind {
    MissingRepo,
    StartUpdate,
    Logs,
}

/// Overview of update status and ability to install updates
/// from a single TUF repo uploaded to wicketd via wicket.
pub struct UpdatePane {
    #[allow(unused)]
    log: Logger,
    tree_state: TreeState,
    items: Vec<TreeItem<'static>>,
    help: Vec<(&'static str, &'static str)>,
    rect: Rect,
    // TODO: These will likely move into a status view, because there will be
    // other update views/tabs
    title_rect: Rect,
    table_headers_rect: Rect,
    contents_rect: Rect,
    help_rect: Rect,
    popup: Option<PopupKind>,
}

impl UpdatePane {
    pub fn new(log: &Logger) -> UpdatePane {
        let log = log.new(o!("component" => "UpdatePane"));
        let mut tree_state = TreeState::default();
        tree_state.select_first();
        UpdatePane {
            log,
            tree_state,
            items: ALL_COMPONENT_IDS
                .iter()
                .map(|id| TreeItem::new(*id, vec![]))
                .collect(),
            help: vec![
                ("Expand", "<e>"),
                ("Collapse", "<c>"),
                ("Move", "<Up/Down>"),
                ("Details", "<d>"),
                ("Update", "<Enter>"),
            ],
            rect: Rect::default(),
            title_rect: Rect::default(),
            table_headers_rect: Rect::default(),
            contents_rect: Rect::default(),
            help_rect: Rect::default(),
            popup: None,
        }
    }

    pub fn draw_log_popup(&mut self, state: &State, frame: &mut Frame<'_>) {
        let selected = state.rack_state.selected;
        let logs = state.update_state.logs.get(&selected).map_or_else(
            || "No Logs Available".to_string(),
            |l| format!("{:#?}", l),
        );

        let popup = Popup {
            header: Text::from(vec![Spans::from(vec![Span::styled(
                format!(" UPDATE LOGS: {}", selected.to_string()),
                style::header(true),
            )])]),
            body: Text::styled(logs, style::plain_text()),
            buttons: vec![
                ButtonText { instruction: "CLOSE", key: "ESC" },
                ButtonText { instruction: "SCROLL", key: "UP/DOWN" },
            ],
        };
        let full_screen = Rect {
            width: state.screen_width,
            height: state.screen_height,
            x: 0,
            y: 0,
        };
        frame.render_widget(popup, full_screen);
    }

    pub fn draw_start_update_popup(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
    ) {
        let popup = Popup {
            header: Text::from(vec![Spans::from(vec![Span::styled(
                format!(
                    " START UPDATE: {}",
                    state.rack_state.selected.to_string()
                ),
                style::header(true),
            )])]),
            body: Text::from(vec![Spans::from(vec![Span::styled(
                " Would you like to start an update?",
                style::plain_text(),
            )])]),
            buttons: vec![
                ButtonText { instruction: "YES", key: "Y" },
                ButtonText { instruction: "NO", key: "N" },
            ],
        };
        let full_screen = Rect {
            width: state.screen_width,
            height: state.screen_height,
            x: 0,
            y: 0,
        };
        frame.render_widget(popup, full_screen);
    }

    pub fn draw_update_missing_popup(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
    ) {
        let popup = Popup {
            header: Text::from(vec![Spans::from(vec![Span::styled(
                " MISSING UPDATE BUNDLE",
                style::header(true),
            )])]),
            body: Text::from(vec![
                Spans::from(vec![Span::styled(
                    " Use the following command to transfer an update: ",
                    style::plain_text(),
                )]),
                "".into(),
                Spans::from(vec![
                    Span::styled(" cat", style::plain_text()),
                    Span::styled(" $UPDATE", style::popup_highlight()),
                    Span::styled(".zip | ssh", style::plain_text()),
                    Span::styled(" $IPV6_ADDRESS", style::popup_highlight()),
                    Span::styled(" upload", style::plain_text()),
                ]),
            ]),
            buttons: vec![ButtonText { instruction: "CLOSE", key: "ESC" }],
        };
        let full_screen = Rect {
            width: state.screen_width,
            height: state.screen_height,
            x: 0,
            y: 0,
        };
        frame.render_widget(popup, full_screen);
    }

    fn update_items(&mut self, state: &State) {
        let versions = state.update_state.artifact_versions.clone();
        let inventory = &state.inventory;

        self.items = state
            .update_state
            .items
            .iter()
            .map(|(id, states)| {
                let children: Vec<_> = states
                    .iter()
                    .map(|(artifact, s)| {
                        let target_version =
                            artifact_version(artifact, &versions);
                        let installed_version =
                            installed_version(id, artifact, inventory);
                        let spans = vec![
                            Span::styled(
                                artifact_title(*artifact),
                                style::selected(),
                            ),
                            Span::styled(
                                installed_version,
                                style::selected_line(),
                            ),
                            Span::styled(target_version, style::selected()),
                            Span::styled(s.to_string(), s.style()),
                        ];
                        TreeItem::new_leaf(align_by(
                            0,
                            MAX_COLUMN_WIDTH,
                            self.contents_rect,
                            spans,
                        ))
                    })
                    .collect();
                TreeItem::new(*id, children)
            })
            .collect();
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
            PopupKind::Logs => None,
            PopupKind::MissingRepo => None,
            PopupKind::StartUpdate => {
                match cmd {
                    Cmd::Yes => {
                        // Trigger the update
                        let selected = state.rack_state.selected;
                        info!(self.log, "Updating {}", selected);
                        self.popup = None;
                        Some(Action::Update(selected))
                    }
                    Cmd::No => {
                        self.popup = None;
                        Some(Action::Redraw)
                    }
                    _ => None,
                }
            }
        }
    }

    fn open_popup(&mut self, state: &mut State) {
        if state.update_state.artifacts.is_empty() {
            self.popup = Some(PopupKind::MissingRepo);
        } else {
            self.popup = Some(PopupKind::StartUpdate);
        }
    }

    // When we switch panes, we may have moved around in the rack. We want to
    // ensure that the currently selected rack component in the  update tree
    // matches what was selected in the rack or inventory views. We already do
    // the converse when on this pane and move around the tree.
    fn ensure_selection_matches_rack_state(&mut self, state: &State) {
        let selected = self.tree_state.selected();
        if state.rack_state.selected != ALL_COMPONENT_IDS[selected[0]] {
            let index = ALL_COMPONENT_IDS
                .iter()
                .position(|&id| id == state.rack_state.selected)
                .unwrap();
            self.tree_state.select(vec![index]);
        }
    }
}

fn installed_version(
    id: &ComponentId,
    artifact: &KnownArtifactKind,
    inventory: &Inventory,
) -> String {
    use KnownArtifactKind::*;
    let component = inventory.get_inventory(id);
    match artifact {
        GimletSp | PscSp | SwitchSp => component.map_or_else(
            || "UNKNOWN".to_string(),
            |component| component.sp_version(),
        ),
        GimletRot | PscRot | SwitchRot => component.map_or_else(
            || "UNKNOWN".to_string(),
            |component| component.rot_version(),
        ),
        _ => "UNKNOWN".to_string(),
    }
}

fn artifact_version(
    artifact: &KnownArtifactKind,
    versions: &BTreeMap<KnownArtifactKind, SemverVersion>,
) -> String {
    versions
        .get(artifact)
        .cloned()
        .map_or_else(|| "UNKNOWN".to_string(), |v| v.to_string())
}

impl Control for UpdatePane {
    fn is_modal_active(&self) -> bool {
        self.popup.is_some()
    }

    fn resize(&mut self, state: &mut State, rect: Rect) {
        self.rect = rect;
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
        self.title_rect = chunks[0];
        self.table_headers_rect = chunks[1];
        self.contents_rect = chunks[2];
        self.help_rect = chunks[3];

        self.update_items(state);
    }

    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        self.ensure_selection_matches_rack_state(state);
        if self.popup.is_some() {
            return self.handle_cmd_in_popup(state, cmd);
        }
        match cmd {
            Cmd::Up => {
                self.tree_state.key_up(&self.items);
                let selected = self.tree_state.selected();
                state.rack_state.selected = ALL_COMPONENT_IDS[selected[0]];
                Some(Action::Redraw)
            }
            Cmd::Down => {
                self.tree_state.key_down(&self.items);
                let selected = self.tree_state.selected();
                state.rack_state.selected = ALL_COMPONENT_IDS[selected[0]];
                Some(Action::Redraw)
            }
            Cmd::Collapse | Cmd::Left => {
                // We always want something selected. If we close the root,
                // we want to re-open it.
                let selected = self.tree_state.selected();
                self.tree_state.key_left();
                if self.tree_state.selected().is_empty() {
                    self.tree_state.select(selected);
                    None
                } else {
                    Some(Action::Redraw)
                }
            }
            Cmd::Expand | Cmd::Right => {
                self.tree_state.key_right();
                Some(Action::Redraw)
            }
            Cmd::Enter => {
                self.open_popup(state);
                Some(Action::Redraw)
            }
            Cmd::Details => {
                self.popup = Some(PopupKind::Logs);
                Some(Action::Redraw)
            }
            _ => None,
        }
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        _: Rect,
        active: bool,
    ) {
        self.ensure_selection_matches_rack_state(state);
        let border_style = style::line(active);
        let header_style = style::header(active);

        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .style(border_style);

        // Draw the title/tab bar
        let title_bar = Paragraph::new(Spans::from(vec![Span::styled(
            "UPDATE STATUS",
            header_style,
        )]))
        .block(block.clone().title("<ENTER>"));
        frame.render_widget(title_bar, self.title_rect);

        // Draw the table headers
        let mut line_rect = self.table_headers_rect;
        line_rect.x += 2;
        line_rect.width -= 2;
        let headers = Paragraph::new(align_by(
            4,
            MAX_COLUMN_WIDTH,
            line_rect,
            vec![
                Span::styled("COMPONENT", header_style),
                Span::styled("VERSION", header_style),
                Span::styled("TARGET", header_style),
                Span::styled("STATUS", header_style),
            ],
        ))
        .block(block.clone());
        frame.render_widget(headers, self.table_headers_rect);

        // Need to refresh the items, as their versions/state may have changed
        self.update_items(state);

        // Draw the contents
        let tree = Tree::new(self.items.clone())
            .block(block.clone().borders(Borders::LEFT | Borders::RIGHT))
            .style(style::plain_text())
            .highlight_style(style::highlighted());
        frame.render_stateful_widget(
            tree,
            self.contents_rect,
            &mut self.tree_state,
        );

        // Draw the help bar
        let help = help_text(&self.help).block(block.clone());
        frame.render_widget(help, self.help_rect);

        // Ensure the contents is connected to the table headers and help bar
        frame.render_widget(
            BoxConnector::new(BoxConnectorKind::Both),
            self.contents_rect,
        );

        match self.popup {
            Some(PopupKind::Logs) => self.draw_log_popup(state, frame),
            Some(PopupKind::MissingRepo) => {
                self.draw_update_missing_popup(state, frame)
            }
            Some(PopupKind::StartUpdate) => {
                self.draw_start_update_popup(state, frame)
            }
            None => (),
        }
    }
}
