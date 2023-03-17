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
use slog::{o, Logger};
use tui::layout::{Constraint, Direction, Layout, Rect};
use tui::text::{Span, Spans, Text};
use tui::widgets::{Block, BorderType, Borders, Paragraph};
use tui_tree_widget::{Tree, TreeItem, TreeState};
use wicketd_client::types::SemverVersion;

const MAX_COLUMN_WIDTH: u16 = 25;

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
    popup_open: bool,
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
                ("OPEN", "<RIGHT>"),
                ("CLOSE", "<LEFT>"),
                ("SELECT", "<UP/DOWN>"),
            ],
            rect: Rect::default(),
            title_rect: Rect::default(),
            table_headers_rect: Rect::default(),
            contents_rect: Rect::default(),
            help_rect: Rect::default(),
            popup_open: false,
        }
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
        self.popup_open
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
                // we want to re-open it. This is the only API currently provided
                // that allows this.
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
                // Only open the warning popup if an upload is required
                if state.update_state.artifacts.is_empty() {
                    if !self.popup_open {
                        self.popup_open = true;
                        Some(Action::Redraw)
                    } else {
                        None
                    }
                } else {
                    // Trigger the update
                    Some(Action::Update(state.rack_state.selected))
                }
            }
            Cmd::Exit => {
                if self.popup_open {
                    self.popup_open = false;
                    Some(Action::Redraw)
                } else {
                    None
                }
            }
            Cmd::GotoTop => {
                self.tree_state.select_first();
                state.rack_state.selected = ALL_COMPONENT_IDS[0];
                Some(Action::Redraw)
            }
            Cmd::GotoBottom => {
                self.tree_state.select_last(&self.items);
                state.rack_state.selected =
                    ALL_COMPONENT_IDS[ALL_COMPONENT_IDS.len() - 1];
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

        // TODO: Check to see which popup is open
        if self.popup_open {
            // TODO: Only open if an update has not been uploaded to wicketd
            // Otherwise, prompt whether to update or not.
            // We can also open the update logs inline once the update has been started
            // or has completed.
            self.draw_update_missing_popup(state, frame);
        }
    }
}
