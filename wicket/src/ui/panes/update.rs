// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use super::{align_by, help_text, Control};
use crate::state::{
    update_component_title, ComponentId, Inventory, UpdateItemState,
    ALL_COMPONENT_IDS,
};
use crate::ui::defaults::style;
use crate::ui::widgets::{
    BoxConnector, BoxConnectorKind, ButtonText, IgnitionPopup, Popup,
};
use crate::{Action, Cmd, Frame, State};
use indexmap::IndexMap;
use omicron_common::api::internal::nexus::KnownArtifactKind;
use slog::{info, o, Logger};
use tui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use tui::text::{Span, Spans, Text};
use tui::widgets::{
    Block, BorderType, Borders, Cell, List, ListItem, ListState, Paragraph,
    Row, Table,
};
use tui_tree_widget::{Tree, TreeItem, TreeState};
use update_engine::{ExecutionStatus, StepKey};
use wicket_common::update_events::{
    EventBuffer, EventReport, StepOutcome, StepStatus, UpdateComponent,
};
use wicketd_client::types::SemverVersion;

const MAX_COLUMN_WIDTH: u16 = 25;

enum PopupKind {
    StartUpdate,
    StepLogs,
    Ignition,
}

/// Overview of update status and ability to install updates
/// from a single TUF repo uploaded to wicketd via wicket.
pub struct UpdatePane {
    #[allow(unused)]
    log: Logger,
    help: Vec<(&'static str, &'static str)>,

    /// TODO: Move following  state into global `State` so that recorder snapshots
    /// capture all state.
    tree_state: TreeState,
    items: Vec<TreeItem<'static>>,

    // Per-component update state that isn't serializable.
    component_state: BTreeMap<ComponentId, ComponentUpdateListState>,

    rect: Rect,

    // TODO: These will likely move into a status view, because there will be
    // other update views/tabs
    title_rect: Rect,
    table_headers_rect: Rect,
    contents_rect: Rect,
    // TODO: remove the help rect and replace it with a popup.
    help_rect: Rect,

    status_view_version_rect: Rect,
    status_view_main_rect: Rect,
    popup: Option<PopupKind>,

    ignition: IgnitionPopup,
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
                ("Ignition", "<i>"),
                ("Update", "<Enter>"),
            ],
            component_state: ALL_COMPONENT_IDS
                .iter()
                .map(|id| (*id, ComponentUpdateListState::default()))
                .collect(),
            rect: Rect::default(),
            title_rect: Rect::default(),
            table_headers_rect: Rect::default(),
            contents_rect: Rect::default(),
            help_rect: Rect::default(),
            status_view_version_rect: Rect::default(),
            status_view_main_rect: Rect::default(),
            popup: None,
            ignition: IgnitionPopup::default(),
        }
    }

    pub fn draw_step_log_popup(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
    ) {
        let selected = state.rack_state.selected;
        let id_state = self.component_state.get(&selected).unwrap();
        // We only open the popup if id_state.selected is not None, but in some cases
        // the selected key can disappear from underneath us (e.g. a new wicketd
        // comes up.) If that happens, close the popup.
        let selected_key = match id_state.selected {
            Some(key) => key,
            None => {
                self.popup = None;
                return;
            }
        };
        let value = id_state
            .event_buffer
            .get(&selected_key)
            .expect("selected_key is always valid");
        let step_info = value.step_info();

        let mut header = Text::default();
        header.lines.push(Spans::from(vec![Span::styled(
            step_info.description.clone(),
            style::header(true),
        )]));

        let mut body = Text::default();

        match value.step_status() {
            StepStatus::NotStarted => {
                body.lines.push(Spans::from("Step has not started yet"));
            }
            StepStatus::Running { progress_event, .. } => {
                let mut spans = vec![
                    Span::styled("Status: ", style::selected()),
                    Span::styled("Running", style::bold()),
                ];
                if let Some(attempt) = progress_event.kind.leaf_attempt() {
                    if attempt > 1 {
                        // Display the attempt number.
                        spans.push(Span::raw(" (attempt "));
                        spans.push(Span::styled(
                            format!("{attempt}"),
                            style::bold(),
                        ));
                        spans.push(Span::raw(")"));
                    }
                }
                body.lines.push(Spans::from(spans));

                body.lines.push(Spans::default());

                let mut progress_spans = Vec::new();
                if let Some(progress) = progress_event.kind.progress_counter() {
                    progress_spans
                        .push(Span::styled("Progress: ", style::selected()));
                    let current = progress.current;
                    progress_spans.push(Span::styled(
                        format!("{current}"),
                        style::bold(),
                    ));
                    if let Some(total) = progress.total {
                        progress_spans.push(Span::raw("/"));
                        progress_spans.push(Span::styled(
                            format!("{total}"),
                            style::bold(),
                        ));
                    }
                    // TODO: progress units
                    // TODO: show a progress bar?
                } else {
                    progress_spans.push(Span::raw("Waiting for progress"));
                }
                if let Some(step_elapsed) =
                    progress_event.kind.leaf_step_elapsed()
                {
                    progress_spans
                        .push(Span::raw(format!(" (at {step_elapsed:.2?})")));
                }

                body.lines.push(Spans::from(progress_spans));

                // TODO: show previous attempts
            }
            StepStatus::Completed { info: Some(info) } => {
                let mut spans =
                    vec![Span::styled("Status: ", style::selected())];

                let message = match &info.outcome {
                    StepOutcome::Success { .. } => {
                        spans.push(Span::styled(
                            "Completed",
                            style::successful_update_bold(),
                        ));
                        None
                    }
                    StepOutcome::Warning { message, .. } => {
                        spans.push(Span::styled(
                            "Completed with warning",
                            style::warning_update_bold(),
                        ));
                        Some(message)
                    }
                    StepOutcome::Skipped { message, .. } => {
                        spans.push(Span::styled(
                            "Skipped",
                            style::plain_text_bold(),
                        ));
                        Some(message)
                    }
                };

                if info.attempt > 1 {
                    // Display the attempt number.
                    spans.push(Span::raw(" (attempt "));
                    spans.push(Span::styled(
                        format!("{}", info.attempt),
                        style::bold(),
                    ));
                    spans.push(Span::raw(")"));
                }

                spans.push(Span::raw(format!(
                    " after {:.2?}",
                    info.step_elapsed
                )));
                body.lines.push(Spans::from(spans));

                if let Some(message) = message {
                    body.lines.push(Spans::default());
                    let message_spans = vec![
                        Span::styled("Message: ", style::selected()),
                        Span::raw(message.as_ref()),
                    ];
                    body.lines.push(Spans::from(message_spans));
                }
            }
            StepStatus::Completed { info: None } => {
                // No information is available, so all we can do is say that
                // this step is completed.
                body.lines.push(Spans::from(vec![
                    Span::styled("Status: ", style::selected()),
                    Span::styled("Completed", style::successful_update_bold()),
                ]));
            }
            StepStatus::Failed { info: Some(info) } => {
                let mut spans = vec![
                    Span::styled("Status: ", style::selected()),
                    Span::styled("Failed", style::failed_update_bold()),
                ];
                if info.total_attempts > 1 {
                    // Display the attempt number.
                    spans.push(Span::styled(" (", style::plain_text()));
                    spans.push(Span::styled(
                        format!("{}", info.total_attempts),
                        style::plain_text(),
                    ));
                    spans.push(Span::styled(" attempts)", style::plain_text()));
                }
                spans.push(Span::styled(
                    format!(" after {:.2?}", info.step_elapsed),
                    style::plain_text(),
                ));
                body.lines.push(Spans::from(spans));

                body.lines.push(Spans::default());

                // Show the message.
                let message_spans = vec![
                    Span::styled("Message: ", style::selected()),
                    Span::styled(&info.message, style::plain_text()),
                ];
                body.lines.push(Spans::from(message_spans));

                // Show causes.
                if !info.causes.is_empty() {
                    body.lines.push(Spans::default());
                    body.lines.push(Spans::from(Span::styled(
                        "Caused by:",
                        style::selected(),
                    )));
                    for cause in &info.causes {
                        body.lines.push(Spans::from(vec![
                            Span::raw("-> "),
                            Span::styled(cause, style::plain_text()),
                        ]))
                    }
                }
            }
            StepStatus::Failed { info: None } => {
                // No information is available, so all we can do is say that
                // this step failed.
                let spans = vec![
                    Span::styled("Status: ", style::selected()),
                    Span::styled("Failed", style::failed_update_bold()),
                ];
                body.lines.push(Spans::from(spans));
            }
            StepStatus::WillNotBeRun { step_that_failed } => {
                let mut spans = vec![
                    Span::styled("Status: ", style::selected()),
                    Span::styled("Will not be run", style::plain_text_bold()),
                ];
                if let Some(value) = id_state.event_buffer.get(step_that_failed)
                {
                    spans.push(Span::styled(
                        " because step ",
                        style::plain_text(),
                    ));
                    spans.push(Span::styled(
                        value.step_info().description.as_ref(),
                        style::selected(),
                    ));
                    spans.push(Span::styled(" failed", style::plain_text()));
                }
                body.lines.push(Spans::from(spans));
            }
        }

        let popup = Popup {
            header,
            body,
            buttons: vec![ButtonText {
                instruction: "NAVIGATE",
                key: "LEFT/RIGHT",
            }],
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
                format!(" START UPDATE: {}", state.rack_state.selected),
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

    pub fn draw_ignition_popup(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
    ) {
        let popup = self.ignition.popup(state.rack_state.selected);
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
                    .map(|(component, s)| {
                        let target_version =
                            artifact_version(id, component, &versions);
                        let installed_version =
                            installed_version(id, component, inventory);
                        let spans = vec![
                            Span::styled(
                                update_component_title(component),
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

    fn update_component_list_items(
        &mut self,
        component_id: ComponentId,
        state: &State,
    ) {
        let id_state = self.component_state.get_mut(&component_id).unwrap();
        if let UpdateItemState::RunningOrCompleted { event_report } =
            state.update_state.item_state(component_id)
        {
            id_state.process_report(event_report.clone());
        } else {
            // No event report being available means an update isn't running.
            *id_state = Default::default();
        }
    }

    // Called when state.update_state.status_view_displayed is true.
    fn handle_cmd_in_status_view(
        &mut self,
        state: &mut State,
        cmd: Cmd,
    ) -> Option<Action> {
        debug_assert!(
            state.update_state.status_view_displayed,
            "called when status view is displayed"
        );
        match cmd {
            Cmd::Up => {
                let id_state = self
                    .component_state
                    .get_mut(&state.rack_state.selected)
                    .unwrap();
                id_state.prev_item();
                Some(Action::Redraw)
            }
            Cmd::Down => {
                let id_state = self
                    .component_state
                    .get_mut(&state.rack_state.selected)
                    .unwrap();
                id_state.next_item();
                Some(Action::Redraw)
            }
            Cmd::Left => {
                state.rack_state.prev();
                Some(Action::Redraw)
            }
            Cmd::Right => {
                state.rack_state.next();
                Some(Action::Redraw)
            }
            Cmd::Enter => {
                // Only open the popup if an item is actually selected.
                let id_state = self
                    .component_state
                    .get(&state.rack_state.selected)
                    .unwrap();
                if id_state.selected.is_some() {
                    self.popup = Some(PopupKind::StepLogs);
                    Some(Action::Redraw)
                } else {
                    None
                }
            }
            Cmd::Exit => {
                state.update_state.status_view_displayed = false;
                Some(Action::Redraw)
            }
            Cmd::StartUpdate => {
                let selected = state.rack_state.selected;
                if state.update_state.item_state(selected)
                    == UpdateItemState::NotStarted
                {
                    // NotStarted means that "Press ... to start" is displayed.
                    self.popup = Some(PopupKind::StartUpdate);
                    Some(Action::Redraw)
                } else {
                    None
                }
            }
            Cmd::GotoTop => {
                let id_state = self
                    .component_state
                    .get_mut(&state.rack_state.selected)
                    .unwrap();
                id_state.select_first();
                Some(Action::Redraw)
            }
            Cmd::GotoBottom => {
                let id_state = self
                    .component_state
                    .get_mut(&state.rack_state.selected)
                    .unwrap();
                id_state.select_last();
                Some(Action::Redraw)
            }
            _ => None,
        }
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
            PopupKind::StepLogs => match cmd {
                // TODO: up/down for scrolling popup data
                Cmd::Left => {
                    let id_state = self
                        .component_state
                        .get_mut(&state.rack_state.selected)
                        .unwrap();
                    id_state.prev_item();
                    Some(Action::Redraw)
                }
                Cmd::Right => {
                    let id_state = self
                        .component_state
                        .get_mut(&state.rack_state.selected)
                        .unwrap();
                    id_state.next_item();
                    Some(Action::Redraw)
                }
                _ => None,
            },
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
                    // same arm of `InventoryView::handle_cmd_in_popup()` in the
                    // `overview` pane.
                    let command = self.ignition.selected_command();
                    let selected = state.rack_state.selected;
                    info!(self.log, "Sending {command:?} to {selected}");
                    self.popup = None;
                    Some(Action::Ignition(selected, command))
                }
                _ => None,
            },
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

    fn draw_tree_view(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
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
    }

    fn draw_status_view(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        active: bool,
    ) {
        let border_style = style::line(active);
        let header_style = style::header(active);

        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .style(border_style);

        // Draw the title/tab bar
        let title_bar = Paragraph::new(Spans::from(vec![
            Span::styled("UPDATE STATUS / ", border_style),
            Span::styled(state.rack_state.selected.to_string(), header_style),
        ]))
        .block(block.clone().title("<ENTER>"));
        frame.render_widget(title_bar, self.title_rect);

        let versions = &state.update_state.artifact_versions;
        let inventory = &state.inventory;

        // - 2 accounts for the left and right borders.
        let cell_width = self.table_headers_rect.width.saturating_sub(2) / 4;
        let width_constraints = [
            Constraint::Length(cell_width),
            Constraint::Length(cell_width),
            Constraint::Length(cell_width),
            Constraint::Length(cell_width),
        ];
        let header_table = Table::new(std::iter::empty())
            .header(
                Row::new(vec!["COMPONENT", "VERSION", "TARGET", "STATUS"])
                    .style(header_style),
            )
            .widths(&width_constraints)
            .block(block.clone().title("OVERVIEW"));
        frame.render_widget(header_table, self.table_headers_rect);

        // For the selected item, draw the version table.
        let selected = state.rack_state.selected;
        let item_state = &state.update_state.items[&selected];

        let version_rows =
            item_state.iter().map(|(component, update_state)| {
                let target_version = artifact_version(
                    &state.rack_state.selected,
                    component,
                    versions,
                );
                let installed_version = installed_version(
                    &state.rack_state.selected,
                    component,
                    inventory,
                );

                Row::new(vec![
                    Cell::from(update_component_title(component))
                        .style(style::selected()),
                    Cell::from(installed_version).style(style::selected_line()),
                    Cell::from(target_version).style(style::selected()),
                    Cell::from(update_state.to_string())
                        .style(update_state.style()),
                ])
            });
        let version_table =
            Table::new(version_rows).widths(&width_constraints).block(
                block
                    .clone()
                    .borders(Borders::LEFT | Borders::RIGHT | Borders::BOTTOM),
            );
        frame.render_widget(version_table, self.status_view_version_rect);

        // Ensure the version table is connected to the table headers
        frame.render_widget(
            BoxConnector::new(BoxConnectorKind::Top),
            self.status_view_version_rect,
        );

        // Need to refresh the list items, as their versions/state may have
        // changed.
        self.update_component_list_items(state.rack_state.selected, state);

        match state.update_state.item_state(state.rack_state.selected) {
            UpdateItemState::AwaitingRepository => {
                // Show this command.
                let text = Text::from(vec![
                    Spans::from(Vec::new()),
                    Spans::from(vec![Span::styled(
                        "Use the following command to transfer an update:",
                        style::plain_text(),
                    )]),
                    "".into(),
                    Spans::from(vec![
                        Span::styled("cat", style::plain_text()),
                        Span::styled(" $UPDATE", style::popup_highlight()),
                        Span::styled(".zip | ssh", style::plain_text()),
                        Span::styled(
                            " $IPV6_ADDRESS",
                            style::popup_highlight(),
                        ),
                        Span::styled(" upload", style::plain_text()),
                    ]),
                ]);
                let paragraph = Paragraph::new(text)
                    .alignment(Alignment::Center)
                    .block(block.clone().title("AWAITING REPOSITORY"));
                frame.render_widget(paragraph, self.status_view_main_rect);
            }
            UpdateItemState::NotStarted => {
                let text = Text::from(vec![
                    Spans::from(Vec::new()),
                    Spans::from(vec![
                        Span::styled(
                            "Update ready: Press ",
                            style::plain_text(),
                        ),
                        Span::styled("<Ctrl-U>", style::selected_line()),
                        Span::styled(" to start", style::plain_text()),
                    ]),
                ]);
                let paragraph = Paragraph::new(text)
                    .alignment(Alignment::Center)
                    .block(block.clone().title("UPDATE READY"));
                frame.render_widget(paragraph, self.status_view_main_rect);
            }
            UpdateItemState::RunningOrCompleted { .. } => {
                // Split the status view rect into two rects:
                // * status text (3 lines including borders)
                // * the list (everything else)
                let mut status_text_rect = self.status_view_main_rect;
                status_text_rect.height = 3;

                let mut list_rect = self.status_view_main_rect;
                list_rect.y += 3;
                list_rect.height = list_rect.height.saturating_sub(3);

                let id_state = self
                    .component_state
                    .get_mut(&state.rack_state.selected)
                    .unwrap();

                let text = Text::from(id_state.status_text.clone());

                let paragraph = Paragraph::new(text)
                    .block(block.clone().title("UPDATE STATUS"))
                    .alignment(Alignment::Center);
                frame.render_widget(paragraph, status_text_rect);

                let list = List::new(
                    id_state.list_items.values().cloned().collect::<Vec<_>>(),
                )
                .block(
                    block.clone().borders(
                        Borders::LEFT | Borders::RIGHT | Borders::BOTTOM,
                    ),
                )
                .highlight_style(style::highlighted());
                frame.render_stateful_widget(
                    list,
                    list_rect,
                    &mut id_state.tui_list_state,
                );

                frame.render_widget(
                    BoxConnector::new(BoxConnectorKind::Top),
                    list_rect,
                );
            }
        }
    }
}

#[derive(Debug, Default)]
struct ComponentUpdateListState {
    event_buffer: EventBuffer,
    status_text: Spans<'static>,
    list_items: IndexMap<StepKey, ListItem<'static>>,

    // For the selected item, we use the step key rather than the numerical
    // index as canonical because it's possible for steps to move around (e.g.
    // if a nested event adds new steps).
    //
    // This is always Some if step_keys is non-empty, and always points to a
    // valid index in step_keys. These invariants are enforced by the
    // process_report method.
    selected: Option<StepKey>,
    // list_state maintains both the numerical index and the list display
    // offset.
    //
    // This is kept in sync with `self.selected`.
    tui_list_state: ListState,
}

impl ComponentUpdateListState {
    fn process_report(&mut self, report: EventReport) {
        let mut event_buffer = EventBuffer::default();
        event_buffer.add_event_report(report);
        let steps = event_buffer.steps();

        // Generate the status text (displayed in a single line at the top.)
        let mut status_text = Vec::new();
        if let Some(root_execution_id) = event_buffer.root_execution_id() {
            let summary = steps.summarize();
            let summary = summary.get(&root_execution_id).expect(
                "root execution ID should have a summary associated with it",
            );

            match summary.execution_status {
                ExecutionStatus::NotStarted => {
                    status_text.push(Span::styled(
                        "Update not started",
                        style::plain_text(),
                    ));
                }
                ExecutionStatus::Running { step_key } => {
                    status_text
                        .push(Span::styled("Update ", style::plain_text()));
                    status_text.push(Span::styled(
                        "running",
                        style::successful_update_bold(),
                    ));
                    status_text.push(Span::styled(
                        format!(
                            " (step {}/{})",
                            step_key.index + 1,
                            summary.total_steps,
                        ),
                        style::plain_text(),
                    ));
                }
                ExecutionStatus::Completed { .. } => {
                    status_text
                        .push(Span::styled("Update ", style::plain_text()));
                    status_text.push(Span::styled(
                        "completed",
                        style::successful_update_bold(),
                    ));
                }
                ExecutionStatus::Failed { step_key } => {
                    status_text
                        .push(Span::styled("Update ", style::plain_text()));
                    status_text.push(Span::styled(
                        "failed",
                        style::failed_update_bold(),
                    ));
                    status_text.push(Span::styled(
                        format!(
                            " at step {}/{}",
                            step_key.index + 1,
                            summary.total_steps,
                        ),
                        style::plain_text(),
                    ));
                }
            }
        } else {
            status_text
                .push(Span::styled("Update not started", style::plain_text()));
        }

        let mut list_items = IndexMap::new();
        for &(step_key, value) in steps.as_slice() {
            let step_info = value.step_info();
            let mut item_spans = Vec::new();
            let indent = value.nest_level() * 2;
            if indent > 0 {
                item_spans.push(Span::raw(format!("{:indent$}", ' ')));
            }

            let description_style = match value.step_status() {
                StepStatus::NotStarted | StepStatus::WillNotBeRun { .. } => {
                    item_spans.push(Span::styled(
                        format!("{:>5} ", step_key.index + 1),
                        style::selected_line(),
                    ));
                    style::selected_line()
                }
                StepStatus::Running { progress_event, .. } => {
                    let mut pushed = false;
                    if let Some(counter) =
                        progress_event.kind.progress_counter()
                    {
                        if let Some(total) = counter.total {
                            let percentage =
                                (counter.current as u128 * 100) / total as u128;
                            item_spans.push(Span::styled(
                                format!("[{:>2}%] ", percentage),
                                style::selected(),
                            ));
                            pushed = true;
                        }
                    }

                    if !pushed {
                        // The ... has the same width as the other text above (6
                        // characters).
                        item_spans
                            .push(Span::styled("  ... ", style::selected()));
                    }
                    style::selected()
                }
                StepStatus::Completed { info } => {
                    let (character, style) = if let Some(info) = info {
                        match info.outcome {
                            StepOutcome::Success { .. } => {
                                ('✔', style::successful_update())
                            }
                            StepOutcome::Warning { .. } => {
                                ('⚠', style::warning_update())
                            }
                            StepOutcome::Skipped { .. } => {
                                ('*', style::successful_update())
                            }
                        }
                    } else {
                        // No information available for this step -- just mark
                        // it successful.
                        ('✔', style::successful_update())
                    };
                    item_spans.push(Span::styled(
                        format!("{:>5} ", character),
                        style,
                    ));
                    style::selected()
                }
                StepStatus::Failed { .. } => {
                    // Use a Unicode HEAVY BALLOT X character here because it
                    // looks nicer in a terminal than ❌ does.
                    item_spans.push(Span::styled(
                        format!("{:>5} ", "✘"),
                        style::failed_update(),
                    ));
                    style::selected()
                }
            };

            item_spans.push(Span::styled(
                step_info.description.clone(),
                description_style,
            ));

            // Add step keys and items to the list.
            list_items.insert(step_key, ListItem::new(Spans(item_spans)));
        }

        self.event_buffer = event_buffer;
        self.status_text = Spans(status_text);
        self.list_items = list_items;
        let selected_needs_reset = match self.selected {
            Some(step_key) => {
                // If step_keys doesn't contain the selected step key, it means
                // that the step key disappeared (which should only happen if
                // wicketd decided to send us an event report corresponding to a
                // brand new execution).
                !self.list_items.contains_key(&step_key)
            }
            None => true,
        };

        if selected_needs_reset {
            // To reset, select the first step key.
            self.selected = self
                .list_items
                .get_index(0)
                .map(|(step_key, _)| step_key.clone());
        }

        // Update the tui state to be in sync with the selected element.
        if let Some(selected) = self.selected {
            let selected_index = self
                .list_items
                .get_index_of(&selected)
                .expect("above block ensures selected is always valid");
            self.tui_list_state.select(Some(selected_index));
        } else {
            debug_assert!(
                self.list_items.is_empty(),
                "selected can only be None here if the list has no elements"
            );
            self.tui_list_state.select(None);
        }
    }

    fn prev_item(&mut self) {
        if let Some(selected) = self.selected {
            let index = self
                .list_items
                .get_index_of(&selected)
                .expect("selected is always a valid step key");
            let new_index = index.saturating_sub(1);
            let new_selected = *self
                .list_items
                .get_index(new_index)
                .expect("index is present")
                .0;
            self.selected = Some(new_selected);
            self.tui_list_state.select(Some(new_index));
        } else {
            // The list is empty. Don't need to do anything here.
        }
    }

    fn next_item(&mut self) {
        if let Some(selected) = self.selected {
            let index = self
                .list_items
                .get_index_of(&selected)
                .expect("selected is always a valid step key");
            // Cap the index at the size of the list.
            let new_index = if index + 1 == self.list_items.len() {
                index
            } else {
                index + 1
            };
            let new_selected = *self
                .list_items
                .get_index(new_index)
                .expect("index is present")
                .0;
            self.selected = Some(new_selected);
            self.tui_list_state.select(Some(new_index));
        } else {
            // The list is empty. Don't need to do anything here.
        }
    }

    fn select_first(&mut self) {
        if let Some((step_key, _)) = self.list_items.first() {
            self.selected = Some(*step_key);
            self.tui_list_state.select(Some(0));
        } else {
            // The list is empty. Don't need to do anything here.
        }
    }

    fn select_last(&mut self) {
        if let Some((step_key, _)) = self.list_items.last() {
            self.selected = Some(*step_key);
            self.tui_list_state.select(Some(self.list_items.len() - 1));
        } else {
            // The list is empty. Don't need to do anything here.
        }
    }
}

fn installed_version(
    id: &ComponentId,
    update_component: UpdateComponent,
    inventory: &Inventory,
) -> String {
    let component = inventory.get_inventory(id);
    match update_component {
        UpdateComponent::Sp => component.map_or_else(
            || "UNKNOWN".to_string(),
            |component| component.sp_version(),
        ),
        UpdateComponent::Rot => component.map_or_else(
            || "UNKNOWN".to_string(),
            |component| component.rot_version(),
        ),
        UpdateComponent::Host => {
            // We currently have no way to tell what version of host software is
            // installed.
            "-----".to_string()
        }
    }
}

fn artifact_version(
    id: &ComponentId,
    component: UpdateComponent,
    versions: &BTreeMap<KnownArtifactKind, SemverVersion>,
) -> String {
    let artifact = match (id, component) {
        (ComponentId::Sled(_), UpdateComponent::Rot) => {
            KnownArtifactKind::GimletRot
        }
        (ComponentId::Sled(_), UpdateComponent::Sp) => {
            KnownArtifactKind::GimletSp
        }
        (ComponentId::Sled(_), UpdateComponent::Host) => {
            KnownArtifactKind::Host
        }
        (ComponentId::Switch(_), UpdateComponent::Rot) => {
            KnownArtifactKind::SwitchRot
        }
        (ComponentId::Switch(_), UpdateComponent::Sp) => {
            KnownArtifactKind::SwitchSp
        }
        (ComponentId::Psc(_), UpdateComponent::Rot) => {
            KnownArtifactKind::PscRot
        }
        (ComponentId::Psc(_), UpdateComponent::Sp) => KnownArtifactKind::PscSp,

        // Switches and PSCs do not have a host.
        (ComponentId::Switch(_), UpdateComponent::Host)
        | (ComponentId::Psc(_), UpdateComponent::Host) => {
            return "N/A".to_string()
        }
    };
    versions
        .get(&artifact)
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

        let status_view_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    Constraint::Length(3),
                    Constraint::Length(3),
                    Constraint::Length(4),
                    Constraint::Min(0),
                ]
                .as_ref(),
            )
            .split(rect);
        // status_view_chunks[0] is title_rect as above.
        // status_view_chunks[1] is table_headers_rect as above.
        self.status_view_version_rect = status_view_chunks[2];
        self.status_view_main_rect = status_view_chunks[3];

        self.update_items(state);
    }

    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        self.ensure_selection_matches_rack_state(state);
        if self.popup.is_some() {
            return self.handle_cmd_in_popup(state, cmd);
        }
        if state.update_state.status_view_displayed {
            return self.handle_cmd_in_status_view(state, cmd);
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
                state.update_state.status_view_displayed = true;
                Some(Action::Redraw)
            }
            Cmd::Ignition => {
                self.ignition.reset();
                self.popup = Some(PopupKind::Ignition);
                Some(Action::Redraw)
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
        self.ensure_selection_matches_rack_state(state);
        if state.update_state.status_view_displayed {
            self.draw_status_view(state, frame, active);
        } else {
            self.draw_tree_view(state, frame, active);
        }

        match self.popup {
            Some(PopupKind::StepLogs) => self.draw_step_log_popup(state, frame),
            Some(PopupKind::StartUpdate) => {
                self.draw_start_update_popup(state, frame)
            }
            Some(PopupKind::Ignition) => self.draw_ignition_popup(state, frame),
            None => (),
        }
    }
}
