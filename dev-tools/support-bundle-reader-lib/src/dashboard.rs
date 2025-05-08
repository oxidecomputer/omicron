// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Dashboard for inspecting bundles

use crate::bundle_accessor::BoxedFileAccessor;
use crate::bundle_accessor::InternalApiAccess;
use crate::bundle_accessor::LocalFileAccess;
use crate::bundle_accessor::SupportBundleAccessor;
use crate::index::SupportBundleIndex;
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use bytes::BytesMut;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use crossterm::event;
use crossterm::event::DisableMouseCapture;
use crossterm::event::EnableMouseCapture;
use crossterm::event::Event;
use crossterm::event::KeyCode;
use crossterm::execute;
use crossterm::terminal::EnterAlternateScreen;
use crossterm::terminal::LeaveAlternateScreen;
use crossterm::terminal::disable_raw_mode;
use crossterm::terminal::enable_raw_mode;
use futures::TryStreamExt;
use nexus_client::types::SupportBundleInfo;
use nexus_client::types::SupportBundleState;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::Backend;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Constraint;
use ratatui::layout::Layout;
use ratatui::style::Modifier;
use ratatui::style::Style;
use ratatui::widgets::Block;
use ratatui::widgets::Borders;
use ratatui::widgets::List;
use ratatui::widgets::ListState;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Wrap;
use std::io::Write;
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;

const BUF_READER_CAPACITY: usize = 1 << 16;

enum FileState<'a> {
    Open { access: Option<BufReader<BoxedFileAccessor<'a>>>, preview: BytesMut },
    Closed,
}

/// A dashboard for inspecting a support bundle's contents
pub struct SupportBundleDashboard<'a> {
    access: Box<dyn SupportBundleAccessor + 'a>,
    index: SupportBundleIndex,
    selected: usize,
    file: FileState<'a>,
}

impl<'a> SupportBundleDashboard<'a> {
    async fn new(access: Box<dyn SupportBundleAccessor + 'a>) -> Result<Self> {
        let index = access.get_index().await?;
        if index.files().is_empty() {
            bail!("No files found in support bundle");
        }
        Ok(Self { access, index, selected: 0, file: FileState::Closed })
    }

    fn index(&self) -> &SupportBundleIndex {
        &self.index
    }

    async fn select_up(&mut self, count: usize) -> Result<()> {
        let old_selection = self.selected;
        self.selected = self.selected.saturating_sub(count);

        // Buffer the new file if we're currently viewing open files
        if old_selection != self.selected
            && matches!(self.file, FileState::Open { .. })
        {
            self.open_and_buffer().await?;
        }
        Ok(())
    }

    async fn select_down(&mut self, count: usize) -> Result<()> {
        let old_selection = self.selected;
        self.selected =
            std::cmp::min(self.selected + count, self.index.files().len() - 1);

        // Buffer the new file if we're currently viewing open files
        if old_selection != self.selected
            && matches!(self.file, FileState::Open { .. })
        {
            self.open_and_buffer().await?;
        }
        Ok(())
    }

    async fn toggle_file_open(&mut self) -> Result<()> {
        match self.file {
            FileState::Open { .. } => self.close_file(),
            FileState::Closed => self.open_and_buffer().await?,
        }
        Ok(())
    }

    async fn open_and_buffer(&mut self) -> Result<()> {
        self.open_file().await?;
        self.read_to_buffer().await?;
        Ok(())
    }

    async fn open_file(&mut self) -> Result<()> {
        let path = &self.index.files()[self.selected];
        if path.as_str().ends_with("/") {
            self.file = FileState::Open {
                access: None,
                preview: BytesMut::from(&b"<directory>"[..]),
            };
            return Ok(());
        }

        let file = self
            .access
            .get_file(&path)
            .await
            .with_context(|| format!("Failed to access {path}"))?;
        self.file = FileState::Open {
            access: Some(BufReader::with_capacity(BUF_READER_CAPACITY, file)),
            preview: BytesMut::new(),
        };
        Ok(())
    }

    fn close_file(&mut self) {
        self.file = FileState::Closed;
    }

    async fn read_to_buffer(&mut self) -> Result<()> {
        let FileState::Open { access, ref mut preview } = &mut self.file else {
            bail!("File cannot be buffered while closed");
        };
        let Some(file) = access.as_mut() else {
            return Ok(());
        };
        preview.reserve(BUF_READER_CAPACITY);
        file.read_buf(preview).await?;
        Ok(())
    }

    fn buffered_file_preview(&self) -> Option<&[u8]> {
        let FileState::Open { ref preview, .. } = &self.file else {
            return None;
        };
        return Some(preview);
    }

    fn streaming_file_contents(
        &mut self,
    ) -> Option<impl AsyncRead + use<'_, 'a>> {
        match &mut self.file {
            FileState::Open { access: Some(access), preview } => {
                Some(preview.chain(access))
            }
            _ => None,
        }
    }

    fn selected_file_index(&self) -> usize {
        self.selected
    }

    fn selected_file_name(&self) -> &Utf8Path {
        &self.index.files()[self.selected_file_index()]
    }
}

enum InspectRunStep {
    // Keep running the dashboard
    Continue,
    // Exit the dashboard
    Exit,
    // Exit the dashboard GUI, but pipe a selected file to an output stream
    PipeFile,
}

async fn wait_for_bundle_to_be_collected(
    client: &nexus_client::Client,
    id: SupportBundleUuid,
) -> Result<SupportBundleInfo, anyhow::Error> {
    let mut printed_wait_msg = false;
    loop {
        let sb = client
            .support_bundle_view(id.as_untyped_uuid())
            .await
            .with_context(|| {
                format!("failed to query for support bundle {}", id)
            })?;

        match sb.state {
            SupportBundleState::Active => {
                if printed_wait_msg {
                    eprintln!("");
                }
                return Ok(sb.into_inner());
            }
            SupportBundleState::Collecting => {
                if !printed_wait_msg {
                    eprint!("Waiting for {} to finish collection...", id);
                    printed_wait_msg = true;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                eprint!(".");
                std::io::stderr()
                    .flush()
                    .with_context(|| "cannot flush stderr".to_string())?;
            }
            other => bail!("Unexepcted state: {other}"),
        }
    }
}

async fn access_bundle_from_id(
    client: &nexus_client::Client,
    id: Option<SupportBundleUuid>,
) -> Result<InternalApiAccess<'_>, anyhow::Error> {
    let id = match id {
        Some(id) => {
            // Ensure the bundle has been collected
            let sb = wait_for_bundle_to_be_collected(
                client,
                SupportBundleUuid::from_untyped_uuid(*id.as_untyped_uuid()),
            )
            .await?;
            SupportBundleUuid::from_untyped_uuid(sb.id.into_untyped_uuid())
        }
        None => {
            // Grab the latest if one isn't supplied
            let support_bundle_stream =
                client.support_bundle_list_stream(None, None);
            let mut support_bundles = support_bundle_stream
                .try_collect::<Vec<_>>()
                .await
                .context("listing support bundles")?;
            support_bundles.sort_by_key(|k| k.time_created);

            let active_sb = support_bundles
                .iter()
                .find(|sb| matches!(sb.state, SupportBundleState::Active));

            let sb = match active_sb {
                Some(sb) => sb.clone(),
                None => {
                    // This is a special case, but not an uncommon one:
                    //
                    // - Someone just created a bundle...
                    // - ... but collection is still happening.
                    //
                    // To smooth out this experience for users, we wait for the
                    // collection to complete.
                    let collecting_sb = support_bundles.iter().find(|sb| {
                        matches!(sb.state, SupportBundleState::Collecting)
                    });
                    if let Some(collecting_sb) = collecting_sb {
                        let id = &collecting_sb.id;
                        wait_for_bundle_to_be_collected(
                            client,
                            SupportBundleUuid::from_untyped_uuid(
                                *id.as_untyped_uuid(),
                            ),
                        )
                        .await?
                    } else {
                        bail!(
                            "Cannot find active support bundle. Try creating one"
                        )
                    }
                }
            };

            eprintln!("Inspecting bundle {} from {}", sb.id, sb.time_created);

            SupportBundleUuid::from_untyped_uuid(sb.id.into_untyped_uuid())
        }
    };
    Ok(InternalApiAccess::new(client, id))
}

pub async fn run_dashboard(
    client: &nexus_client::Client,
    id: Option<SupportBundleUuid>,
    path: Option<&Utf8PathBuf>,
) -> Result<(), anyhow::Error> {
    let accessor: Box<dyn SupportBundleAccessor> = match (id, &path) {
        (None, Some(path)) => Box::new(LocalFileAccess::new(path)?),
        (maybe_id, None) => {
            Box::new(access_bundle_from_id(client, maybe_id).await?)
        }
        (Some(_), Some(_)) => {
            bail!("Cannot specify both UUID and path");
        }
    };

    let mut dashboard = SupportBundleDashboard::new(accessor).await?;

    enable_raw_mode()?;

    // TODO: It should probably be a flag whether or not this is stderr or
    // stdout.
    let mut stderr = std::io::stderr();
    execute!(stderr, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stderr);
    let mut terminal = Terminal::new(backend)?;

    let mut force_update = true;
    let pipe_selected_file = loop {
        match run_support_bundle_dashboard(
            &mut terminal,
            &mut dashboard,
            force_update,
        )
        .await
        {
            Err(err) => break Err(err),
            Ok(InspectRunStep::Exit) => break Ok(false),
            Ok(InspectRunStep::Continue) => (),
            Ok(InspectRunStep::PipeFile) => break Ok(true),
        };

        force_update = false;
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    // restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    match pipe_selected_file {
        Ok(true) => {
            if let Some(mut stream) = dashboard.streaming_file_contents() {
                tokio::io::copy(&mut stream, &mut tokio::io::stdout()).await?;
            }
        }
        Ok(false) => (),
        Err(err) => eprintln!("{err:?}"),
    }
    Ok(())
}

async fn run_support_bundle_dashboard<B: Backend>(
    terminal: &mut Terminal<B>,
    dashboard: &mut SupportBundleDashboard<'_>,
    force_update: bool,
) -> anyhow::Result<InspectRunStep> {
    let update = if crossterm::event::poll(Duration::from_secs(0))? {
        if let Event::Key(key) = event::read()? {
            let shifted = key.modifiers.contains(event::KeyModifiers::SHIFT);
            match key.code {
                KeyCode::Char('q') => return Ok(InspectRunStep::Exit),
                KeyCode::Up | KeyCode::Char('k') | KeyCode::Char('K') => {
                    let count = if shifted { 5 } else { 1 };
                    dashboard.select_up(count).await?;
                }
                KeyCode::Down | KeyCode::Char('j') | KeyCode::Char('J') => {
                    let count = if shifted { 5 } else { 1 };
                    dashboard.select_down(count).await?;
                }
                KeyCode::Char(' ') => {
                    dashboard.open_and_buffer().await?;
                    return Ok(InspectRunStep::PipeFile);
                }
                KeyCode::Enter => dashboard.toggle_file_open().await?,
                _ => {}
            }
        }
        true
    } else {
        force_update
    };

    if force_update {
        terminal.clear()?;
    }

    if update {
        terminal.draw(|f| draw(f, dashboard))?;
    }

    Ok(InspectRunStep::Continue)
}

fn create_file_list<'a>(dashboard: &'a SupportBundleDashboard<'_>) -> List<'a> {
    let files = dashboard.index().files().iter().map(|f| f.as_str());
    List::new(files)
        .highlight_symbol("> ")
        .highlight_style(Style::new().add_modifier(Modifier::BOLD))
        .block(Block::new().title("Files").borders(Borders::ALL))
}

fn create_file_preview<'a>(
    dashboard: &'a SupportBundleDashboard<'_>,
) -> Option<Paragraph<'a>> {
    dashboard.buffered_file_preview().map(|c| {
        let c = std::str::from_utf8(c).unwrap_or("Not valid UTF-8");

        Paragraph::new(c).wrap(Wrap { trim: false }).block(
            Block::new()
                .title(dashboard.selected_file_name().as_str())
                .borders(Borders::ALL),
        )
    })
}

const FILE_PICKER_USAGE: [&'static str; 4] = [
    "Press UP or DOWN to select a file. Hold SHIFT to move faster",
    "Press ENTER to view a file",
    "Press SPACE to exit the terminal and dump the file to stdout",
    "Press 'q' to quit",
];

const FILE_VIEWER_USAGE: [&'static str; 4] = [
    "Press UP or DOWN to select a file. Hold SHIFT to move faster",
    "Press ENTER to stop viewing file",
    "Press SPACE to exit the terminal and dump the file to stdout",
    "Press 'q' to quit",
];

fn draw(f: &mut Frame, dashboard: &mut SupportBundleDashboard<'_>) {
    let file_list = create_file_list(dashboard);
    let file_preview = create_file_preview(dashboard);

    let mut file_state = ListState::default()
        .with_offset(0)
        .with_selected(Some(dashboard.selected_file_index()));

    let layout = Layout::vertical([Constraint::Min(0), Constraint::Length(6)]);

    let [main_display_rect, usage_rect] = layout.areas(f.area());

    if let Some(file_preview) = file_preview {
        let usage_list = List::new(FILE_VIEWER_USAGE)
            .block(Block::new().title("Usage").borders(Borders::ALL));

        f.render_widget(file_preview, main_display_rect);
        f.render_widget(usage_list, usage_rect);
    } else {
        let usage_list = List::new(FILE_PICKER_USAGE)
            .block(Block::new().title("Usage").borders(Borders::ALL));
        f.render_stateful_widget(file_list, main_display_rect, &mut file_state);
        f.render_widget(usage_list, usage_rect);
    }
}
