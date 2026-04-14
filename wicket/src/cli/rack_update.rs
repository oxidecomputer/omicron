// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Command-line driven rack update.
//!
//! This is an alternative to using the Wicket UI to perform a rack update.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufReader, Write},
    net::SocketAddrV6,
    process::ExitCode,
    time::Duration,
};

use crate::{
    cli::GlobalOpts,
    state::{
        ComponentId, CreateClearUpdateStateOptions, CreateStartUpdateOptions,
        parse_event_report_map,
    },
    wicketd::create_wicketd_client,
};
use anyhow::{Context, Result, anyhow, bail};
use camino::Utf8PathBuf;
use clap::{Args, Subcommand, ValueEnum};
use omicron_common::update::ArtifactId;
use slog::Logger;
use tokio::{sync::watch, task::JoinHandle};
use update_engine::{
    EventBuffer, NestedError,
    display::{GroupDisplay, LineDisplayStyles},
};
use update_engine::{ExecutionStatus, TerminalKind};
use wicket_common::{
    WICKETD_TIMEOUT,
    rack_update::{
        ClearUpdateStateResponse, ComponentUpdateStatus, RackUpdateStatus,
        UpdateState, rollup_update_state,
    },
    update_events::{EventReport, WicketdEngineSpec},
};
use wicketd_client::types::{
    ClearUpdateStateParams, GetArtifactsAndEventReportsResponse,
    StartUpdateParams,
};

use super::command::CommandOutput;

#[derive(Debug, Subcommand)]
pub(crate) enum RackUpdateArgs {
    /// Start one or more updates.
    Start(StartRackUpdateArgs),

    /// Attach to one or more running updates.
    Attach(AttachArgs),

    /// Get the status of the updates.
    Status(StatusArgs),

    /// Clear updates.
    Clear(ClearArgs),

    /// Dump artifacts and event reports from wicketd.
    ///
    /// Debug-only, intended for development.
    DebugDump(DumpArgs),

    /// Replay update logs from a dump file.
    ///
    /// Debug-only, intended for development.
    DebugReplay(ReplayArgs),
}

impl RackUpdateArgs {
    pub(crate) async fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        global_opts: GlobalOpts,
        output: CommandOutput<'_>,
    ) -> Result<ExitCode> {
        match self {
            RackUpdateArgs::Start(args) => {
                args.exec(log, wicketd_addr, global_opts, output).await?;
                Ok(ExitCode::SUCCESS)
            }
            RackUpdateArgs::Attach(args) => {
                args.exec(log, wicketd_addr, global_opts, output).await?;
                Ok(ExitCode::SUCCESS)
            }
            RackUpdateArgs::Status(args) => {
                args.exec(log, wicketd_addr, output).await
            }
            RackUpdateArgs::Clear(args) => {
                args.exec(log, wicketd_addr, global_opts, output).await?;
                Ok(ExitCode::SUCCESS)
            }
            RackUpdateArgs::DebugDump(args) => {
                args.exec(log, wicketd_addr).await?;
                Ok(ExitCode::SUCCESS)
            }
            RackUpdateArgs::DebugReplay(args) => {
                args.exec(log, global_opts, output)?;
                Ok(ExitCode::SUCCESS)
            }
        }
    }
}

#[derive(Debug, Args)]
pub(crate) struct StartRackUpdateArgs {
    #[clap(flatten)]
    component_ids: ComponentIdSelector,

    /// Force update the RoT Bootloader even if the version is the same.
    #[clap(long, help_heading = "Update options")]
    force_update_rot_bootloader: bool,

    /// Force update the RoT even if the version is the same.
    #[clap(long, help_heading = "Update options")]
    force_update_rot: bool,

    /// Force update the SP even if the version is the same.
    #[clap(long, help_heading = "Update options")]
    force_update_sp: bool,

    /// Detach after starting the update.
    ///
    /// The `attach` command can be used to reattach to the running update.
    #[clap(short, long, help_heading = "Update options")]
    detach: bool,
}

impl StartRackUpdateArgs {
    async fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        global_opts: GlobalOpts,
        output: CommandOutput<'_>,
    ) -> Result<()> {
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        let update_ids = self.component_ids.to_component_ids()?;
        let options = CreateStartUpdateOptions {
            force_update_rot_bootloader: self.force_update_rot_bootloader,
            force_update_rot: self.force_update_rot,
            force_update_sp: self.force_update_sp,
        }
        .to_start_update_options()?;

        let num_update_ids = update_ids.len();

        let params = StartUpdateParams {
            targets: update_ids.iter().copied().map(Into::into).collect(),
            options,
        };

        slog::debug!(log, "Sending post_start_update"; "num_update_ids" => num_update_ids);
        match client.post_start_update(&params).await {
            Ok(_) => {
                slog::info!(log, "Update started for {num_update_ids} targets");
            }
            Err(error) => {
                // Error responses can be printed out more clearly.
                if let wicketd_client::Error::ErrorResponse(rv) = &error {
                    slog::error!(
                        log,
                        "Error response from wicketd: {}",
                        rv.message
                    );
                    bail!("Received error from wicketd while starting update");
                } else {
                    bail!(error);
                }
            }
        }

        if self.detach {
            return Ok(());
        }

        // Now, attach to the update by printing out update logs.
        do_attach_to_updates(log, client, update_ids, global_opts, output)
            .await?;

        Ok(())
    }
}

#[derive(Debug, Args)]
pub(crate) struct AttachArgs {
    #[clap(flatten)]
    component_ids: ComponentIdSelector,
}

impl AttachArgs {
    async fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        global_opts: GlobalOpts,
        output: CommandOutput<'_>,
    ) -> Result<()> {
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        let update_ids = self.component_ids.to_component_ids()?;
        do_attach_to_updates(log, client, update_ids, global_opts, output).await
    }
}

async fn do_attach_to_updates(
    log: Logger,
    client: wicketd_client::Client,
    update_ids: BTreeSet<ComponentId>,
    global_opts: GlobalOpts,
    output: CommandOutput<'_>,
) -> Result<()> {
    let mut display = GroupDisplay::new_with_display(
        &log,
        update_ids.iter().copied(),
        output.stderr,
    );
    if global_opts.use_color() {
        display.set_styles(LineDisplayStyles::colorized());
    }

    let (mut rx, handle) = start_fetch_reports_task(&log, client.clone()).await;
    let mut status_timer = tokio::time::interval(Duration::from_secs(5));
    status_timer.tick().await;

    while !display.stats().is_terminal() {
        tokio::select! {
            res = rx.changed() => {
                if res.is_err() {
                    // The sending end is closed, which means that the task
                    // created by start_fetch_reports_task died... this can
                    // happen either due to a panic or due to an error.
                    match handle.await {
                        Ok(Ok(())) => {
                            // The task exited normally, which means that the
                            // sending end was closed normally. This cannot
                            // happen.
                            bail!("fetch_reports task exited with Ok(()) \
                                   -- this should never happen here");
                        }
                        Ok(Err(error)) => {
                            // The task exited with an error.
                            return Err(error).context("fetch_reports task errored out");
                        }
                        Err(error) => {
                            // The task panicked.
                            return Err(anyhow!(error)).context("fetch_reports task panicked");
                        }
                    }
                }

                let event_reports = rx.borrow_and_update();
                // TODO: parallelize this computation?
                for (id, event_report) in &*event_reports {
                    // If display.add_event_report errors out, it's for a report for a
                    // component we weren't interested in. Ignore it.
                    _ = display.add_event_report(&id, event_report.clone());
                }

                // Print out status for each component ID at the end -- do it here so
                // that we also consider components for which we haven't seen status
                // yet.
                display.write_events()?;
            }
            _ = status_timer.tick() => {
                display.write_stats("Status")?;
            }
        }
    }

    // Show any remaining events.
    display.write_events()?;
    // And also show a summary.
    display.write_stats("Summary")?;

    std::mem::drop(rx);
    handle
        .await
        .context("fetch_reports task panicked after rx dropped")?
        .context("fetch_reports task errored out after rx dropped")?;

    if display.stats().has_failures() {
        bail!("one or more failures occurred");
    }

    Ok(())
}

async fn start_fetch_reports_task(
    log: &Logger,
    client: wicketd_client::Client,
) -> (watch::Receiver<BTreeMap<ComponentId, EventReport>>, JoinHandle<Result<()>>)
{
    // Since reports are always cumulative, we can use a watch receiver here
    // rather than an mpsc receiver. If we start using incremental reports at
    // some point this would need to be changed to be an mpsc receiver.
    let (tx, rx) = watch::channel(BTreeMap::new());
    let log = log.new(slog::o!("task" => "fetch_reports"));

    let handle = tokio::spawn(async move {
        loop {
            let response = client.get_artifacts_and_event_reports().await?;
            let reports = response.into_inner().event_reports;
            let reports = parse_event_report_map(&log, reports);
            if tx.send(reports).is_err() {
                // The receiving end is closed, exit.
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                _ = tx.closed() => {
                    // The receiving end is closed, exit.
                    break;
                }
            }
        }

        Ok(())
    });
    (rx, handle)
}

#[derive(Debug, Args)]
pub(crate) struct StatusArgs {
    /// Return the data as JSON for programmatic use.
    #[clap(long)]
    json: bool,

    /// Read debug-dump output from a file, or - for stdin.
    /// If omitted, fetch data from wicketd.
    #[clap(long, value_name = "FILE")]
    file: Option<Utf8PathBuf>,
}

impl StatusArgs {
    async fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        output: CommandOutput<'_>,
    ) -> Result<ExitCode> {
        // Read the artifact & event reports from wicketd, a file, or stdin.
        let response = if let Some(path) = self.file {
            if path == "-" {
                serde_json::from_reader(BufReader::new(std::io::stdin()))
                    .context("error parsing stdin")?
            } else {
                let file = BufReader::new(
                    std::fs::File::open(&path)
                        .with_context(|| format!("error opening {path}"))?,
                );
                serde_json::from_reader(file)
                    .with_context(|| format!("error parsing {path}"))?
            }
        } else {
            let client =
                create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);
            client
                .get_artifacts_and_event_reports()
                .await
                .context("error fetching artifacts and event reports")?
                .into_inner()
        };

        // Derive the status from events & artifacts.
        let status = build_rack_update_status(&log, response);

        // Write either JSON or a human-readable table to stdout.
        if self.json {
            serde_json::to_writer_pretty(&mut *output.stdout, &status)
                .context("error writing JSON to output")?;
            writeln!(output.stdout).context("error writing to output")?;
        } else {
            write_status_table(output.stdout, &status)?;
        }

        Ok(ExitCode::from(status.state.exit_code()))
    }
}

fn build_rack_update_status(
    log: &Logger,
    response: GetArtifactsAndEventReportsResponse,
) -> RackUpdateStatus {
    let artifacts: Vec<ArtifactId> = response
        .artifacts
        .into_iter()
        .map(|a| a.artifact_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let event_reports = parse_event_report_map(log, response.event_reports);

    let components: Vec<ComponentUpdateStatus> = event_reports
        .iter()
        .map(|(&id, report)| {
            let mut buffer = EventBuffer::default();
            buffer.add_event_report(report.clone());

            // Derive the ComponentUpdateStatus status from the output of
            // update-engine's ExecutionSummary, a rollup of all events.
            match buffer.root_execution_summary() {
                None => ComponentUpdateStatus {
                    id: id.into(),
                    state: UpdateState::NotStarted,
                    step_index: None,
                    total_steps: None,
                    elapsed_secs: None,
                },
                Some(summary) => {
                    let (state, current_step_index, elapsed_secs) =
                        match &summary.execution_status {
                            ExecutionStatus::NotStarted => {
                                (UpdateState::NotStarted, None, None)
                            }
                            ExecutionStatus::Running {
                                step_key,
                                root_total_elapsed,
                            } => (
                                UpdateState::InProgress,
                                Some(step_key.index),
                                Some(root_total_elapsed.as_secs_f64()),
                            ),
                            ExecutionStatus::Terminal(info) => {
                                let state = match info.kind {
                                    TerminalKind::Completed => {
                                        UpdateState::Completed
                                    }
                                    TerminalKind::Failed => UpdateState::Failed,
                                    TerminalKind::Aborted => {
                                        UpdateState::Aborted
                                    }
                                };
                                (
                                    state,
                                    Some(info.step_key.index),
                                    info.root_total_elapsed
                                        .map(|d| d.as_secs_f64()),
                                )
                            }
                        };
                    ComponentUpdateStatus {
                        id: id.into(),
                        state,
                        step_index: current_step_index,
                        total_steps: Some(summary.total_steps),
                        elapsed_secs,
                    }
                }
            }
        })
        .collect();

    let component_states: Vec<UpdateState> =
        components.iter().map(|c| c.state).collect();
    let state = rollup_update_state(&component_states);

    RackUpdateStatus {
        state,
        system_version: response.system_version,
        artifacts,
        components,
    }
}

/// Write a human-readable status table to `out`.
fn write_status_table(
    out: &mut dyn Write,
    status: &RackUpdateStatus,
) -> Result<()> {
    #[derive(tabled::Tabled)]
    #[tabled(rename_all = "UPPERCASE")]
    struct ArtifactRow {
        name: String,
        kind: String,
        version: String,
    }

    #[derive(tabled::Tabled)]
    #[tabled(rename_all = "UPPERCASE")]
    struct ComponentRow {
        #[tabled(rename = "TYPE")]
        type_: String,
        slot: u16,
        state: String,
        progress: String,
        elapsed: String,
    }

    // System version and artifacts.
    writeln!(out, "State: {}\n", status.state)?;
    writeln!(
        out,
        "System version: {}",
        status
            .system_version
            .as_ref()
            .map(|v| v.to_string())
            .as_deref()
            .unwrap_or("(none)")
    )?;

    let artifact_rows: Vec<ArtifactRow> = status
        .artifacts
        .iter()
        .map(|a| ArtifactRow {
            name: a.name.clone(),
            kind: a.kind.to_string(),
            version: a.version.to_string(),
        })
        .collect();
    let artifact_table = tabled::Table::new(artifact_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 2, 0, 0))
        .to_string();
    writeln!(out, "{artifact_table}\n")?;

    // Component table.
    let mut n_completed = 0;
    let mut n_failed = 0;
    let mut n_aborted = 0;
    let mut n_inprogress = 0;
    let mut n_notstarted = 0;

    let component_rows: Vec<ComponentRow> = status
        .components
        .iter()
        .map(|c| {
            let progress = match (c.step_index, c.total_steps) {
                (Some(i), Some(t)) => format!("{}/{}", i + 1, t),
                (None, Some(t)) => format!("-/{t}"),
                _ => "-".to_string(),
            };
            let elapsed = match c.elapsed_secs {
                Some(secs) => {
                    let total = secs as u64;
                    format!(
                        "{:02}:{:02}:{:02}",
                        total / 3600,
                        (total % 3600) / 60,
                        total % 60
                    )
                }
                None => "-".to_string(),
            };

            match c.state {
                UpdateState::Completed => n_completed += 1,
                UpdateState::Failed => n_failed += 1,
                UpdateState::Aborted => n_aborted += 1,
                UpdateState::InProgress => n_inprogress += 1,
                UpdateState::NotStarted => n_notstarted += 1,
            }

            ComponentRow {
                type_: c.id.type_.to_string(),
                slot: c.id.slot,
                state: c.state.to_string(),
                progress,
                elapsed,
            }
        })
        .collect();

    let component_table = tabled::Table::new(component_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 2, 0, 0))
        .to_string();
    writeln!(out, "{component_table}")?;

    writeln!(
        out,
        "\n{n_completed} completed, {n_failed} failed, {n_aborted} aborted, \
         {n_inprogress} in progress, {n_notstarted} not started",
    )?;

    Ok(())
}

#[derive(Debug, Args)]
pub(crate) struct ClearArgs {
    #[clap(flatten)]
    component_ids: ComponentIdSelector,
    #[clap(long, value_name = "FORMAT", value_enum, default_value_t = MessageFormat::Human)]
    message_format: MessageFormat,
}

impl ClearArgs {
    async fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        global_opts: GlobalOpts,
        output: CommandOutput<'_>,
    ) -> Result<()> {
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        let update_ids = self.component_ids.to_component_ids()?;
        let response =
            do_clear_update_state(client, update_ids, global_opts).await;

        match self.message_format {
            MessageFormat::Human => {
                let response = response?;
                let cleared = response
                    .cleared
                    .iter()
                    .map(|sp| {
                        ComponentId::from_sp_type_and_slot(sp.type_, sp.slot)
                            .map(|id| id.to_string())
                    })
                    .collect::<Result<Vec<_>>>()
                    .context("unknown component ID returned in response")?;
                let no_update_data = response
                    .no_update_data
                    .iter()
                    .map(|sp| {
                        ComponentId::from_sp_type_and_slot(sp.type_, sp.slot)
                            .map(|id| id.to_string())
                    })
                    .collect::<Result<Vec<_>>>()
                    .context("unknown component ID returned in response")?;

                if !cleared.is_empty() {
                    slog::info!(
                        log,
                        "cleared update state for {} components: {}",
                        cleared.len(),
                        cleared.join(", ")
                    );
                }
                if !no_update_data.is_empty() {
                    slog::info!(
                        log,
                        "no update data found for {} components: {}",
                        no_update_data.len(),
                        no_update_data.join(", ")
                    );
                }
            }
            MessageFormat::Json => {
                let response =
                    response.map_err(|error| NestedError::new(error.as_ref()));
                // Return the response as a JSON object.
                serde_json::to_writer_pretty(output.stdout, &response)
                    .context("error writing to output")?;
                if response.is_err() {
                    bail!("error clearing update state");
                }
            }
        }

        Ok(())
    }
}

async fn do_clear_update_state(
    client: wicketd_client::Client,
    update_ids: BTreeSet<ComponentId>,
    _global_opts: GlobalOpts,
) -> Result<ClearUpdateStateResponse> {
    let options =
        CreateClearUpdateStateOptions {}.to_clear_update_state_options()?;
    let params = ClearUpdateStateParams {
        targets: update_ids.iter().copied().map(Into::into).collect(),
        options,
    };

    let result = client
        .post_clear_update_state(&params)
        .await
        .context("error calling clear_update_state")?;
    let response = result.into_inner();
    Ok(response)
}

#[derive(Debug, Args)]
pub(crate) struct DumpArgs {
    /// Pretty-print JSON output.
    #[clap(long)]
    pretty: bool,
}

impl DumpArgs {
    async fn exec(self, log: Logger, wicketd_addr: SocketAddrV6) -> Result<()> {
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        let response = client
            .get_artifacts_and_event_reports()
            .await
            .context("error calling get_artifacts_and_event_reports")?;
        let response = response.into_inner();

        // Return the response as a JSON object.
        if self.pretty {
            serde_json::to_writer_pretty(std::io::stdout(), &response)
                .context("error writing to stdout")?;
        } else {
            serde_json::to_writer(std::io::stdout(), &response)
                .context("error writing to stdout")?;
        }
        Ok(())
    }
}

#[derive(Debug, Args)]
pub(crate) struct ReplayArgs {
    /// The dump file to replay.
    ///
    /// This should be the output of `rack-update debug-dump`, or something
    /// like <curl http://localhost:12226/artifacts-and-event-reports>.
    file: Utf8PathBuf,

    /// How to feed events into the display.
    #[clap(long, value_enum, default_value_t)]
    strategy: ReplayStrategy,

    #[clap(flatten)]
    component_ids: ComponentIdSelector,
}

impl ReplayArgs {
    fn exec(
        self,
        log: Logger,
        global_opts: GlobalOpts,
        output: CommandOutput<'_>,
    ) -> Result<()> {
        let update_ids = self.component_ids.to_component_ids()?;
        let mut display = GroupDisplay::new_with_display(
            &log,
            update_ids.iter().copied(),
            output.stderr,
        );
        if global_opts.use_color() {
            display.set_styles(LineDisplayStyles::colorized());
        }

        let file = BufReader::new(
            std::fs::File::open(&self.file)
                .with_context(|| format!("error opening {}", self.file))?,
        );
        let response: GetArtifactsAndEventReportsResponse =
            serde_json::from_reader(file)?;
        let event_reports =
            parse_event_report_map(&log, response.event_reports);

        self.strategy.execute(display, event_reports)?;

        Ok(())
    }
}

#[derive(Clone, Copy, Default, Eq, PartialEq, Hash, Debug, ValueEnum)]
enum ReplayStrategy {
    /// Feed all events into the buffer immediately.
    #[default]
    Oneshot,

    /// Feed events into the buffer one at a time.
    Incremental,

    /// Feed events into the buffer as 0, 0..1, 0..2, 0..3 etc.
    Idempotent,
}

impl ReplayStrategy {
    fn execute(
        self,
        mut display: GroupDisplay<
            ComponentId,
            &mut dyn Write,
            WicketdEngineSpec,
        >,
        event_reports: BTreeMap<ComponentId, EventReport>,
    ) -> Result<()> {
        match self {
            ReplayStrategy::Oneshot => {
                // TODO: parallelize this computation?
                for (id, event_report) in event_reports {
                    // If display.add_event_report errors out, it's for a report for a
                    // component we weren't interested in. Ignore it.
                    _ = display.add_event_report(&id, event_report);
                }

                display.write_events()?;
            }
            ReplayStrategy::Incremental => {
                for (id, event_report) in &event_reports {
                    let mut buffer = EventBuffer::default();
                    let mut last_seen = None;
                    for event in &event_report.step_events {
                        buffer.add_step_event(event.clone());
                        let report =
                            buffer.generate_report_since(&mut last_seen);

                        // If display.add_event_report errors out, it's for a report for a
                        // component we weren't interested in. Ignore it.
                        _ = display.add_event_report(&id, report);

                        display.write_events()?;
                    }
                }
            }
            ReplayStrategy::Idempotent => {
                for (id, event_report) in &event_reports {
                    let mut buffer = EventBuffer::default();
                    for event in &event_report.step_events {
                        buffer.add_step_event(event.clone());
                        let report = buffer.generate_report();

                        // If display.add_event_report errors out, it's for a report for a
                        // component we weren't interested in. Ignore it.
                        _ = display.add_event_report(&id, report);

                        display.write_events()?;
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug, ValueEnum)]
enum MessageFormat {
    Human,
    Json,
}

/// Command-line arguments for selecting component IDs.
#[derive(Debug, Args)]
#[clap(next_help_heading = "Component selectors")]
struct ComponentIdSelector {
    /// The sleds to operate on.
    #[clap(long, value_delimiter = ',')]
    sled: Vec<u8>,

    /// The switches to operate on.
    #[clap(long, value_delimiter = ',')]
    switch: Vec<u8>,

    /// The PSCs to operate on.
    #[clap(long, value_delimiter = ',')]
    psc: Vec<u8>,
}

impl ComponentIdSelector {
    /// Validates that all the sleds, switches, and PSCs are reasonable (though
    /// they might not exist on the actual hardware), then return the set of
    /// selected component IDs.
    fn to_component_ids(&self) -> Result<BTreeSet<ComponentId>> {
        let mut component_ids = BTreeSet::new();
        for sled in &self.sled {
            component_ids.insert(ComponentId::new_sled(*sled)?);
        }
        for switch in &self.switch {
            component_ids.insert(ComponentId::new_switch(*switch)?);
        }
        for psc in &self.psc {
            component_ids.insert(ComponentId::new_psc(*psc)?);
        }
        if component_ids.is_empty() {
            bail!(
                "at least one component ID must be selected via --sled, --switch or --psc"
            );
        }

        Ok(component_ids)
    }
}
