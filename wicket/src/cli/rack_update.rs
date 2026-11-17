// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Command-line driven rack update.
//!
//! This is an alternative to using the Wicket UI to perform a rack update.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufReader, Read, Write},
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
    wicketd::{WicketdAddrs, create_commission_client, create_wicketd_client},
};
use anyhow::{Context, Result, anyhow, bail};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Args, Subcommand, ValueEnum};
use oxide_update_engine_display::{GroupDisplay, LineDisplayStyles};
use oxide_update_engine_types::buffer::EventBuffer;
use oxide_update_engine_types::spec::SerializableError;
use oxide_versioned_envelope::{ReadOutput, WriteEnvelope};
use slog::Logger;
use tokio::{sync::watch, task::JoinHandle};
use wicket_cli_types::rack_update::{
    RackUpdateStatus, StepPosition, UpdateProgressExt, UpdateStateExt,
};
use wicket_common::{
    WICKETD_TIMEOUT,
    update_events::{EventReport, WicketdEngineSpec},
};
use wicketd_client::types::{
    ClearUpdateStateParams, GetArtifactsAndEventReportsResponse,
    StartUpdateParams,
};
use wicketd_commission_types::inventory::SpIdentifier;
use wicketd_commission_types::update::{
    ClearUpdateStateResponse, UpdateTargets,
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
        addrs: WicketdAddrs,
        global_opts: GlobalOpts,
        output: CommandOutput<'_>,
    ) -> Result<ExitCode> {
        match self {
            RackUpdateArgs::Start(args) => {
                args.exec(log, addrs, global_opts, output).await?;
                Ok(ExitCode::SUCCESS)
            }
            RackUpdateArgs::Attach(args) => {
                args.exec(log, addrs, global_opts, output).await?;
                Ok(ExitCode::SUCCESS)
            }
            RackUpdateArgs::Status(args) => args.exec(log, addrs, output).await,
            RackUpdateArgs::Clear(args) => {
                args.exec(log, addrs, global_opts, output).await?;
                Ok(ExitCode::SUCCESS)
            }
            RackUpdateArgs::DebugDump(args) => {
                args.exec(log, addrs).await?;
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
        addrs: WicketdAddrs,
        global_opts: GlobalOpts,
        output: CommandOutput<'_>,
    ) -> Result<()> {
        let client =
            create_wicketd_client(&log, addrs.wicketd, WICKETD_TIMEOUT);

        let update_ids = self.component_ids.to_component_ids()?;
        let options = CreateStartUpdateOptions {
            force_update_rot_bootloader: self.force_update_rot_bootloader,
            force_update_rot: self.force_update_rot,
            force_update_sp: self.force_update_sp,
        }
        .to_start_update_options()?;

        let num_update_ids = update_ids.len();

        let targets = UpdateTargets::new(
            update_ids.iter().copied().map(Into::into).collect(),
        )
        .context("error starting update")?;
        let params = StartUpdateParams { targets, options };

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
        addrs: WicketdAddrs,
        global_opts: GlobalOpts,
        output: CommandOutput<'_>,
    ) -> Result<()> {
        let client =
            create_wicketd_client(&log, addrs.wicketd, WICKETD_TIMEOUT);

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
    #[clap(flatten)]
    component_ids: ComponentIdSelector,

    /// Return the data as JSON for programmatic use.
    #[clap(long)]
    json: bool,

    /// Read this command's own `--json` output from a file, or - for stdin.
    /// If omitted, fetch data from wicketd.
    ///
    /// Cannot be combined with `--sled`, `--switch`, or `--psc`.
    #[clap(long, value_name = "FILE", conflicts_with = "ComponentIdSelector")]
    file: Option<Utf8PathBuf>,
}

impl StatusArgs {
    async fn exec(
        self,
        log: Logger,
        addrs: WicketdAddrs,
        output: CommandOutput<'_>,
    ) -> Result<ExitCode> {
        let status = match &self.file {
            Some(path) => read_status_file(path)?,
            None => {
                // Resolve the selector before performing I/O, so an unusable
                // --sled/--switch/--psc fails straight away.
                let selected = self.component_ids.to_selected_sps()?;
                fetch_status(&log, addrs.commission, selected).await?
            }
        };

        let exit_code = ExitCode::from(status.rollup().exit_code());

        // Write either JSON or a human-readable table to stdout.
        if self.json {
            let envelope = WriteEnvelope::new(&status);
            serde_json::to_writer_pretty(&mut *output.stdout, &envelope)
                .context("error writing JSON to output")?;
            writeln!(output.stdout).context("error writing to output")?;
        } else {
            write_status_table(output.stdout, &status)
                .context("error writing status table to output")?;
        }

        Ok(exit_code)
    }
}

async fn fetch_status(
    log: &Logger,
    commission_addr: SocketAddrV6,
    selected: Option<BTreeSet<SpIdentifier>>,
) -> Result<RackUpdateStatus> {
    let client =
        create_commission_client(log, commission_addr, WICKETD_TIMEOUT);

    let mut update_progress = client
        .get_update_progress()
        .await
        .map_err(commission_error)
        .with_context(|| {
            format!(
                "error fetching the update progress \
                 from the commission API at {commission_addr}"
            )
        })?
        .into_inner();

    if let Some(selected) = selected {
        let missing: Vec<String> = selected
            .iter()
            .filter(|sp| !update_progress.sps.contains_key(*sp))
            .map(|sp| format!("{} {}", sp.typ, sp.slot))
            .collect();
        if !missing.is_empty() {
            slog::warn!(
                log,
                "no update progress for selected components: {}",
                missing.join(", ")
            );
        }
        update_progress.sps.retain(|sp| selected.contains(&sp.sp));
    }

    Ok(RackUpdateStatus::from_latest(update_progress))
}

/// Converts a commission client error into an `anyhow::Error`.
fn commission_error(
    error: wicketd_commission_client::ClientError,
) -> anyhow::Error {
    // XXX This mirrors wicketd's ba_lockstep_error_to_http and has a workaround
    // for the same reason. We should consider fixing this in progenitor (or
    // progenitor-extras?).
    use wicketd_commission_client::Error as CommissionError;

    match &error {
        CommissionError::ErrorResponse(rv) => anyhow!(
            "wicketd returned {} (request ID {}): {}",
            rv.status(),
            rv.request_id,
            rv.message
        ),
        CommissionError::InvalidRequest(_)
        | CommissionError::CommunicationError(_)
        | CommissionError::InvalidUpgrade(_)
        | CommissionError::ResponseBodyError(_)
        | CommissionError::InvalidResponsePayload(_, _)
        | CommissionError::UnexpectedResponse(_)
        | CommissionError::Custom(_) => {
            // Progenitor's alternate formatter prints the whole error chain
            // once -- wrapping the error itself in anyhow would print its first
            // cause twice.
            anyhow!("{error:#}")
        }
    }
}

fn read_status_file(path: &Utf8Path) -> Result<RackUpdateStatus> {
    if path == "-" {
        read_status(BufReader::new(std::io::stdin()), "stdin")
    } else {
        let file = std::fs::File::open(path)
            .with_context(|| format!("error opening {path}"))?;
        read_status(BufReader::new(file), path.as_str())
    }
}

fn read_status(
    mut reader: impl Read,
    source: &str,
) -> Result<RackUpdateStatus> {
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .with_context(|| format!("error reading {source}"))?;

    oxide_versioned_envelope::read_json::<RackUpdateStatus>(&bytes)
        .map(ReadOutput::into_value)
        .with_context(|| {
            format!(
                "error reading rack-update status JSON from {source} \
                 (the output of `rack-update status --json`)"
            )
        })
}

/// Write a human-readable status table to `out`.
fn write_status_table(
    out: &mut dyn Write,
    status: &RackUpdateStatus,
) -> Result<()> {
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

    let counts = status.state_counts();
    writeln!(out, "State: {}\n", counts.rollup())?;
    writeln!(
        out,
        "System version: {}\n",
        format_system_version(
            status.update_progress.repository.system_version.as_ref()
        )
    )?;

    // Component table. The rows are in `SpIdentifier` order.
    let component_rows: Vec<ComponentRow> = status
        .update_progress
        .sps
        .iter()
        .map(|sp| ComponentRow {
            type_: sp.sp.typ.to_string(),
            slot: sp.sp.slot,
            state: sp.progress.state.label().to_owned(),
            progress: format_step_position(sp.progress.step_position()),
            elapsed: format_elapsed(sp.progress.state.elapsed()),
        })
        .collect();

    let component_table = tabled::Table::new(component_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 2, 0, 0))
        .to_string();
    writeln!(out, "{component_table}")?;

    writeln!(
        out,
        "\n{} completed, {} failed, {} aborted, {} in progress, {} not started",
        counts.completed,
        counts.failed,
        counts.aborted,
        counts.in_progress,
        counts.not_started,
    )?;

    for sp in status.update_progress.sps.iter() {
        if let Some(message) = sp.progress.state.terminal_message() {
            writeln!(
                out,
                "\n{} {} ({}): {}",
                sp.sp.typ,
                sp.sp.slot,
                sp.progress.state.label(),
                message,
            )?;
        }
    }

    Ok(())
}

fn format_system_version(system_version: Option<&semver::Version>) -> String {
    match system_version {
        Some(version) => version.to_string(),
        None => "(none)".to_owned(),
    }
}

fn format_step_position(position: Option<StepPosition>) -> String {
    match position {
        Some(StepPosition { current, total }) => format!("{current}/{total}"),
        None => "-".to_owned(),
    }
}

fn format_elapsed(elapsed: Option<Duration>) -> String {
    match elapsed {
        Some(elapsed) => {
            let total = elapsed.as_secs();
            format!(
                "{:02}:{:02}:{:02}",
                total / 3600,
                (total % 3600) / 60,
                total % 60
            )
        }
        None => "-".to_owned(),
    }
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
        addrs: WicketdAddrs,
        global_opts: GlobalOpts,
        output: CommandOutput<'_>,
    ) -> Result<()> {
        let client =
            create_wicketd_client(&log, addrs.wicketd, WICKETD_TIMEOUT);

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
                        ComponentId::from_sp_type_and_slot(sp.typ, sp.slot)
                            .map(|id| id.to_string())
                    })
                    .collect::<Result<Vec<_>>>()
                    .context("unknown component ID returned in response")?;
                let no_update_data = response
                    .no_update_data
                    .iter()
                    .map(|sp| {
                        ComponentId::from_sp_type_and_slot(sp.typ, sp.slot)
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
                let response = response
                    .map_err(|error| SerializableError::new(error.as_ref()));
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
    let targets = UpdateTargets::new(
        update_ids.iter().copied().map(Into::into).collect(),
    )
    .context("error clearing update state")?;
    let params = ClearUpdateStateParams { targets, options };

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
    async fn exec(self, log: Logger, addrs: WicketdAddrs) -> Result<()> {
        let client =
            create_wicketd_client(&log, addrs.wicketd, WICKETD_TIMEOUT);

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

    fn is_empty(&self) -> bool {
        self.sled.is_empty() && self.switch.is_empty() && self.psc.is_empty()
    }

    /// Validate that all the sleds, switches, and PSCs are reasonable (though
    /// they might not exist on the actual hardware), then return the set of
    /// selected [`SpIdentifier`]s.
    ///
    /// Returns `None` if no components are selected.
    fn to_selected_sps(&self) -> Result<Option<BTreeSet<SpIdentifier>>> {
        if self.is_empty() {
            return Ok(None);
        }
        let sps = self
            .to_component_ids()?
            .into_iter()
            .map(SpIdentifier::from)
            .collect();
        Ok(Some(sps))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iddqd::{IdOrdMap, id_ord_map};
    use semver::Version;
    use wicketd_commission_types::inventory::SpType;
    use wicketd_commission_types::update::{
        GetUpdateProgressResponse, RepositoryDescription, RunningProgress,
        SpUpdateProgress, StepOutcome, StepProgress, UpdateProgress,
        UpdateState, UpdateStep, UpdateStepStatus,
    };

    fn step(description: &str, status: UpdateStepStatus) -> UpdateStep {
        UpdateStep {
            description: description.to_owned(),
            status,
            children: Vec::new(),
        }
    }

    fn completed_step(description: &str) -> UpdateStep {
        step(
            description,
            UpdateStepStatus::Completed {
                outcome: StepOutcome::Success { message: None },
            },
        )
    }

    fn not_started_step(description: &str) -> UpdateStep {
        step(description, UpdateStepStatus::NotStarted)
    }

    fn will_not_be_run_step(description: &str, reason: &str) -> UpdateStep {
        step(
            description,
            UpdateStepStatus::WillNotBeRun { reason: reason.to_owned() },
        )
    }

    fn repository() -> RepositoryDescription {
        RepositoryDescription { system_version: Some(Version::new(1, 0, 0)) }
    }

    fn update_progress() -> GetUpdateProgressResponse {
        GetUpdateProgressResponse {
            repository: repository(),
            sps: id_ord_map! {
                sled0_completed(),
                sled1_failed(),
                sled2_waiting_with_steps(),
                switch1_running(),
                power0_aborted(),
                power1_waiting(),
            },
        }
    }

    fn sled0_completed() -> SpUpdateProgress {
        SpUpdateProgress {
            sp: SpIdentifier { typ: SpType::Sled, slot: 0 },
            progress: UpdateProgress {
                state: UpdateState::Completed {
                    elapsed: Some(Duration::from_secs(754)),
                },
                steps: vec![
                    completed_step("Update RoT bootloader"),
                    completed_step("Update RoT"),
                    completed_step("Update SP"),
                    completed_step("Update host OS"),
                ],
            },
        }
    }

    fn sled1_failed() -> SpUpdateProgress {
        SpUpdateProgress {
            sp: SpIdentifier { typ: SpType::Sled, slot: 1 },
            progress: UpdateProgress {
                state: UpdateState::Failed {
                    message: "Get host type: Unknown host type i86pc: \
                              unknown model string \"i86pc\": expected one \
                              of gimlet, cosmo"
                        .to_owned(),
                    elapsed: Some(Duration::from_secs(62)),
                },
                steps: vec![
                    completed_step("Update RoT bootloader"),
                    completed_step("Update RoT"),
                    step(
                        "Get host type",
                        UpdateStepStatus::Failed {
                            message: "Unknown host type i86pc".to_owned(),
                            causes: vec![
                                "unknown model string \"i86pc\"".to_owned(),
                                "expected one of gimlet, cosmo".to_owned(),
                            ],
                        },
                    ),
                    will_not_be_run_step(
                        "Update host OS",
                        "Get host type failed",
                    ),
                ],
            },
        }
    }

    // The running step carries a nested execution, so this also checks that the
    // progress column counts top-level steps only.
    fn switch1_running() -> SpUpdateProgress {
        SpUpdateProgress {
            sp: SpIdentifier { typ: SpType::Switch, slot: 1 },
            progress: UpdateProgress {
                state: UpdateState::Running {
                    elapsed: Duration::from_secs(3661),
                },
                steps: vec![
                    completed_step("Update RoT bootloader"),
                    UpdateStep {
                        description: "Update RoT".to_owned(),
                        status: UpdateStepStatus::Running {
                            progress: RunningProgress::Progress {
                                progress: Some(StepProgress {
                                    current: 1024,
                                    total: Some(4096),
                                    units: "bytes".to_owned(),
                                }),
                            },
                        },
                        children: vec![UpdateProgress {
                            state: UpdateState::Running {
                                elapsed: Duration::from_secs(12),
                            },
                            steps: vec![step(
                                "Write RoT image",
                                UpdateStepStatus::Running {
                                    progress:
                                        RunningProgress::WaitingForProgress,
                                },
                            )],
                        }],
                    },
                    not_started_step("Update SP"),
                ],
            },
        }
    }

    fn power0_aborted() -> SpUpdateProgress {
        SpUpdateProgress {
            sp: SpIdentifier { typ: SpType::Power, slot: 0 },
            progress: UpdateProgress {
                state: UpdateState::Aborted {
                    message: "Update RoT: aborted by operator".to_owned(),
                    elapsed: None,
                },
                steps: vec![
                    completed_step("Update RoT bootloader"),
                    step(
                        "Update RoT",
                        UpdateStepStatus::Aborted {
                            message: "aborted by operator".to_owned(),
                        },
                    ),
                    will_not_be_run_step("Update SP", "Update RoT was aborted"),
                ],
            },
        }
    }

    fn power1_waiting() -> SpUpdateProgress {
        SpUpdateProgress {
            sp: SpIdentifier { typ: SpType::Power, slot: 1 },
            progress: UpdateProgress {
                state: UpdateState::Waiting,
                steps: Vec::new(),
            },
        }
    }

    // An update that has been started but whose steps have not run yet -- in
    // this case, the progress column should read as 1/n rather than -.
    fn sled2_waiting_with_steps() -> SpUpdateProgress {
        SpUpdateProgress {
            sp: SpIdentifier { typ: SpType::Sled, slot: 2 },
            progress: UpdateProgress {
                state: UpdateState::Waiting,
                steps: vec![
                    not_started_step("Update RoT bootloader"),
                    not_started_step("Update RoT"),
                    not_started_step("Update SP"),
                ],
            },
        }
    }

    fn non_empty_status() -> RackUpdateStatus {
        RackUpdateStatus::from_latest(update_progress())
    }

    // These snapshots test the JSON body and the human-readable table of
    // `rack-update status`.

    #[test]
    fn status_json_non_empty() {
        let envelope = WriteEnvelope::new(non_empty_status());
        let json = serde_json::to_string_pretty(&envelope)
            .expect("status serialized to JSON");
        expectorate::assert_contents(
            "tests/output/rack-update-status.json",
            &json,
        );
    }

    #[test]
    fn status_table_non_empty() {
        let mut out = Vec::new();
        write_status_table(&mut out, &non_empty_status())
            .expect("status table written to a Vec");
        expectorate::assert_contents(
            "tests/output/rack-update-status-table.txt",
            &String::from_utf8(out).expect("status table is valid UTF-8"),
        );
    }

    #[test]
    fn status_file_conflicts_with_component_selectors() {
        use clap::Parser;

        for selector in [["--sled", "0"], ["--switch", "1"], ["--psc", "0"]] {
            let args =
                ["wicket", "rack-update", "status", "--file", "saved.json"]
                    .into_iter()
                    .chain(selector);
            let error = crate::cli::ShellApp::try_parse_from(args)
                .expect_err("--file with a component selector was refused");
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::ArgumentConflict,
                "{selector:?}: the selector conflicts with --file: {error}",
            );
            let rendered = error.to_string();
            assert!(
                rendered.contains("--file") && rendered.contains(selector[0]),
                "{selector:?}: the error names both arguments: {rendered}",
            );
        }
    }

    #[test]
    fn status_table_empty() {
        let status = RackUpdateStatus::from_latest(GetUpdateProgressResponse {
            repository: RepositoryDescription { system_version: None },
            sps: IdOrdMap::new(),
        });

        let mut out = Vec::new();
        write_status_table(&mut out, &status)
            .expect("status table written to a Vec");
        expectorate::assert_contents(
            "tests/output/rack-update-status-table-empty.txt",
            &String::from_utf8(out).expect("status table is valid UTF-8"),
        );
    }
}
