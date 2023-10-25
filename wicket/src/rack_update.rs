// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Command-line driven rack update.
//!
//! This is an alternative to using the Wicket UI to perform a rack update.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Stdout,
    net::SocketAddrV6,
    time::Duration,
};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use slog::Logger;
use update_engine::{
    display::{LineDisplay, LineDisplayStyles},
    ExecutionTerminalStatus,
};
use wicket_common::update_events::{EventBuffer, EventReport};
use wicketd_client::types::{StartUpdateOptions, StartUpdateParams};

use crate::{
    events::EventReportMap,
    helpers::{get_update_simulated_result, get_update_test_error},
    state::{ComponentId, ParsableComponentId},
    wicketd::{create_wicketd_client, WICKETD_TIMEOUT},
    GlobalOpts,
};

#[derive(Debug, Subcommand)]
pub(crate) enum RackUpdateArgs {
    /// Start a rack update.
    Start(StartRackUpdateArgs),
    /// Attach to a running update.
    Attach(AttachArgs),
}

impl RackUpdateArgs {
    pub(crate) fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        global_opts: GlobalOpts,
    ) -> Result<()> {
        let runtime =
            tokio::runtime::Runtime::new().context("creating tokio runtime")?;
        runtime.block_on(self.exec_impl(log, wicketd_addr, global_opts))
    }

    async fn exec_impl(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        global_opts: GlobalOpts,
    ) -> Result<()> {
        match self {
            RackUpdateArgs::Start(args) => {
                args.exec(log, wicketd_addr, global_opts).await
            }
            RackUpdateArgs::Attach(args) => {
                args.exec(log, wicketd_addr, global_opts).await
            }
        }
    }
}

#[derive(Debug, Args)]
pub(crate) struct StartRackUpdateArgs {
    #[clap(flatten)]
    component_ids: ComponentIdSelector,

    /// Force update the RoT even if the version is the same.
    #[clap(long)]
    force_update_rot: bool,

    /// Force update the SP even if the version is the same.
    #[clap(long)]
    force_update_sp: bool,

    /// Detach after starting the update.
    ///
    /// The `attach` command can be used to reattach to the running update.
    #[clap(short, long)]
    detach: bool,
}

impl StartRackUpdateArgs {
    async fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        global_opts: GlobalOpts,
    ) -> Result<()> {
        // NOTE: This process is not idempotent. Doing so would require using a
        // UUID, and storing that UUID in wicket.
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        let update_ids = self.component_ids.to_component_ids()?;
        let options = CreateStartUpdateOptions {
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
        do_attach_to_updates(log, client, update_ids, global_opts).await?;

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
    ) -> Result<()> {
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        let update_ids = self.component_ids.to_component_ids()?;
        do_attach_to_updates(log, client, update_ids, global_opts).await
    }
}

async fn do_attach_to_updates(
    log: Logger,
    client: wicketd_client::Client,
    update_ids: BTreeSet<ComponentId>,
    global_opts: GlobalOpts,
) -> Result<()> {
    // Maintain state for each update ID.
    let mut states: BTreeMap<_, _> = update_ids
        .iter()
        .map(|id| (*id, AttachState::new(*id, global_opts.use_color())))
        .collect();
    let mut results = AttachResults::default();

    // results.terminal is always a subset of the keys in states.
    while states.len() > results.terminal.len() {
        // TODO: Ideally we'd not fetch the full event report each time and just
        // get diffs from the last report. The capability for this exists in the
        // update-engine, it just needs to be plumbed through the API.
        let response = match client.get_artifacts_and_event_reports().await {
            Ok(response) => response,
            Err(error) => {
                if let wicketd_client::Error::ErrorResponse(error) = &error {
                    slog::error!(
                        log,
                        "Error response from wicketd: {}",
                        error.message
                    );
                } else {
                    slog::error!(log, "Error from wicketd: {}", error);
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let event_reports = response.into_inner().event_reports;
        let event_reports = parse_event_report_map(&log, event_reports);
        // TODO: parallelize this computation?
        for (id, event_report) in event_reports {
            let Some(state) = states.get_mut(&id) else {
                // We were never interested in this component ID, so ignore this
                // report.
                continue;
            };
            state.add_event_report(event_report);
        }

        // Print out status for each component ID at the end -- do it here so
        // that we also consider components for which we haven't seen status
        // yet.
        for (id, state) in &mut states {
            state.display_and_update()?;
            if let AttachStateKind::Terminal = state.kind {
                terminal.insert(*id);
            }
        }
    }

    slog::info!(log, "All updates completed"; "num_updates" => update_ids.len());

    Ok(())
}

struct AttachState {
    kind: AttachStateKind,
    line_display: LineDisplay<Stdout>,
}

impl AttachState {
    fn new(id: ComponentId, use_color: bool) -> AttachState {
        let mut line_display = LineDisplay::new(std::io::stdout());
        if use_color {
            line_display.set_styles(LineDisplayStyles::colorized());
        }

        line_display.set_prefix(id.to_string_standard_case());
        AttachState {
            kind: AttachStateKind::NotStarted { displayed: false },
            line_display,
        }
    }

    /// Adds an event report.
    fn add_event_report(&mut self, event_report: EventReport) {
        let was_running = match &self.kind {
            AttachStateKind::NotStarted { .. } => {
                self.kind = AttachStateKind::Running {
                    event_buffer: EventBuffer::new(8),
                };
                false
            }
            AttachStateKind::Running { .. } => true,
            AttachStateKind::Terminal { .. }
            | AttachStateKind::Overwritten { .. } => {
                // This update has already completed -- assume that the event
                // buffer is for a new update, which we don't show.
                return;
            }
        };

        let AttachStateKind::Running { event_buffer } = &mut self.kind else {
            unreachable!("other branches were handled above");
        };

        if let Some(root_execution_id) = event_buffer.root_execution_id() {
            if event_report.root_execution_id != Some(root_execution_id) {
                // The report is for a different execution ID -- assume that
                // this event is completed and mark our current execution as
                // completed.
                self.kind = AttachStateKind::Overwritten { displayed: false };
                return;
            }
        }

        event_buffer.add_event_report(event_report);
    }

    /// Displays the latest state that must be shown, and updates internal state
    /// accordingly. Returns true if this has been marked completed.
    fn display_and_update(&mut self) -> std::io::Result<()> {
        match &mut self.kind {
            AttachStateKind::NotStarted { displayed } => {
                if !*displayed {
                    self.line_display
                        .println("Update not started, waiting...")?;
                    *displayed = true;
                }
            }
            AttachStateKind::Running { event_buffer } => {
                self.line_display.display_event_buffer(event_buffer)?;
                // Is this update done?
                if let Some((status, total_elapsed)) =
                    event_buffer.root_terminal_status()
                {
                    self.line_display
                        .print_terminal_status(&status, total_elapsed)?;
                    self.kind = AttachStateKind::Terminal { status };
                }
            }
            AttachStateKind::Terminal { .. } => {
                // Nothing to do, the terminal status was already printed above.
            }
            AttachStateKind::Overwritten { displayed } => {
                if !*displayed {
                    self.line_display.println(
                        "Update overwritten (a different update was started)\
                                assuming failure",
                    )?;
                    *displayed = true;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
enum AttachStateKind {
    NotStarted { displayed: bool },
    Running { event_buffer: EventBuffer },
    Terminal { status: ExecutionTerminalStatus },
    Overwritten { displayed: bool },
}

struct AttachResult

#[derive(Debug, Default)]
struct AttachResults {
    terminal: BTreeSet<ComponentId>,
    stats: AttachStats,
}

#[derive(Debug, Default)]
struct AttachStats {
    /// The number of updates currently running.
    running: usize,

    /// The number of components for which the update was completed.
    completed: usize,

    /// The number of components for which the update failed.
    failed: usize,

    /// The number of components for which the update was aborted.
    aborted: usize,
}

impl Stats {
    fn record(&mut self, status: &ExecutionTerminalStatus) {
        match status {
            ExecutionTerminalStatus::Completed { .. } => self.completed += 1,
            ExecutionTerminalStatus::Failed { .. } => self.failed += 1,
            ExecutionTerminalStatus::Aborted { .. } => self.aborted += 1,
        }
    }
}

/// Command-line arguments for selecting component IDs.
#[derive(Debug, Args)]
#[clap(next_help_heading = "COMPONENT SELECTORS")]
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
            bail!("at least one component ID must be selected via --sled, --switch or --psc");
        }

        Ok(component_ids)
    }
}

pub(crate) struct CreateStartUpdateOptions {
    pub(crate) force_update_rot: bool,
    pub(crate) force_update_sp: bool,
}

impl CreateStartUpdateOptions {
    pub(crate) fn to_start_update_options(&self) -> Result<StartUpdateOptions> {
        let test_error =
            get_update_test_error("WICKET_TEST_START_UPDATE_ERROR")?;

        // This is a debug environment variable used to
        // add a test step.
        let test_step_seconds =
            std::env::var("WICKET_UPDATE_TEST_STEP_SECONDS").ok().map(|v| {
                v.parse().expect(
                    "parsed WICKET_UPDATE_TEST_STEP_SECONDS \
                            as a u64",
                )
            });

        let test_simulate_rot_result = get_update_simulated_result(
            "WICKET_UPDATE_TEST_SIMULATE_ROT_RESULT",
        )?;
        let test_simulate_sp_result = get_update_simulated_result(
            "WICKET_UPDATE_TEST_SIMULATE_SP_RESULT",
        )?;

        Ok(StartUpdateOptions {
            test_error,
            test_step_seconds,
            test_simulate_rot_result,
            test_simulate_sp_result,
            skip_rot_version_check: self.force_update_rot,
            skip_sp_version_check: self.force_update_sp,
        })
    }
}

/// Converts an `EventReportMap` to a map by component ID.
pub(crate) fn parse_event_report_map(
    log: &Logger,
    reports: EventReportMap,
) -> BTreeMap<ComponentId, EventReport> {
    let mut component_id_map = BTreeMap::new();
    for (sp_type, logs) in reports {
        for (i, event_report) in logs {
            let Ok(id) = ComponentId::try_from(ParsableComponentId {
                sp_type: &sp_type,
                i: &i,
            }) else {
                slog::warn!(
                    log,
                    "Invalid ComponentId in EventReportMap: {} {}",
                    &sp_type,
                    &i
                );
                continue;
            };
            component_id_map.insert(id, event_report);
        }
    }

    component_id_map
}
