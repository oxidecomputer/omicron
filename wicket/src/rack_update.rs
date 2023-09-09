// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Command-line driven rack update.
//!
//! This is an alternative to using the Wicket UI to perform a rack update.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddrV6,
    time::Duration,
};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use debug_ignore::DebugIgnore;
use owo_colors::{OwoColorize, Style};
use slog::Logger;
use update_engine::{ExecutionId, NestedSpec};
use wicket_common::update_events::{
    EventBuffer, EventReport, StepInfo, StepOutcome, StepStatus,
};
use wicketd_client::types::{StartUpdateOptions, StartUpdateParams};

use crate::{
    events::EventReportMap,
    helpers::{get_update_simulated_result, get_update_test_error},
    state::{ComponentId, ParsableComponentId},
    wicketd::{create_wicketd_client, WICKETD_TIMEOUT},
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
    ) -> Result<()> {
        let runtime =
            tokio::runtime::Runtime::new().context("creating tokio runtime")?;
        runtime.block_on(self.exec_impl(log, wicketd_addr))
    }

    async fn exec_impl(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
    ) -> Result<()> {
        match self {
            RackUpdateArgs::Start(args) => args.exec(log, wicketd_addr).await,
            RackUpdateArgs::Attach(args) => args.exec(log, wicketd_addr).await,
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
    async fn exec(self, log: Logger, wicketd_addr: SocketAddrV6) -> Result<()> {
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
            targets: update_ids.into_iter().map(Into::into).collect(),
            options,
        };

        match client.post_start_update(&params).await {
            Ok(_) => {
                slog::info!(log, "Update started for {num_update_ids} targets");
            }
            Err(error) => {
                // Error responses can be printed out more clearly.
                if let wicketd_client::Error::ErrorResponse(error) = &error {
                    slog::error!(
                        log,
                        "Error response from wicketd: {}",
                        error.message
                    );
                    bail!("Received error from wicketd");
                } else {
                    bail!(error);
                }
            }
        }

        if self.detach {
            return Ok(());
        }

        // Now, attach to the update by printing out update logs.
    }
}

#[derive(Debug, Args)]
pub(crate) struct AttachArgs {
    #[clap(flatten)]
    component_ids: ComponentIdSelector,
}

impl AttachArgs {
    async fn exec(self, log: Logger, wicketd_addr: SocketAddrV6) -> Result<()> {
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        let update_ids = self.component_ids.to_component_ids()?;
        do_attach_to_updates(log, client, update_ids).await
    }
}

async fn do_attach_to_updates(
    log: Logger,
    client: wicketd_client::Client,
    update_ids: BTreeSet<ComponentId>,
) -> Result<()> {
    // Maintain an UpdateAttachState for each update ID.
    let mut event_buffers: BTreeMap<_, _> =
        update_ids.iter().map(|id| (*id, AttachState::new())).collect();
    let reporter = AttachReporter::new();

    loop {
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
            let state = event_buffers.get_mut(&id).unwrap();
            if !state.add_event_report(event_report) {
                // This event report is for a different execution ID -- likely
                // means that a new report has been started.
                continue;
            }

            reporter.report(id, state);
        }
    }
}

struct AttachState {
    event_buffer: EventBuffer,
    root_execution_id: Option<ExecutionId>,
    last_seen: Option<usize>,
}

impl AttachState {
    fn new() -> AttachState {
        AttachState {
            event_buffer: EventBuffer::new(8),
            root_execution_id: None,
            last_seen: None,
        }
    }

    fn root_execution_id(&self) -> Option<ExecutionId> {
        self.event_buffer.root_execution_id()
    }

    /// Returns false if the event report was not added because it is from a
    /// different execution ID, and true otherwise.
    fn add_event_report(&mut self, event_report: EventReport) -> bool {
        if let Some(root_execution_id) = self.root_execution_id() {
            if event_report.root_execution_id != Some(root_execution_id) {
                return false;
            }
        } else {
            // This is the first event report with a root execution ID we've
            // seen.
            self.root_execution_id = event_report.root_execution_id;
        }
        self.event_buffer.add_event_report(event_report);
        true
    }

    // fn diff_and_update(&mut self) -> EventReport {
    //     let diff = self.event_buffer.generate_report_since(self.last_seen);
    //     self.last_seen = diff.last_seen;
    //     diff
    // }
}

#[derive(Debug)]
struct AttachReporter<'out> {
    styles: AttachReporterStyles,
    output: DebugIgnore<&'out mut dyn std::io::Write>,
    // TODO: write to an explicit output buffer here?
}

impl<'out> AttachReporter<'out> {
    fn new(output: &'out mut dyn std::io::Write) -> Self {
        Self { styles: Default::default(), output: output.into() }
    }

    fn colorize(&mut self) {
        self.styles.colorize();
    }

    fn report(&self, id: ComponentId, state: &AttachState) {
        let steps = state.event_buffer.steps();
        for (step_key, data) in steps.as_slice() {
            self.report_step_event(
                id,
                data.nest_level(),
                data.step_info(),
                data.step_status(),
            );
        }
    }

    // TODO: diffs against last seen
    fn report_step_event(
        &self,
        id: ComponentId,
        nest_level: usize,
        step_info: &StepInfo<NestedSpec>,
        status: &StepStatus,
    ) -> std::io::Result<()> {
        match status {
            StepStatus::NotStarted => {}
            StepStatus::Running { low_priority, progress_event } => {
                // TODO: report low-priority and progress events here.
            }
            StepStatus::Completed { info } => match info {
                Some(info) => {
                    let mut line = match info.outcome {
                        StepOutcome::Success { message, metadata } => {
                            match message {
                                Some(message) => {
                                    format!(
                                        "{}: {} with message \"{}\"",
                                        step_info
                                            .description
                                            .style(self.styles.meta_style),
                                        "Completed"
                                            .style(self.styles.progress_style),
                                        message,
                                    )
                                }
                                None => {
                                    format!(
                                        "{}: {}",
                                        step_info
                                            .description
                                            .style(self.styles.meta_style),
                                        "Completed"
                                            .style(self.styles.progress_style),
                                    )
                                }
                            }
                        }
                        StepOutcome::Warning { message, .. } => {
                            format!(
                                "{}: {}: \"{}\"",
                                step_info
                                    .description
                                    .style(self.styles.meta_style),
                                "Completed with warning"
                                    .style(self.styles.warning_style),
                                message,
                            )
                        }
                        StepOutcome::Skipped { message, .. } => {
                            format!(
                                "{}: {} with message \"{}\"",
                                step_info
                                    .description
                                    .style(self.styles.meta_style),
                                "Skipped".style(self.styles.skipped_style),
                                message,
                            )
                        }
                    };

                    self.display_line(id, nest_level, &line)?;
                }
                None => {
                    // This means that we don't know what happened to the step
                    // but it did complete.
                    let line = format!(
                        "{}: {} with {}",
                        step_info.description.style(self.styles.meta_style),
                        "Completed".style(self.styles.progress_style),
                        "unknown outcome".style(self.styles.meta_style),
                    );
                    self.display_line(id, nest_level, &line)?;
                }
            },
            StepStatus::Failed { info } => match info {
                Some(info) => match info {},
            },
            update_engine::StepStatus::Aborted { reason, last_progress } => {
                todo!()
            }
            update_engine::StepStatus::WillNotBeRun { reason } => todo!(),
        }

        Ok(())
    }

    fn display_line(
        &self,
        id: ComponentId,
        nest_level: usize,
        line: &str,
    ) -> std::io::Result<()> {
        writeln!(
            self.output,
            "{}{} {}",
            " ".repeat(nest_level * 2),
            id.style(self.styles.meta_style),
            line,
        )
    }
}

#[derive(Debug, Default)]
struct AttachReporterStyles {
    meta_style: Style,
    progress_style: Style,
    warning_style: Style,
    skipped_style: Style,
    retry_style: Style,
}

impl AttachReporterStyles {
    fn colorize(&mut self) {
        self.meta_style = Style::new().bold();
        self.progress_style = Style::new().bold().green();
        self.warning_style = Style::new().bold().yellow();
        self.skipped_style = Style::new().bold().yellow();
        self.retry_style = Style::new().bold().yellow();
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
        for sled in self.sled {
            component_ids.insert(ComponentId::new_sled(sled)?);
        }
        for switch in self.switch {
            component_ids.insert(ComponentId::new_switch(switch)?);
        }
        for psc in self.psc {
            component_ids.insert(ComponentId::new_psc(psc)?);
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
