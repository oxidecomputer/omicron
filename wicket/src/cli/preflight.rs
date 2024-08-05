// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for preflight checks via wicketd.

use crate::wicketd::create_wicketd_client;
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use clap::Subcommand;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use owo_colors::OwoColorize;
use slog::Logger;
use std::borrow::Cow;
use std::fmt::Display;
use std::net::SocketAddrV6;
use std::time::Duration;
use wicket_common::preflight_check::StepEvent;
use wicket_common::preflight_check::StepEventKind;
use wicket_common::preflight_check::StepInfo;
use wicket_common::preflight_check::StepInfoWithMetadata;
use wicket_common::preflight_check::StepOutcome;
use wicketd_client::types::PreflightUplinkCheckOptions;
use wicketd_client::Client;

const WICKETD_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Subcommand)]
pub(crate) enum PreflightArgs {
    /// Run a preflight check on the uplink configuration to ensure connectivity
    /// to external DNS and NTP servers.
    Uplink {
        /// Optional DNS name to query.
        ///
        /// The preflight check will query for the IPs of any NTP servers that
        /// are provided by name. If all NTP servers are listed by IP, we will
        /// either query for this name (if provided) or `oxide.computer` to
        /// check for DNS connectivity.
        #[clap(long)]
        query_dns: Option<String>,
    },
    /// Display progress of a previously-started uplink config check.
    UplinkStatus,
}

impl PreflightArgs {
    pub(crate) async fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
    ) -> Result<()> {
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        match self {
            Self::Uplink { query_dns } => {
                let options = PreflightUplinkCheckOptions {
                    dns_name_to_query: query_dns,
                };
                client
                    .post_start_preflight_uplink_check(&options)
                    .await
                    .context("failed to start uplink preflight check")?;

                // Immediately transition into displaying the progress
                poll_uplink_status_until_complete(client).await
            }
            Self::UplinkStatus => {
                poll_uplink_status_until_complete(client).await
            }
        }
    }
}

async fn poll_uplink_status_until_complete(client: Client) -> Result<()> {
    // We have to pick a max width for our progress "bar" (actually a spinner
    // with a message). Let's pick something slightly smaller than 80 columns.
    const PROGRESS_BAR_WIDTH: u64 = 72;

    let mut last_seen = None;
    let mut all_steps = None;
    let mut progress_bar = None;
    let mut delay = tokio::time::interval(Duration::from_millis(100));
    let mut execution_failed = false;

    loop {
        delay.tick().await;
        let report = client
            .get_preflight_uplink_report()
            .await
            .context("failed to get uplink preflight progress")?
            .into_inner();

        print_completed_steps(
            report.step_events,
            &mut last_seen,
            &mut all_steps,
            &mut progress_bar,
            &mut execution_failed,
        )?;

        // Display or tick the progress spinner for the in-progress step, if
        // there is one.
        let Some(last_seen) = last_seen else {
            continue;
        };
        let Some(all_steps) = all_steps.as_ref() else {
            continue;
        };

        // Are we done?
        if execution_failed
            || all_steps.last().map(|step| step.index) == Some(last_seen)
        {
            return Ok(());
        }

        let Some(active_step) =
            all_steps.iter().find(|step| step.index == last_seen + 1)
        else {
            continue;
        };

        // Is this a new active step that needs a new progress bar?
        if progress_bar.is_none() {
            progress_bar = Some(
                ProgressBar::new(PROGRESS_BAR_WIDTH)
                    .with_style(
                        ProgressStyle::with_template("{spinner:.green} {msg}")
                            .unwrap(),
                    )
                    .with_message(active_step.description.clone()),
            );
        }

        if let Some(pb) = progress_bar.as_ref() {
            pb.tick();
        }
    }
}

fn print_completed_steps(
    step_events: Vec<StepEvent>,
    last_seen: &mut Option<usize>,
    all_steps: &mut Option<Vec<StepInfo>>,
    progress_bar: &mut Option<ProgressBar>,
    execution_failed: &mut bool,
) -> Result<()> {
    for step in step_events {
        match step.kind {
            // Our uplink preflight check doesn't emit any of these step kinds:
            // it has no retries, nested engines, or support for abort.
            StepEventKind::NoStepsDefined
            | StepEventKind::ProgressReset { .. }
            | StepEventKind::AttemptRetry { .. }
            | StepEventKind::ExecutionAborted { .. }
            | StepEventKind::Nested { .. }
            | StepEventKind::Unknown => {
                bail!("unexpected preflight step kind {:?}", step.kind);
            }
            // Once (if?) we're added to the wicket TUI, this kind will be used
            // to populate the steps-to-be-run. For our print-as-it-happens
            // display, we record the steps but don't display anything.
            StepEventKind::ExecutionStarted { steps, .. } => {
                *all_steps = Some(steps);
            }
            StepEventKind::StepCompleted {
                step,
                outcome,
                step_elapsed,
                ..
            }
            | StepEventKind::ExecutionCompleted {
                last_step: step,
                last_outcome: outcome,
                step_elapsed,
                ..
            } => {
                if last_seen
                    .map(|last_seen| step.info.index <= last_seen)
                    .unwrap_or(false)
                {
                    continue;
                }
                *last_seen = Some(step.info.index);
                if let Some(pb) = progress_bar.take() {
                    pb.finish_and_clear();
                }

                print_completed_step(&step, &outcome, step_elapsed);
            }
            StepEventKind::ExecutionFailed {
                failed_step,
                step_elapsed,
                message,
                causes,
                ..
            } => {
                if last_seen
                    .map(|last_seen| failed_step.info.index <= last_seen)
                    .unwrap_or(false)
                {
                    continue;
                }
                *last_seen = Some(failed_step.info.index);
                if let Some(pb) = progress_bar.take() {
                    pb.finish_and_clear();
                }

                let mut message = message.clone();
                if !causes.is_empty() {
                    message.push_str("\nCaused by:");
                    for cause in causes {
                        message.push_str("\n    ");
                        message.push_str(&cause);
                    }
                }
                print_failed_step(&failed_step, step_elapsed, message);
                *execution_failed = true;
            }
        }
    }

    Ok(())
}

fn print_completed_step(
    info: &StepInfoWithMetadata,
    outcome: &StepOutcome,
    step_elapsed: Duration,
) {
    let icon = icon_for_outcome(outcome);
    print_step(
        icon,
        info,
        step_elapsed,
        outcome.completion_metadata(),
        outcome.message(),
    );
}

fn print_failed_step(
    info: &StepInfoWithMetadata,
    step_elapsed: Duration,
    message: String,
) {
    let icon = '✘';
    print_step(icon, info, step_elapsed, None, Some(&Cow::from(message)));
}

fn print_step(
    icon: impl Display,
    info: &StepInfoWithMetadata,
    step_elapsed: Duration,
    outcome_metadata: Option<&Vec<String>>,
    message: Option<&Cow<'static, str>>,
) {
    println!("{icon} {} ({:?})", info.info.description, step_elapsed);
    if let Some(metadata) = outcome_metadata {
        for element in metadata {
            println!("    {element}");
        }
    } else {
        println!("    missing metadata");
    }

    if let Some(message) = message {
        for line in message.split('\n') {
            println!("    {line}");
        }
    }
}

fn icon_for_outcome(outcome: &StepOutcome) -> Box<dyn Display> {
    match outcome {
        StepOutcome::Success { .. } => Box::new('✔'.green()),
        StepOutcome::Warning { .. } => Box::new('⚠'.red()),
        StepOutcome::Skipped { .. } => Box::new('*'.yellow()),
    }
}
