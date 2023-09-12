// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query or update specific Nexus instances

use anyhow::Context;
use clap::Args;
use clap::Subcommand;
use nexus_client::types::ActivationReason;
use nexus_client::types::BackgroundTask;
use nexus_client::types::CurrentStatus;
use nexus_client::types::LastResult;

/// Arguments to the "omdb nexus" subcommand
#[derive(Debug, Args)]
pub struct NexusArgs {
    /// URL of the Nexus internal API
    nexus_internal_url: String,

    #[command(subcommand)]
    command: NexusCommands,
}

/// Subcommands for the "omdb nexus" subcommand
#[derive(Debug, Subcommand)]
enum NexusCommands {
    /// print information about background tasks
    BackgroundTask(BackgroundTaskArgs),
}

#[derive(Debug, Args)]
struct BackgroundTaskArgs {
    #[command(subcommand)]
    command: BackgroundTaskCommands,
}

#[derive(Debug, Subcommand)]
enum BackgroundTaskCommands {
    /// List background tasks
    List,
    /// Print the status of all background tasks
    Status,
}

impl NexusArgs {
    /// Run a `omdb nexus` subcommand.
    pub async fn run_cmd(
        &self,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        let client =
            nexus_client::Client::new(&self.nexus_internal_url, log.clone());

        match &self.command {
            NexusCommands::BackgroundTask(BackgroundTaskArgs {
                command: BackgroundTaskCommands::List,
            }) => cmd_nexus_background_task_list(&client).await,
            NexusCommands::BackgroundTask(BackgroundTaskArgs {
                command: BackgroundTaskCommands::Status,
            }) => cmd_nexus_background_task_status(&client).await,
        }
    }
}

/// Runs `omdb nexus background-task list`
async fn cmd_nexus_background_task_list(
    client: &nexus_client::Client,
) -> Result<(), anyhow::Error> {
    let response =
        client.bgtask_list().await.context("listing background tasks")?;
    let tasks = response.into_inner();
    for (_, bgtask) in &tasks {
        println!("task: {:?}", bgtask.name);
        println!(
            "{}",
            textwrap::fill(
                &bgtask.description,
                &textwrap::Options::new(80)
                    .initial_indent("    ")
                    .subsequent_indent("    ")
            )
        );

        println!("\n");
    }

    Ok(())
}

/// Runs `omdb nexus background-task status`
async fn cmd_nexus_background_task_status(
    client: &nexus_client::Client,
) -> Result<(), anyhow::Error> {
    let response =
        client.bgtask_list().await.context("listing background tasks")?;
    let tasks = response.into_inner();
    for (_, bgtask) in &tasks {
        print_task(bgtask);
    }

    Ok(())
}

fn print_task(bgtask: &BackgroundTask) {
    println!("task: {:?}", bgtask.name);
    print!("  currently executing: ");
    match &bgtask.current {
        CurrentStatus::Idle => println!("no"),
        CurrentStatus::Running(current) => {
            let elapsed = std::time::SystemTime::from(current.start_time)
                .elapsed()
                .map(|s| format!("{:.3}ms", s.as_millis()))
                .unwrap_or_else(|error| format!("(unknown: {:#})", error));
            print!(
                "iter {}, triggered by {}\n",
                current.iteration,
                reason_str(&current.reason)
            );
            print!(
                "    started at {}, running for {}\n",
                humantime::format_rfc3339_millis(current.start_time.into()),
                elapsed,
            );
        }
    };

    print!("  last completed activation: ");
    match &bgtask.last {
        LastResult::NeverCompleted => print!("never\n"),
        LastResult::Completed(last) => {
            print!(
                "iter {}, triggered by {}\n",
                last.iteration,
                reason_str(&last.reason)
            );
            print!(
                "    started at {} and ran for {:.3}ms\n",
                humantime::format_rfc3339_millis(last.start_time.into()),
                std::time::Duration::from(last.elapsed.clone()).as_millis(),
            );
        }
    };

    println!("");
}

/// Summarizes an `ActivationReason`
fn reason_str(reason: &ActivationReason) -> &'static str {
    match reason {
        ActivationReason::Signaled => "an explicit signal",
        ActivationReason::Dependency => "a dependent task completing",
        ActivationReason::Timeout => "a periodic timer firing",
    }
}
