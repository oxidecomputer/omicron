// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query or update specific Nexus instances

use anyhow::Context;
use chrono::SecondsFormat;
use clap::Args;
use clap::Subcommand;
use nexus_client::types::ActivationReason;
use nexus_client::types::BackgroundTask;
use nexus_client::types::CurrentStatus;
use nexus_client::types::LastResult;
use tabled::Tabled;

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
    /// Show documentation about background tasks
    Doc,
    /// Print a summary of the status of all background tasks
    List,
    /// Print human-readable summary of the status of each background task
    Details,
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
                command: BackgroundTaskCommands::Doc,
            }) => cmd_nexus_background_task_doc(&client).await,
            NexusCommands::BackgroundTask(BackgroundTaskArgs {
                command: BackgroundTaskCommands::List,
            }) => cmd_nexus_background_task_list(&client).await,
            NexusCommands::BackgroundTask(BackgroundTaskArgs {
                command: BackgroundTaskCommands::Details,
            }) => cmd_nexus_background_task_details(&client).await,
        }
    }
}

/// Runs `omdb nexus background-task doc`
async fn cmd_nexus_background_task_doc(
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

/// Runs `omdb nexus background-task list`
async fn cmd_nexus_background_task_list(
    client: &nexus_client::Client,
) -> Result<(), anyhow::Error> {
    let response =
        client.bgtask_list().await.context("listing background tasks")?;
    let tasks = response.into_inner();
    let table_rows = tasks.values().map(BackgroundTaskStatusRow::from);
    let table = tabled::Table::new(table_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", table);
    Ok(())
}

/// Runs `omdb nexus background-task details`
async fn cmd_nexus_background_task_details(
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

// XXX-dap
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

/// Used for printing background task status as a table
#[derive(Tabled)]
#[tabled(rename_all = "UPPERCASE")]
struct BackgroundTaskStatusRow {
    task_name: String,
    #[tabled(rename = "PGEN#")]
    completed_generation: String,
    #[tabled(rename = "PSTART")]
    completed_start_time: String,
    #[tabled(rename = "Psecs")]
    completed_elapsed: String,
    #[tabled(rename = "P")]
    completed_reason: char,
    #[tabled(rename = "CSTART")]
    running_since: String,
    #[tabled(rename = "C")]
    running_reason: char,
}

impl<'a> From<&'a BackgroundTask> for BackgroundTaskStatusRow {
    fn from(t: &'a BackgroundTask) -> Self {
        let (
            completed_generation,
            completed_start_time,
            completed_elapsed,
            completed_reason,
        ) = match &t.last {
            LastResult::NeverCompleted => {
                (String::from("-"), String::from("-"), String::from("-"), '-')
            }
            LastResult::Completed(last) => (
                last.iteration.to_string(),
                last.start_time.to_rfc3339_opts(SecondsFormat::Secs, true),
                format!(
                    "{:5.1}",
                    std::time::Duration::from(last.elapsed.clone())
                        .as_secs_f64()
                ),
                reason_code(last.reason),
            ),
        };

        let (running_since, running_reason) = match &t.current {
            CurrentStatus::Idle => (String::from("-"), '-'),
            CurrentStatus::Running(current) => (
                current.start_time.to_rfc3339_opts(SecondsFormat::Secs, true),
                reason_code(current.reason),
            ),
        };

        BackgroundTaskStatusRow {
            task_name: t.name.clone(),
            completed_generation,
            completed_start_time,
            completed_elapsed,
            completed_reason,
            running_since,
            running_reason,
        }
    }
}

fn reason_code(reason: ActivationReason) -> char {
    match reason {
        ActivationReason::Signaled => 'S',
        ActivationReason::Dependency => 'D',
        ActivationReason::Timeout => 'T',
    }
}
