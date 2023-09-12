// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI for querying Nexus internal state

use anyhow::Context;
use clap::Parser;
use nexus_client::types::ActivationReason;
use nexus_client::types::BackgroundTask;
use nexus_client::types::CurrentStatus;
use nexus_client::types::LastResult;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;

#[derive(Debug, Parser)]
#[clap(name = "nexus-status", about = "View Nexus internal state")]
struct Args {
    #[clap(
        help = "print task descriptions",
        long = "verbose",
        short = 'v',
        action
    )]
    verbose: bool,

    #[clap(help = "URL of Nexus internal API", action)]
    nexus_internal_url: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(cmd_error) = do_run(&args).await {
        fatal(CmdError::Failure(format!("{:#}", cmd_error)));
    }
}

async fn do_run(args: &Args) -> Result<(), anyhow::Error> {
    let url = &args.nexus_internal_url;
    let log = dropshot::ConfigLogging::StderrTerminal {
        level: dropshot::ConfigLoggingLevel::Warn,
    }
    .to_logger("nexus-status")
    .context("failed to create logger")?;

    let client = nexus_client::Client::new(url, log);
    let response =
        client.bgtask_list().await.context("listing background tasks")?;
    let tasks = response.into_inner();
    for (_, bgtask) in &tasks {
        print_task(bgtask, args.verbose);
    }

    Ok(())
}

fn print_task(bgtask: &BackgroundTask, verbose: bool) {
    println!("task: {:?}", bgtask.name);
    if verbose {
        println!(
            "{}",
            textwrap::fill(
                &bgtask.description,
                &textwrap::Options::new(80)
                    .initial_indent("  description: ")
                    .subsequent_indent("      ")
            )
        );
    }

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

fn reason_str(reason: &ActivationReason) -> &'static str {
    match reason {
        ActivationReason::Signaled => "an explicit signal",
        ActivationReason::Dependency => "a dependent task completing",
        ActivationReason::Timeout => "a periodic timer firing",
    }
}
