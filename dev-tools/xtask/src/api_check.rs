// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask api-check
//!
//! Unified API change validation workflow that chains multiple steps
//! in a single process to minimize cargo overhead.

use crate::common::run_subcmd;
use anyhow::{Result, bail};
use clap::Parser;
use dev_tools_common::{CargoLocation, cargo_command};
use std::time::{Duration, Instant};

#[derive(Parser)]
pub struct ApiCheckArgs {
    /// Skip specific steps (comma-separated: check,openapi,recheck,test,clippy)
    #[clap(long, value_delimiter = ',')]
    skip: Vec<String>,

    /// Only run specific steps (comma-separated: check,openapi,recheck,test,clippy)
    #[clap(long, value_delimiter = ',', conflicts_with = "skip")]
    only: Vec<String>,

    /// Test filter pattern for nextest (default: "ls-apis")
    #[clap(long, default_value = "ls-apis")]
    test_filter: String,

    /// Continue through failures instead of stopping at first failure
    #[clap(long)]
    keep_going: bool,

    /// Quick mode: only run check + openapi + recheck (skip tests and clippy)
    #[clap(long, conflicts_with_all = ["skip", "only"])]
    quick: bool,

    /// Print what would be executed without running
    #[clap(long)]
    dry_run: bool,

    /// Error format passed to cargo commands
    #[clap(long, value_name = "FMT")]
    message_format: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    Check,
    Openapi,
    Recheck,
    Test,
    Clippy,
}

impl Step {
    fn name(&self) -> &'static str {
        match self {
            Step::Check => "check",
            Step::Openapi => "openapi",
            Step::Recheck => "recheck",
            Step::Test => "test",
            Step::Clippy => "clippy",
        }
    }
}

struct StepResult {
    step: Step,
    duration: Duration,
    success: bool,
}

pub fn run_cmd(args: ApiCheckArgs) -> Result<()> {
    let steps = resolve_steps(&args);
    let mut results = Vec::new();

    if args.dry_run {
        print_dry_run(&steps, &args);
        return Ok(());
    }

    let total_start = Instant::now();

    for (idx, &step) in steps.iter().enumerate() {
        print_step_header(step, idx + 1, steps.len());

        let start = Instant::now();
        let result = match step {
            Step::Check => run_check(&args),
            Step::Openapi => run_openapi(&args),
            Step::Recheck => {
                // Only run recheck if openapi changed files
                // For now, always run it (optimization for later)
                run_check(&args)
            }
            Step::Test => run_test(&args),
            Step::Clippy => run_clippy_step(&args),
        };
        let duration = start.elapsed();

        let success = result.is_ok();
        results.push(StepResult { step, duration, success });

        if success {
            print_step_success(step, duration);
        } else {
            print_step_failure(step, duration);
            if !args.keep_going {
                print_failure_summary(&results, total_start.elapsed());
                return result;
            }
        }
    }

    print_summary(&results, total_start.elapsed());

    // Return error if any step failed
    if results.iter().any(|r| !r.success) {
        bail!("one or more steps failed");
    }

    Ok(())
}

fn resolve_steps(args: &ApiCheckArgs) -> Vec<Step> {
    let all_steps = vec![
        Step::Check,
        Step::Openapi,
        Step::Recheck,
        Step::Test,
        Step::Clippy,
    ];

    if args.quick {
        return vec![Step::Check, Step::Openapi, Step::Recheck];
    }

    if !args.only.is_empty() {
        return all_steps
            .into_iter()
            .filter(|s| args.only.contains(&s.name().to_string()))
            .collect();
    }

    if !args.skip.is_empty() {
        return all_steps
            .into_iter()
            .filter(|s| !args.skip.contains(&s.name().to_string()))
            .collect();
    }

    all_steps
}

fn run_check(args: &ApiCheckArgs) -> Result<()> {
    let mut command = cargo_command(CargoLocation::FromEnv);
    command.arg("check").arg("--workspace").arg("--all-targets");

    if let Some(fmt) = &args.message_format {
        command.args(["--message-format", fmt]);
    }

    run_subcmd(command)
}

fn run_openapi(args: &ApiCheckArgs) -> Result<()> {
    let mut command = cargo_command(CargoLocation::FromEnv);
    command
        .arg("run")
        .arg("--bin")
        .arg("omicron-dropshot-apis")
        .arg("--")
        .arg("generate");

    if let Some(fmt) = &args.message_format {
        command.args(["--message-format", fmt]);
    }

    run_subcmd(command)
}

fn run_test(args: &ApiCheckArgs) -> Result<()> {
    let mut command = cargo_command(CargoLocation::FromEnv);
    command.arg("nextest").arg("run").arg(&args.test_filter);

    if let Some(fmt) = &args.message_format {
        command.args(["--message-format", fmt]);
    }

    run_subcmd(command)
}

fn run_clippy_step(args: &ApiCheckArgs) -> Result<()> {
    // Run clippy with the same configuration as `cargo xtask clippy`
    let mut command = cargo_command(CargoLocation::FromEnv);
    command
        .arg("clippy")
        .arg("--all-targets")
        .arg("--workspace")
        .arg("--")
        .arg("--deny")
        .arg("warnings");

    if let Some(fmt) = &args.message_format {
        command.args(["--message-format", fmt]);
    }

    run_subcmd(command)
}

fn print_step_header(step: Step, current: usize, total: usize) {
    eprintln!();
    eprintln!("━━━ api-check: step {}/{} — {} ━━━", current, total, step.name());
}

fn print_step_success(step: Step, duration: Duration) {
    eprintln!("✓ {} passed ({:.1}s)", step.name(), duration.as_secs_f64());
}

fn print_step_failure(step: Step, duration: Duration) {
    eprintln!("✗ {} failed ({:.1}s)", step.name(), duration.as_secs_f64());
}

fn print_summary(results: &[StepResult], total_duration: Duration) {
    eprintln!();
    eprintln!("━━━ api-check: summary ━━━");
    for result in results {
        let symbol = if result.success { "✓" } else { "✗" };
        eprintln!(
            "  {} {} ({:.1}s)",
            symbol,
            result.step.name(),
            result.duration.as_secs_f64()
        );
    }
    eprintln!();
    let all_passed = results.iter().all(|r| r.success);
    if all_passed {
        eprintln!(
            "━━━ api-check: all steps passed ({:.1}s) ━━━",
            total_duration.as_secs_f64()
        );
    } else {
        eprintln!(
            "━━━ api-check: {} step(s) failed ({:.1}s) ━━━",
            results.iter().filter(|r| !r.success).count(),
            total_duration.as_secs_f64()
        );
    }
}

fn print_failure_summary(results: &[StepResult], total_duration: Duration) {
    eprintln!();
    eprintln!("━━━ api-check: failed at step '{}' ━━━", results.last().unwrap().step.name());
    eprintln!();
    eprintln!("Steps completed before failure:");
    for result in results.iter().take(results.len() - 1) {
        let symbol = if result.success { "✓" } else { "✗" };
        eprintln!(
            "  {} {} ({:.1}s)",
            symbol,
            result.step.name(),
            result.duration.as_secs_f64()
        );
    }
    eprintln!();
    eprintln!("To re-run just the failed step, use:");
    match results.last().unwrap().step {
        Step::Check => eprintln!("  cargo check --workspace --all-targets"),
        Step::Openapi => eprintln!("  cargo xtask openapi generate"),
        Step::Recheck => eprintln!("  cargo check --workspace --all-targets"),
        Step::Test => eprintln!("  cargo nextest run <filter>"),
        Step::Clippy => eprintln!("  cargo xtask clippy"),
    }
    eprintln!();
    eprintln!("Total time: {:.1}s", total_duration.as_secs_f64());
}

fn print_dry_run(steps: &[Step], args: &ApiCheckArgs) {
    eprintln!("━━━ api-check: dry run ━━━");
    eprintln!();
    eprintln!("Would execute {} steps:", steps.len());
    for (idx, step) in steps.iter().enumerate() {
        eprintln!("  {}. {}", idx + 1, step.name());
    }
    eprintln!();
    eprintln!("Configuration:");
    eprintln!("  Test filter: {}", args.test_filter);
    eprintln!("  Keep going: {}", args.keep_going);
    eprintln!("  Quick mode: {}", args.quick);
}
