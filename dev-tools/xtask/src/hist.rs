// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Historical build and test timing analysis

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use futures::future::join_all;
use quick_xml::Reader;
use quick_xml::events::Event;
use std::time::Duration;
use tokio::process::Command;

#[derive(Args)]
pub struct HistArgs {
    /// Platform to analyze (helios or linux)
    #[arg(short = 'p', long, default_value = "linux")]
    pub platform: Platform,

    #[command(subcommand)]
    pub command: HistCommand,
}

#[derive(clap::Subcommand)]
pub enum HistCommand {
    /// Analyze historical test timing for recent commits
    Summary {
        /// Number of recent commits to analyze
        #[arg(short = 'n', long, default_value = "10")]
        count: usize,

        /// Include commits without timing data (show errors)
        #[arg(long)]
        include_missing: bool,
    },
    /// Show test suite breakdown for a specific commit
    Testsuites {
        /// Specific commit hash (defaults to most recent commit)
        #[arg(long)]
        commit: Option<String>,

        /// Sort test suites by duration in reverse order (fastest first)
        ///
        /// Default shows slowest first
        #[arg(short = 'r', long)]
        reverse: bool,
    },
    /// Show individual tests within a specific test suite
    Tests {
        /// Name of the test suite to analyze
        suite_name: String,

        /// Specific commit hash (defaults to most recent commit)
        #[arg(long)]
        commit: Option<String>,

        /// Sort tests by duration in reverse order (fastest first)
        ///
        /// Default shows slowest first
        #[arg(short = 'r', long)]
        reverse: bool,
    },
}

#[derive(ValueEnum, Clone, Debug)]
pub enum Platform {
    /// Analyze Helios test timing data
    Helios,
    /// Analyze Linux test timing data
    Linux,
}

impl Platform {
    fn series(&self) -> &'static str {
        match self {
            Platform::Helios => "junit-helios",
            Platform::Linux => "junit-linux",
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct TestCaseInfo {
    pub name: String,
    pub time: Duration,
}

#[derive(Debug, Default, Clone)]
pub struct TestSuiteInfo {
    pub name: String,
    pub tests: u32,
    pub time: Duration,
    pub test_cases: Vec<TestCaseInfo>,
}

#[derive(Debug, Default)]
pub struct JunitSummary {
    pub test_suites: Vec<TestSuiteInfo>,
    pub total_tests: u32,
    pub total_time: Duration,
}

pub fn run_cmd(args: HistArgs) -> Result<()> {
    sigpipe::reset();

    let rt = tokio::runtime::Runtime::new()
        .context("Failed to create tokio runtime")?;
    rt.block_on(run_cmd_async(args))
}

async fn run_cmd_async(args: HistArgs) -> Result<()> {
    match args.command {
        HistCommand::Summary { count, include_missing } => {
            run_summary_command(count, args.platform, include_missing).await
        }
        HistCommand::Testsuites { commit, reverse } => {
            let commit_hash = get_commit_hash(commit).await?;
            show_commit_detail(&commit_hash, &args.platform, reverse).await
        }
        HistCommand::Tests { suite_name, commit, reverse } => {
            let commit_hash = get_commit_hash(commit).await?;
            show_tests_in_suite(
                &commit_hash,
                &args.platform,
                &suite_name,
                reverse,
            )
            .await
        }
    }
}

async fn get_commit_hash(commit: Option<String>) -> Result<String> {
    match commit {
        Some(hash) => Ok(hash),
        None => {
            // Get the most recent commit
            let commits = get_recent_commits_with_offset(1, 0).await?;
            if commits.is_empty() {
                anyhow::bail!("No commits found");
            }
            Ok(commits[0]
                .split_whitespace()
                .next()
                .context(
                    "Failed to extract commit hash from most recent commit",
                )?
                .to_string())
        }
    }
}

async fn run_summary_command(
    count: usize,
    platform: Platform,
    include_missing: bool,
) -> Result<()> {
    println!(
        "Analyzing historical {} test timing for {} recent commits...",
        platform.series(),
        count
    );

    let mut analyzed_count = 0;
    let mut skipped_count = 0;
    let mut commit_offset = 0;
    let mut first_result = true;

    // Keep fetching commits until we have enough data points or run out of commits
    while analyzed_count < count {
        let batch_size = std::cmp::max(count - analyzed_count, 10);
        let commits =
            get_recent_commits_with_offset(batch_size, commit_offset).await?;

        if commits.is_empty() {
            break;
        }

        // Concurrently fetch timing data for all commits in this batch
        let fetch_tasks: Vec<_> = commits
            .iter()
            .map(|commit_line| {
                let commit_hash =
                    commit_line.split_whitespace().next().unwrap_or("");
                let platform = platform.clone();
                let commit_line = commit_line.clone();

                async move {
                    let result =
                        fetch_test_timing_data_async(&commit_hash, &platform)
                            .await;
                    (commit_line, commit_hash.to_string(), result)
                }
            })
            .collect();

        let fetch_results = join_all(fetch_tasks).await;
        let mut found_data_in_batch = false;

        // Process results in order
        for (commit_line, commit_hash, fetch_result) in fetch_results {
            match fetch_result {
                Ok(timing_data) => match parse_junit_xml(&timing_data) {
                    Ok(summary) => {
                        analyzed_count += 1;
                        found_data_in_batch = true;

                        // Print header on first result
                        if first_result {
                            println!(
                                "{:>12}   {:>6}   {:>11}   {}",
                                "TOTAL TIME", "TESTS", "TEST SUITES", "COMMIT"
                            );
                            first_result = false;
                        }

                        // Extract abbreviated commit hash (first 8 chars) and commit message
                        let short_hash =
                            &commit_hash[..std::cmp::min(8, commit_hash.len())];
                        let commit_msg = commit_line
                            .split_whitespace()
                            .skip(1) // Skip the full hash
                            .collect::<Vec<_>>()
                            .join(" ");

                        println!(
                            "{:>12}   {:>6}   {:>11}   {} {}",
                            format!("{:.2}s", summary.total_time.as_secs_f64()),
                            summary.total_tests,
                            summary.test_suites.len(),
                            short_hash,
                            commit_msg
                        );

                        if analyzed_count >= count {
                            break; // Got enough data points
                        }
                    }
                    Err(e) => {
                        if include_missing {
                            println!("  (skipped): {}", commit_line);
                            println!(
                                "    Could not parse test timing data: {}",
                                e
                            );
                        } else {
                            skipped_count += 1;
                        }
                    }
                },
                Err(e) => {
                    if include_missing {
                        println!("  (skipped): {}", commit_line);
                        println!("    No timing data available: {}", e);
                    } else {
                        skipped_count += 1;
                    }
                }
            }
        }

        commit_offset += commits.len();

        // If we didn't find any data in this batch, stop trying
        if !found_data_in_batch {
            break;
        }
    }

    if skipped_count > 0 {
        println!(
            "\nSkipped {} commits without timing data (use --include-missing to show them)",
            skipped_count
        );
    }

    if analyzed_count == 0 {
        println!("\nNo commits with timing data found!");
    }

    Ok(())
}

async fn show_commit_detail(
    commit_hash: &str,
    platform: &Platform,
    reverse: bool,
) -> Result<()> {
    println!(
        "Analyzing {} test suite details for commit {}...",
        platform.series(),
        commit_hash
    );

    match fetch_test_timing_data_async(commit_hash, platform).await {
        Ok(timing_data) => match parse_junit_xml(&timing_data) {
            Ok(summary) => {
                // Sort test suites by execution time
                let mut suites = summary.test_suites;
                if reverse {
                    // Ascending order (fastest first)
                    suites.sort_by(|a, b| a.time.cmp(&b.time));
                } else {
                    // Descending order (slowest first) - default
                    suites.sort_by(|a, b| b.time.cmp(&a.time));
                }

                // Calculate the maximum suite name length for proper alignment
                let max_name_len = suites
                    .iter()
                    .map(|suite| suite.name.len())
                    .max()
                    .unwrap_or(0)
                    .max(4); // At least as wide as "NAME" header

                println!(
                    "{:<width$}   {:>9}   {:>10}",
                    "NAME",
                    "DURATION",
                    "TEST COUNT",
                    width = max_name_len
                );
                for suite in suites.iter() {
                    println!(
                        "{:<width$}   {:8.2}s   {:>10}",
                        suite.name,
                        suite.time.as_secs_f64(),
                        suite.tests,
                        width = max_name_len
                    );
                }
            }
            Err(e) => {
                println!("Failed to parse test timing data: {}", e);
            }
        },
        Err(e) => {
            println!(
                "No timing data available for commit {}: {}",
                commit_hash, e
            );
        }
    }

    Ok(())
}

async fn show_tests_in_suite(
    commit_hash: &str,
    platform: &Platform,
    suite_name: &str,
    reverse: bool,
) -> Result<()> {
    println!(
        "Analyzing {} tests within '{}' for commit {}...",
        platform.series(),
        suite_name,
        commit_hash
    );

    match fetch_test_timing_data_async(commit_hash, platform).await {
        Ok(timing_data) => match parse_junit_xml(&timing_data) {
            Ok(summary) => {
                // Find the specific test suite
                let target_suite = summary
                    .test_suites
                    .iter()
                    .find(|suite| suite.name == suite_name);

                match target_suite {
                    Some(suite) => {
                        if suite.test_cases.is_empty() {
                            println!(
                                "No individual test timing data available for test suite '{}'",
                                suite_name
                            );
                            return Ok(());
                        }

                        // Sort test cases by execution time
                        let mut test_cases = suite.test_cases.clone();
                        if reverse {
                            // Ascending order (fastest first)
                            test_cases.sort_by(|a, b| a.time.cmp(&b.time));
                        } else {
                            // Descending order (slowest first) - default
                            test_cases.sort_by(|a, b| b.time.cmp(&a.time));
                        }

                        // Calculate the maximum test name length for proper alignment
                        let max_name_len = test_cases
                            .iter()
                            .map(|tc| tc.name.len())
                            .max()
                            .unwrap_or(0)
                            .max(9); // At least as wide as "TEST NAME" header

                        println!(
                            "{:<width$}   {:>9}",
                            "TEST NAME",
                            "DURATION",
                            width = max_name_len
                        );
                        for test_case in test_cases.iter() {
                            println!(
                                "{:<width$}   {:8.3}s",
                                test_case.name,
                                test_case.time.as_secs_f64(),
                                width = max_name_len
                            );
                        }

                        println!(
                            "\nSummary: {} tests, total duration: {:.3}s",
                            suite.tests,
                            suite.time.as_secs_f64()
                        );
                    }
                    None => {
                        println!(
                            "Test suite '{}' not found. Available test suites:",
                            suite_name
                        );
                        for suite in summary.test_suites.iter().take(10) {
                            println!("  {}", suite.name);
                        }
                        if summary.test_suites.len() > 10 {
                            println!(
                                "  ... and {} more",
                                summary.test_suites.len() - 10
                            );
                        }
                    }
                }
            }
            Err(e) => {
                println!("Failed to parse test timing data: {}", e);
            }
        },
        Err(e) => {
            println!(
                "No timing data available for commit {}: {}",
                commit_hash, e
            );
        }
    }

    Ok(())
}

async fn get_recent_commits_with_offset(
    count: usize,
    offset: usize,
) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args([
            "log",
            "--format=%H %s", // Full hash + subject instead of --oneline
            &format!("--skip={}", offset),
            &format!("-{}", count),
            "origin/main",
        ])
        .output()
        .await
        .context("Failed to run git log command")?;

    if !output.status.success() {
        anyhow::bail!(
            "git log failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let commits = String::from_utf8(output.stdout)
        .context("git log output was not valid UTF-8")?
        .lines()
        .map(|line| line.to_string())
        .collect();

    Ok(commits)
}

async fn fetch_test_timing_data_async(
    commit_hash: &str,
    platform: &Platform,
) -> Result<String> {
    let url = format!(
        "https://buildomat.eng.oxide.computer/wg/0/public/file/oxidecomputer/omicron/{}/{}/junit.xml",
        platform.series(),
        commit_hash
    );

    let output = Command::new("curl")
        .args([
            "--silent",
            "--fail",
            "--max-time",
            "30", // Total operation timeout
            "--connect-timeout",
            "10", // Connection timeout
            "--retry",
            "3", // Retry 3 times on transient failures
            "--retry-delay",
            "2", // Wait 2 seconds between retries
            "--retry-max-time",
            "60", // Don't retry longer than 60 seconds total
            "--retry-connrefused", // Retry on connection refused
            &url,
        ])
        .output()
        .await
        .context("Failed to run curl command")?;

    if !output.status.success() {
        anyhow::bail!(
            "Failed to fetch data from {}: {}",
            url,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    String::from_utf8(output.stdout).context("Response was not valid UTF-8")
}

fn parse_attribute<T: std::str::FromStr>(
    attr: &quick_xml::events::attributes::Attribute,
    context: &str,
) -> Result<T>
where
    T::Err: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static,
{
    let value = String::from_utf8_lossy(&attr.value);
    value.parse().map_err(|e| {
        anyhow::anyhow!("Failed to parse {}: '{}' - {}", context, value, e)
    })
}

fn parse_duration_seconds(
    attr: &quick_xml::events::attributes::Attribute,
    context: &str,
) -> Result<Duration> {
    let value = String::from_utf8_lossy(&attr.value);
    let seconds: f64 = value.parse().map_err(|e| {
        anyhow::anyhow!("Failed to parse {}: '{}' - {}", context, value, e)
    })?;
    Ok(Duration::from_secs_f64(seconds))
}

fn parse_junit_xml(junit_xml: &str) -> Result<JunitSummary> {
    let mut reader = Reader::from_str(junit_xml);
    reader.config_mut().trim_text(true);

    let mut summary = JunitSummary::default();
    let mut buf = Vec::new();
    let mut current_suite: Option<TestSuiteInfo> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                match e.name().as_ref() {
                    b"testsuites" => {
                        // Parse root element for totals
                        for attr in e.attributes() {
                            let attr =
                                attr.context("Failed to parse XML attribute")?;
                            match attr.key.as_ref() {
                                b"tests" => {
                                    summary.total_tests =
                                        parse_attribute(&attr, "tests count")?;
                                }
                                b"time" => {
                                    summary.total_time =
                                        parse_duration_seconds(
                                            &attr,
                                            "total time",
                                        )?;
                                }
                                _ => {}
                            }
                        }
                    }
                    b"testsuite" => {
                        // Start a new test suite
                        let mut suite_info = TestSuiteInfo::default();

                        // Parse testsuite attributes
                        for attr in e.attributes() {
                            let attr =
                                attr.context("Failed to parse XML attribute")?;
                            match attr.key.as_ref() {
                                b"name" => {
                                    suite_info.name =
                                        String::from_utf8_lossy(&attr.value)
                                            .to_string();
                                }
                                b"tests" => {
                                    suite_info.tests = parse_attribute(
                                        &attr,
                                        "suite tests count",
                                    )?;
                                }
                                _ => {}
                            }
                        }

                        current_suite = Some(suite_info);
                    }
                    b"testcase" => {
                        // Parse individual test case and add its time to current suite
                        if let Some(ref mut suite) = current_suite {
                            let mut test_case = TestCaseInfo::default();
                            for attr in e.attributes() {
                                let attr = attr
                                    .context("Failed to parse XML attribute")?;
                                match attr.key.as_ref() {
                                    b"name" => {
                                        test_case.name =
                                            String::from_utf8_lossy(
                                                &attr.value,
                                            )
                                            .to_string();
                                    }
                                    b"time" => {
                                        let case_time = parse_duration_seconds(
                                            &attr,
                                            "testcase time",
                                        )?;
                                        test_case.time = case_time;
                                        suite.time += case_time;
                                    }
                                    _ => {}
                                }
                            }
                            if !test_case.name.is_empty() {
                                suite.test_cases.push(test_case);
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                if e.name().as_ref() == b"testsuite" {
                    // Finished parsing a test suite, add it to summary
                    if let Some(suite) = current_suite.take() {
                        summary.test_suites.push(suite);
                    }
                }
            }
            Ok(Event::Empty(ref e)) => {
                match e.name().as_ref() {
                    b"testcase" => {
                        // Self-closing testcase element
                        if let Some(ref mut suite) = current_suite {
                            let mut test_case = TestCaseInfo::default();
                            for attr in e.attributes() {
                                let attr = attr
                                    .context("Failed to parse XML attribute")?;
                                match attr.key.as_ref() {
                                    b"name" => {
                                        test_case.name =
                                            String::from_utf8_lossy(
                                                &attr.value,
                                            )
                                            .to_string();
                                    }
                                    b"time" => {
                                        let case_time = parse_duration_seconds(
                                            &attr,
                                            "testcase time",
                                        )?;
                                        test_case.time = case_time;
                                        suite.time += case_time;
                                    }
                                    _ => {}
                                }
                            }
                            if !test_case.name.is_empty() {
                                suite.test_cases.push(test_case);
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(anyhow::anyhow!("XML parsing error: {}", e)),
            _ => {}
        }
        buf.clear();
    }

    if summary.test_suites.is_empty() {
        return Err(anyhow::anyhow!("No test suites found in XML"));
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn load_test_data() -> String {
        let test_data_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata/hist/linux/junit.xml");
        std::fs::read_to_string(test_data_path)
            .expect("Failed to read test data file")
    }

    #[test]
    fn test_parse_junit_xml_basic() {
        let xml_content = load_test_data();
        let summary =
            parse_junit_xml(&xml_content).expect("Failed to parse junit XML");

        // Verify totals match what we expect from the real data
        assert_eq!(summary.total_tests, 2009);
        assert_eq!(summary.total_time.as_secs_f64(), 2655.321);
        assert_eq!(summary.test_suites.len(), 103);
    }

    #[test]
    fn test_parse_junit_xml_suite_details() {
        let xml_content = load_test_data();
        let summary =
            parse_junit_xml(&xml_content).expect("Failed to parse junit XML");

        // Find specific test suites we know should exist with exact values
        let nexus_suite = summary
            .test_suites
            .iter()
            .find(|suite| suite.name == "omicron-nexus")
            .expect("Should find omicron-nexus test suite");
        assert_eq!(nexus_suite.tests, 171);
        assert_eq!(nexus_suite.time.as_secs_f64(), 5383.757);

        let setup_script = summary
            .test_suites
            .iter()
            .find(|suite| suite.name == "@setup-script:crdb-seed")
            .expect("Should find @setup-script:crdb-seed test suite");
        assert_eq!(setup_script.tests, 1);
        assert_eq!(setup_script.time.as_secs_f64(), 25.965);
    }

    #[test]
    fn test_parse_attribute_helper() {
        use quick_xml::events::attributes::Attribute;

        let attr = Attribute::from(("tests", "42"));
        let result: u32 =
            parse_attribute(&attr, "test count").expect("Should parse u32");
        assert_eq!(result, 42);

        let attr = Attribute::from(("time", "123.45"));
        let result = parse_duration_seconds(&attr, "test time")
            .expect("Should parse duration");
        assert_eq!(result.as_secs_f64(), 123.45);
    }
}
