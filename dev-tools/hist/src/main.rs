// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Historical build and test timing analysis

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event as CrosstermEvent,
        KeyCode,
    },
    execute,
    terminal::{
        EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode,
        enable_raw_mode,
    },
};
use futures::future::join_all;
use quick_xml::Reader;
use quick_xml::events::Event;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{
        Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph, Wrap,
    },
};
use std::collections::HashSet;
use std::io;
use std::time::Duration;
use tokio::process::Command;

#[derive(Parser)]
#[command(name = "omicron-hist")]
#[command(about = "Historical build and test timing analysis")]
struct HistArgs {
    /// Platform to analyze (helios or linux)
    #[arg(short = 'p', long, default_value = "linux")]
    platform: Platform,

    #[command(subcommand)]
    command: HistCommand,
}

#[derive(clap::Subcommand)]
pub enum HistCommand {
    /// Compare test timing across recent commits or between specific commits
    Compare {
        /// Number of recent commits to analyze
        #[arg(short = 'n', long, default_value = "10")]
        count: usize,

        /// Compare performance of specific test suites across commits (can specify multiple)
        #[arg(long)]
        suite: Vec<String>,

        /// Compare performance of specific tests within the specified test suite (requires --suite)
        #[arg(long, requires = "suite")]
        test: Vec<String>,

        /// Display results as an interactive graph instead of a table
        #[arg(long)]
        graph: bool,
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

#[derive(ValueEnum, Copy, Clone, Debug)]
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

#[tokio::main]
async fn main() -> Result<()> {
    sigpipe::reset();

    let args = HistArgs::parse();
    run_cmd_async(args).await
}

#[derive(Debug, Default, Clone)]
pub struct TestCaseInfo {
    pub name: String,
    pub time: Duration,
}

#[derive(Debug, Default, Clone)]
pub struct TestSuiteInfo {
    pub name: String,
    pub time: Duration,
    pub test_cases: Vec<TestCaseInfo>,
}

#[derive(Debug, Default)]
pub struct JunitSummary {
    pub test_suites: Vec<TestSuiteInfo>,
    pub total_tests: u32,
    pub total_time: Duration,
}

#[derive(Debug, Default)]
pub struct JunitSummaryByCommit {
    pub summary: JunitSummary,
    pub commit_hash: String,
    pub commit_message: String,
}

impl JunitSummaryByCommit {
    fn extract_total_times(
        summaries: Vec<JunitSummaryByCommit>,
    ) -> Vec<DataPoint> {
        let mut data = Vec::new();
        for (commit_index, input) in summaries.into_iter().enumerate() {
            data.push(DataPoint {
                commit_index,
                value: input.summary.total_time.as_secs_f64(),
                commit_hash: input.commit_hash,
                commit_message: input.commit_message,
                series_name: "Overall".to_string(),
            });
        }
        data
    }

    fn extract_suite_times(
        summaries: Vec<JunitSummaryByCommit>,
        suites: HashSet<String>,
    ) -> Vec<DataPoint> {
        let mut data = Vec::new();
        for (commit_index, input) in summaries.into_iter().enumerate() {
            for suite in &input.summary.test_suites {
                if suites.contains(&suite.name) {
                    data.push(DataPoint {
                        commit_index,
                        value: suite.time.as_secs_f64(),
                        commit_hash: input.commit_hash.clone(),
                        commit_message: input.commit_message.clone(),
                        series_name: suite.name.to_string(),
                    });
                }
            }
        }
        data
    }

    fn extract_test_times(
        summaries: Vec<JunitSummaryByCommit>,
        suite: &str,
        tests: HashSet<String>,
    ) -> Vec<DataPoint> {
        let mut data = Vec::new();
        for (commit_index, input) in summaries.into_iter().enumerate() {
            for observed_suite in &input.summary.test_suites {
                if observed_suite.name == suite {
                    for test in &observed_suite.test_cases {
                        if tests.contains(&test.name) {
                            data.push(DataPoint {
                                commit_index,
                                value: test.time.as_secs_f64(),
                                commit_hash: input.commit_hash.clone(),
                                commit_message: input.commit_message.clone(),
                                series_name: test.name.to_string(),
                            });
                        }
                    }
                }
            }
        }
        data
    }
}

#[derive(Debug, Clone)]
pub struct DataPoint {
    pub commit_index: usize, // X-axis: commit index (0 = oldest)
    pub value: f64,          // Y-axis: the metric being measured
    pub commit_hash: String,
    pub commit_message: String,
    pub series_name: String, // Name of the series (suite name, test name, etc.)
}

#[derive(Debug)]
pub enum SeriesSpec {
    Overall,
    Suite(Vec<String>),
    Test { suite: String, tests: Vec<String> },
}

impl SeriesSpec {
    /// Collect data points for this series specification
    async fn collect_data_points(
        self,
        count: usize,
        platform: Platform,
    ) -> Result<Vec<DataPoint>> {
        let collector = DataCollector::new(platform, count);
        let data = collector.collect_junit().await?;

        match self {
            SeriesSpec::Overall => {
                Ok(JunitSummaryByCommit::extract_total_times(data))
            }
            SeriesSpec::Suite(suites) => {
                Ok(JunitSummaryByCommit::extract_suite_times(
                    data,
                    HashSet::from_iter(suites.into_iter()),
                ))
            }
            SeriesSpec::Test { suite, tests } => {
                Ok(JunitSummaryByCommit::extract_test_times(
                    data,
                    &suite,
                    HashSet::from_iter(tests.into_iter()),
                ))
            }
        }
    }

    /// Get the category type for generating titles
    fn category(&self) -> &'static str {
        match self {
            SeriesSpec::Overall => "Overall Test Performance",
            SeriesSpec::Suite(_) => "Test Suite Performance",
            SeriesSpec::Test { .. } => "Test Performance",
        }
    }
}

async fn run_cmd_async(args: HistArgs) -> Result<()> {
    match args.command {
        HistCommand::Compare { count, suite, test, graph } => {
            run_compare_command(count, args.platform, suite, test, graph).await
        }
        HistCommand::Testsuites { commit, reverse } => {
            let commit_hash = get_commit_hash(commit).await?;
            show_testsuites(&commit_hash, &args.platform, reverse).await
        }
        HistCommand::Tests { suite_name, commit, reverse } => {
            let commit_hash = get_commit_hash(commit).await?;
            show_tests(&commit_hash, &args.platform, &suite_name, reverse).await
        }
    }
}

async fn get_commit_hash(commit: Option<String>) -> Result<String> {
    match commit {
        Some(hash) => Ok(hash),
        None => {
            // Get the most recent commit
            let commits = get_commits_at_offset(0).await?;
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

async fn run_compare_command(
    count: usize,
    platform: Platform,
    suites: Vec<String>,
    tests: Vec<String>,
    graph: bool,
) -> Result<()> {
    // Route to specific comparison type based on arguments
    if !tests.is_empty() {
        // Individual tests comparison - requires exactly one suite
        if suites.len() != 1 {
            anyhow::bail!(
                "When comparing individual tests, you must specify exactly one test suite with --suite"
            );
        }
        let suite_name = &suites[0];
        if graph {
            return compare_tests_graph(count, platform, suite_name, tests)
                .await;
        } else {
            return compare_tests_table(count, platform, suite_name, tests)
                .await;
        }
    }

    if !suites.is_empty() {
        if graph {
            return compare_testsuites_graph(count, platform, suites).await;
        } else {
            return compare_testsuites_table(count, platform, suites).await;
        }
    }

    // Default: show overall summary comparison
    if graph {
        compare_overall_graph(count, platform).await
    } else {
        compare_overall_table(count, platform).await
    }
}

async fn compare_overall_table(count: usize, platform: Platform) -> Result<()> {
    println!(
        "Analyzing historical {} test timing for {} recent commits...",
        platform.series(),
        count
    );
    let collector = DataCollector::new(platform, count);
    let summaries = collector.collect_junit().await?;

    if summaries.is_empty() {
        anyhow::bail!("\nNo commits with timing data found!");
    }
    println!(
        "{:>12}   {:>6}   {:>11}   {}",
        "TOTAL TIME", "TESTS", "TEST SUITES", "COMMIT"
    );
    for summary in summaries {
        println!(
            "{:>12}   {:>6}   {:>11}   {} {}",
            format!("{:.2}s", summary.summary.total_time.as_secs_f64()),
            summary.summary.total_tests,
            summary.summary.test_suites.len(),
            &summary.commit_hash[..8],
            summary.commit_message,
        );
    }

    Ok(())
}

async fn compare_testsuites_table(
    count: usize,
    platform: Platform,
    suites: Vec<String>,
) -> Result<()> {
    if suites.len() != 1 {
        anyhow::bail!(
            "Multiple suite comparison table not yet implemented - use --graph for visualization"
        );
    }
    let suite_name = &suites[0];

    println!(
        "Comparing '{}' test suite performance across {} recent commits ({})...",
        suite_name,
        count,
        platform.series()
    );

    let collector = DataCollector::new(platform, count);
    let summaries = collector.collect_junit().await?;

    if summaries.is_empty() {
        anyhow::bail!("\nNo commits with timing data found!");
    }
    println!("{:>12}   {:>6}   {}", "DURATION", "TESTS", "COMMIT");
    for summary in summaries {
        // Find the specific test suite
        if let Some(suite) =
            summary.summary.test_suites.iter().find(|s| s.name == *suite_name)
        {
            println!(
                "{:>12}   {:>6}   {} {}",
                format!("{:.2}s", suite.time.as_secs_f64()),
                suite.test_cases.len(),
                &summary.commit_hash[..8],
                summary.commit_message,
            );
        }
    }

    Ok(())
}

async fn compare_tests_table(
    count: usize,
    platform: Platform,
    suite_name: &str,
    test_names: Vec<String>,
) -> Result<()> {
    if test_names.len() != 1 {
        println!(
            "Multiple test comparison table not yet implemented - use --graph for visualization"
        );
    }
    let test_name = &test_names[0];

    println!(
        "Comparing '{}' test performance in suite '{}' across {} recent commits ({})...",
        test_name,
        suite_name,
        count,
        platform.series()
    );

    let collector = DataCollector::new(platform, count);
    let summaries = collector.collect_junit().await?;

    if summaries.is_empty() {
        anyhow::bail!("\nNo commits with timing data found!");
    }
    println!("{:>12}   {}", "DURATION", "COMMIT");
    for summary in summaries {
        // Find the specific test suite
        if let Some(suite) =
            summary.summary.test_suites.iter().find(|s| s.name == suite_name)
        {
            // Find the specific test within the suite
            if let Some(test_case) =
                suite.test_cases.iter().find(|tc| tc.name == *test_name)
            {
                println!(
                    "{:>12}   {} {}",
                    format!("{:.3}s", test_case.time.as_secs_f64()),
                    &summary.commit_hash[..8],
                    summary.commit_message,
                );
            }
        }
    }

    Ok(())
}

async fn show_testsuites(
    commit_hash: &str,
    platform: &Platform,
    reverse: bool,
) -> Result<()> {
    println!(
        "Analyzing {} test suite details for commit {}...",
        platform.series(),
        commit_hash
    );

    match fetch_test_timing_data(commit_hash, platform).await {
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
                        suite.test_cases.len(),
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

async fn show_tests(
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

    match fetch_test_timing_data(commit_hash, platform).await {
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
                            suite.test_cases.len(),
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

async fn get_commits_at_offset(offset: usize) -> Result<Vec<String>> {
    let count = 32;
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

async fn fetch_test_timing_data(
    commit_hash: &str,
    platform: &Platform,
) -> Result<String> {
    let url = format!(
        "https://buildomat.eng.oxide.computer/wg/0/public/file/oxidecomputer/omicron/{}/{}/junit.xml",
        platform.series(),
        commit_hash
    );

    // Create a persistent HTTP client with connection pooling for better performance
    static HTTP_CLIENT: std::sync::OnceLock<reqwest::Client> =
        std::sync::OnceLock::new();
    let client = HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(8)
            .build()
            .expect("Failed to create HTTP client")
    });

    let response =
        client.get(&url).send().await.context("Failed to send HTTP request")?;

    if !response.status().is_success() {
        anyhow::bail!(
            "Failed to fetch data from {}: HTTP {}",
            url,
            response.status()
        );
    }

    response.text().await.context("Failed to read response body as text")
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

async fn collect_data_and_show_graph(
    count: usize,
    platform: Platform,
    series: SeriesSpec,
) -> Result<()> {
    // Generate title and labels based on series
    let title = series.category();
    let mut data_points = series.collect_data_points(count, platform).await?;
    if data_points.is_empty() {
        anyhow::bail!("No data points available");
    }

    // Before this point: "commit_index" is "offset from the most recent commit".
    // E.g.: "0" -> "The most recent commit to main".
    //
    // However, it's nice to show time progressing from old -> new on an X-axis,
    // so we flip the ordering here.
    //
    // This transformation makes "X = 0" refer to the oldest commit in the observed data.
    let max_commit_index =
        data_points.iter().map(|p| p.commit_index).fold(0, usize::max);
    for data_point in &mut data_points {
        data_point.commit_index = max_commit_index - data_point.commit_index;
    }

    show_graph(data_points, &title)
}

async fn compare_overall_graph(count: usize, platform: Platform) -> Result<()> {
    collect_data_and_show_graph(count, platform, SeriesSpec::Overall).await
}

struct DataCollector {
    platform: Platform,
    count: usize,
}

impl DataCollector {
    fn new(platform: Platform, count: usize) -> Self {
        Self { platform, count }
    }

    async fn collect_junit(&self) -> Result<Vec<JunitSummaryByCommit>> {
        let mut summaries = Vec::new();
        let mut analyzed_count = 0;
        let mut commit_offset = 0;

        while analyzed_count < self.count {
            let commits = get_commits_at_offset(commit_offset).await?;
            if commits.is_empty() {
                break;
            }

            // Concurrently fetch timing data for all commits in this batch
            let fetch_tasks: Vec<_> = commits
                .iter()
                .map(|commit_line| {
                    let commit_hash =
                        commit_line.split_whitespace().next().unwrap_or("");
                    let platform = self.platform;
                    let commit_line = commit_line.clone();
                    let commit_hash = commit_hash.to_string();

                    tokio::spawn(async move {
                        let fetch_result =
                            fetch_test_timing_data(&commit_hash, &platform)
                                .await;
                        let parse_result = match fetch_result {
                            Ok(timing_data) => parse_junit_xml(&timing_data),
                            Err(e) => {
                                return (commit_line, commit_hash, Err(e));
                            }
                        };
                        (commit_line, commit_hash, parse_result)
                    })
                })
                .collect();

            let fetch_results = join_all(fetch_tasks).await;
            let fetch_results: Result<Vec<_>, _> =
                fetch_results.into_iter().collect();
            let fetch_results = fetch_results.context("Task join error")?;
            let mut found_data_in_batch = false;

            for (commit_line, commit_hash, parse_result) in fetch_results {
                match parse_result {
                    Ok(summary) => {
                        analyzed_count += 1;
                        found_data_in_batch = true;

                        let commit_message = commit_line
                            .split_whitespace()
                            .skip(1)
                            .collect::<Vec<_>>()
                            .join(" ");
                        let commit_hash = commit_hash[..8].to_string();
                        summaries.push(JunitSummaryByCommit {
                            summary,
                            commit_hash,
                            commit_message,
                        });

                        if analyzed_count >= self.count {
                            break;
                        }
                    }
                    Err(_) => {}
                }
            }

            commit_offset += commits.len();

            if !found_data_in_batch {
                break;
            }
        }

        Ok(summaries)
    }
}

async fn compare_testsuites_graph(
    count: usize,
    platform: Platform,
    suite_names: Vec<String>,
) -> Result<()> {
    let series = SeriesSpec::Suite(suite_names);
    collect_data_and_show_graph(count, platform, series).await
}

async fn compare_tests_graph(
    count: usize,
    platform: Platform,
    suite_name: &str,
    test_names: Vec<String>,
) -> Result<()> {
    let series =
        SeriesSpec::Test { suite: suite_name.to_string(), tests: test_names };
    collect_data_and_show_graph(count, platform, series).await
}

fn show_graph(data_points: Vec<DataPoint>, title: &str) -> Result<()> {
    if data_points.is_empty() {
        println!("No data points to display");
        return Ok(());
    }

    // Group data points by series
    use std::collections::HashMap;
    let mut series_data: HashMap<String, Vec<DataPoint>> = HashMap::new();
    for point in data_points {
        series_data
            .entry(point.series_name.clone())
            .or_insert_with(Vec::new)
            .push(point);
    }

    let colors = [
        Color::Cyan,
        Color::Yellow,
        Color::Green,
        Color::Red,
        Color::Blue,
        Color::Magenta,
    ];

    if series_data.len() > colors.len() {
        anyhow::bail!(
            "Too many series (can only compare at most {})",
            colors.len()
        );
    }

    // Setup terminal
    enable_raw_mode().context("Failed to enable raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)
        .context("Failed to setup terminal")?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal =
        Terminal::new(backend).context("Failed to create terminal")?;

    // Prepare data for chart
    let all_points: Vec<&DataPoint> = series_data.values().flatten().collect();
    let min_x = all_points
        .iter()
        .map(|p| p.commit_index as f64)
        .fold(f64::INFINITY, f64::min);
    let max_x = all_points.iter().map(|p| p.commit_index).fold(0, usize::max);
    let max_y =
        all_points.iter().map(|p| p.value).fold(f64::NEG_INFINITY, f64::max);

    // Use absolute Y-axis starting from 0
    let chart_min_y = 0.0;
    let chart_max_y = max_y * 1.05; // Add just 5% padding at the top

    // Convert data to chart format and assign colors
    let mut chart_data_sets: Vec<Vec<(f64, f64)>> = Vec::new();
    let mut legend_info = Vec::new();

    for (i, (series_name, points)) in series_data.iter().enumerate() {
        let color = colors[i % colors.len()];
        let chart_data: Vec<(f64, f64)> =
            points.iter().map(|p| (p.commit_index as f64, p.value)).collect();
        chart_data_sets.push(chart_data);
        legend_info.push((series_name.clone(), color));
    }

    let mut highlighted_x = max_x;

    loop {
        // Redraw with current scroll position
        let result = terminal.draw(|f| {
            let size = f.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([
                    Constraint::Length(3),                        // Title
                    Constraint::Length(legend_info.len() as u16), // Legend
                    Constraint::Min(0),                           // Chart
                    Constraint::Length(3),                        // Commit info
                    Constraint::Length(1),                        // Help text
                ])
                .split(size);

            // Title block
            let title_paragraph =
                Paragraph::new(vec![Line::from(Span::styled(
                    title,
                    Style::default().add_modifier(Modifier::BOLD),
                ))]);
            f.render_widget(title_paragraph, chunks[0]);

            // Collect all normal and highlighted points with their colors first
            let mut all_normal_points: Vec<(Vec<(f64, f64)>, Color)> =
                Vec::new();
            let mut all_highlighted_points: Vec<(Vec<(f64, f64)>, Color)> =
                Vec::new();

            for (chart_data, (_series_name, color)) in
                chart_data_sets.iter().zip(legend_info.iter())
            {
                let normal_points: Vec<(f64, f64)> = chart_data
                    .iter()
                    .filter_map(|&(x, y)| {
                        if x == highlighted_x as f64 {
                            None
                        } else {
                            Some((x, y))
                        }
                    })
                    .collect();
                let highlighted_points: Vec<(f64, f64)> = chart_data
                    .iter()
                    .filter_map(|&(x, y)| {
                        if x == highlighted_x as f64 {
                            Some((x, y))
                        } else {
                            None
                        }
                    })
                    .collect();

                if !normal_points.is_empty() {
                    all_normal_points.push((normal_points, *color));
                }
                if !highlighted_points.is_empty() {
                    all_highlighted_points.push((highlighted_points, *color));
                }
            }

            // Now create datasets from the collected points
            let mut datasets = Vec::new();
            for (normal_points, color) in &all_normal_points {
                let dataset = Dataset::default()
                    .marker(symbols::Marker::Dot)
                    .style(Style::default().fg(*color))
                    .graph_type(GraphType::Scatter)
                    .data(normal_points);
                datasets.push(dataset);
            }
            for (highlighted_points, color) in &all_highlighted_points {
                let dataset = Dataset::default()
                    .marker(symbols::Marker::Block)
                    .style(
                        Style::default()
                            .fg(*color)
                            .add_modifier(Modifier::BOLD),
                    )
                    .graph_type(GraphType::Scatter)
                    .data(highlighted_points);
                datasets.push(dataset);
            }

            // Chart with legend
            let chart = Chart::new(datasets)
                .block(Block::default().borders(Borders::ALL))
                .x_axis(
                    Axis::default()
                        .title("Commits (oldest → newest)")
                        .style(Style::default().fg(Color::Gray))
                        .bounds([min_x, max_x as f64])
                        .labels(vec![
                            Span::styled(
                                "oldest",
                                Style::default().add_modifier(Modifier::BOLD),
                            ),
                            Span::styled(
                                "newest",
                                Style::default().add_modifier(Modifier::BOLD),
                            ),
                        ]),
                )
                .y_axis(
                    Axis::default()
                        .title("Duration (seconds)")
                        .style(Style::default().fg(Color::Gray))
                        .bounds([chart_min_y, chart_max_y])
                        .labels(vec![
                            Span::raw(format!("{:.2}", chart_min_y)),
                            Span::raw(format!(
                                "{:.2}",
                                (chart_min_y + chart_max_y) / 2.0
                            )),
                            Span::raw(format!("{:.2}", chart_max_y)),
                        ]),
                );
            f.render_widget(chart, chunks[2]);

            // Legend section
            let mut legend_lines = Vec::new();
            for (series_name, color) in &legend_info {
                legend_lines.push(Line::from(vec![
                    Span::styled("  ●", Style::default().fg(*color)),
                    Span::raw(format!(" {}", series_name)),
                ]));
            }

            let legend_paragraph = Paragraph::new(legend_lines);
            f.render_widget(legend_paragraph, chunks[1]);

            let commits_text: Line = all_points
                .iter()
                .find(|p| p.commit_index == highlighted_x)
                .map(|point| {
                    Line::from(vec![
                        Span::styled(
                            point.commit_hash.clone(),
                            Style::default().fg(Color::Green),
                        ),
                        Span::raw(" "),
                        Span::raw(point.commit_message.clone()),
                    ])
                })
                .unwrap_or_else(|| {
                    Line::from(Span::raw("No data found at this commit"))
                });

            let commits_title =
                format!("Commit {} of {}", highlighted_x + 1, max_x + 1);
            let commits_paragraph = Paragraph::new(vec![commits_text])
                .block(
                    Block::default().title(commits_title).borders(Borders::ALL),
                )
                .wrap(Wrap { trim: true });
            f.render_widget(commits_paragraph, chunks[3]);

            // Help text with navigation instructions
            let help_text = "Press 'q' to quit, ←/→ or h/l to scroll commits";
            let help = Paragraph::new(help_text)
                .style(Style::default().fg(Color::Gray));
            f.render_widget(help, chunks[4]);
        });

        if let Err(e) = result {
            // Cleanup and return error
            let _ = disable_raw_mode();
            let _ = execute!(
                terminal.backend_mut(),
                LeaveAlternateScreen,
                DisableMouseCapture
            );
            return Err(anyhow::anyhow!("Failed to draw terminal: {}", e));
        }

        match event::read().context("Failed to read terminal event")? {
            CrosstermEvent::Key(key_event) => match key_event.code {
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Left | KeyCode::Char('h') => {
                    if highlighted_x > 0 {
                        highlighted_x = highlighted_x.saturating_sub(1);
                    }
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    if highlighted_x < max_x {
                        highlighted_x += 1;
                    }
                }
                KeyCode::Home => {
                    highlighted_x = 0;
                }
                KeyCode::End => {
                    highlighted_x = max_x;
                }
                _ => {}
            },
            _ => {}
        }
    }

    // Cleanup
    disable_raw_mode().context("Failed to disable raw mode")?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)
        .context("Failed to cleanup terminal")?;

    Ok(())
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
        assert_eq!(nexus_suite.test_cases.len(), 171);
        assert_eq!(nexus_suite.time.as_secs_f64(), 5383.757);

        let setup_script = summary
            .test_suites
            .iter()
            .find(|suite| suite.name == "@setup-script:crdb-seed")
            .expect("Should find @setup-script:crdb-seed test suite");
        assert_eq!(setup_script.test_cases.len(), 1);
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
