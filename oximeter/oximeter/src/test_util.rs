// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for testing the oximeter crate.

// Copyright 2023 Oxide Computer Company

use crate::histogram;
use crate::histogram::Histogram;
use crate::types::Cumulative;
use crate::types::Sample;
use crate::Metric;
use crate::Target;
use anyhow::Context;
use console::Style;
use similar::Algorithm;
use similar::ChangeTag;
use similar::TextDiff;
use std::path::Path;
use uuid::Uuid;

#[derive(oximeter::Target)]
pub struct TestTarget {
    pub name1: String,
    pub name2: String,
    pub num: i64,
}

impl Default for TestTarget {
    fn default() -> Self {
        TestTarget {
            name1: "first_name".into(),
            name2: "second_name".into(),
            num: 0,
        }
    }
}

#[derive(oximeter::Metric)]
pub struct TestMetric {
    pub id: Uuid,
    pub good: bool,
    pub datum: i64,
}

#[derive(oximeter::Metric)]
pub struct TestCumulativeMetric {
    pub id: Uuid,
    pub good: bool,
    pub datum: Cumulative<i64>,
}

#[derive(oximeter::Metric)]
pub struct TestHistogram {
    pub id: Uuid,
    pub good: bool,
    pub datum: Histogram<f64>,
}

pub fn make_sample() -> Sample {
    let target = TestTarget::default();
    let metric = TestMetric { id: Uuid::new_v4(), good: true, datum: 1 };
    Sample::new(&target, &metric).unwrap()
}

pub fn make_hist_sample() -> Sample {
    let target = TestTarget::default();
    let mut hist = histogram::Histogram::new(&[0.0, 5.0, 10.0]).unwrap();
    hist.sample(1.0).unwrap();
    hist.sample(2.0).unwrap();
    hist.sample(6.0).unwrap();
    let metric = TestHistogram { id: Uuid::new_v4(), good: true, datum: hist };
    Sample::new(&target, &metric).unwrap()
}

/// A target identifying a single virtual machine instance
#[derive(Debug, Clone, Copy, oximeter::Target)]
pub struct VirtualMachine {
    pub project_id: Uuid,
    pub instance_id: Uuid,
}

/// A metric recording the total time a vCPU is busy, by its ID
#[derive(Debug, Clone, Copy, oximeter::Metric)]
pub struct CpuBusy {
    cpu_id: i64,
    datum: Cumulative<f64>,
}

pub fn generate_test_samples(
    n_projects: usize,
    n_instances: usize,
    n_cpus: usize,
    n_samples: usize,
) -> Vec<Sample> {
    let n_timeseries = n_projects * n_instances * n_cpus;
    let mut samples = Vec::with_capacity(n_samples * n_timeseries);
    for _ in 0..n_projects {
        let project_id = Uuid::new_v4();
        for _ in 0..n_instances {
            let vm = VirtualMachine { project_id, instance_id: Uuid::new_v4() };
            for cpu in 0..n_cpus {
                for sample in 0..n_samples {
                    let cpu_busy = CpuBusy {
                        cpu_id: cpu as _,
                        datum: Cumulative::new(sample as f64),
                    };
                    let sample = Sample::new(&vm, &cpu_busy).unwrap();
                    samples.push(sample);
                }
            }
        }
    }
    samples
}

/// A test function for verifying and possibly updating a target schema.
///
/// As schema are updated, the version numbers should change, otherwise the data
/// can fail to be correctly inserted into the timeseries database. This
/// function can be used to ensure that.
///
/// One can write the expected schema to a file that is checked into the
/// repository, and then include a test which creates a target and verifies its
/// schema against the file. If those do not match, this function will fail if
/// the version number is not greater than the version stored in the file.
///
/// Similar to the [`expectorate`][1] crate, users can automatically overwrite
/// the file by setting the environment variable `EXPECTORATE=overwrite` when
/// running `cargo test`.
///
/// [1]: <https://docs.rs/expectorate/latest/expectorate/>
pub fn check_target_schema(
    path: impl AsRef<Path>,
    target: impl Target,
) -> anyhow::Result<()> {
    let actual = serde_json::json!({
        "name": target.name(),
        "version": target.version(),
        "field_names": target
            .field_names()
            .into_iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        "field_types": target
            .field_types()
            .into_iter()
            .map(|f| format!("{f:?}"))
            .collect::<Vec<_>>(),
    });
    check_contents(path, actual)
}

/// A test function for verifying and possibly updating a metric schema.
///
/// As schema are updated, the version numbers should change, otherwise the data
/// can fail to be correctly inserted into the timeseries database. This
/// function can be used to ensure that.
///
/// One can write the expected schema to a file that is checked into the
/// repository, and then include a test which creates a metric and verifies its
/// schema against the file. If those do not match, this function will fail if
/// the version number is not greater than the version stored in the file.
///
/// Similar to the [`expectorate`][1] crate, users can automatically overwrite
/// the file by setting the environment variable `EXPECTORATE=overwrite` when
/// running `cargo test`.
///
/// [1]: <https://docs.rs/expectorate/latest/expectorate/>
pub fn check_metric_schema(
    path: impl AsRef<Path>,
    metric: impl Metric,
) -> anyhow::Result<()> {
    let actual = serde_json::json!({
        "name": metric.name(),
        "version": metric.version(),
        "field_names": metric
            .field_names()
            .into_iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        "field_types": metric
            .field_types()
            .into_iter()
            .map(|f| format!("{f:?}"))
            .collect::<Vec<_>>(),
        "datum_type": metric.datum_type(),
    });
    check_contents(path, actual)
}

fn check_contents(
    path: impl AsRef<Path>,
    actual: serde_json::Value,
) -> anyhow::Result<()> {
    let actual_str = serde_json::to_string_pretty(&actual)
        .context("failed to serialize JSON")?;

    // Try to read the file.
    //
    // If this fails, either to read at all or parse as JSON, overrwrite it and
    // succeed if the `EXPECTORATE=overwrite` environment variable is set.
    let is_overwrite =
        std::env::var("EXPECTORATE").map(|v| v == "overwrite").unwrap_or(false);
    let contents = match std::fs::read_to_string(path.as_ref()) {
        Ok(s) => s,
        Err(_) => {
            if is_overwrite {
                std::fs::write(path, &actual_str)
                    .context("failed to overwrite schema file")?;
                return Ok(());
            }
            anyhow::bail!(
                "Failed to read schema file. Set \
                EXPECTORATE=overwrite if you'd like \
                to overwrite the file anyway"
            );
        }
    };
    let expected: serde_json::Value = match serde_json::from_str(&contents) {
        Ok(v) => v,
        Err(_) => {
            if is_overwrite {
                std::fs::write(path, &actual_str)
                    .context("failed to overwrite schema file")?;
                return Ok(());
            }
            anyhow::bail!(
                "Failed to read schema file. Set \
                EXPECTORATE=overwrite if you'd like \
                to overwrite the file anyway"
            );
        }
    };
    if expected == actual {
        return Ok(());
    }

    // If the contents have changed, we ensure that the version is also greater.
    let (
        serde_json::Value::Number(expected_version),
        serde_json::Value::Number(actual_version)) = (&expected["version"], &actual["version"]) else {
        anyhow::bail!("expected versions to be numbers");
    };
    if expected_version.as_i64().unwrap() >= actual_version.as_i64().unwrap()
        && !is_overwrite
    {
        for hunk in TextDiff::configure()
            .algorithm(Algorithm::Myers)
            .diff_lines(&contents, &actual_str)
            .unified_diff()
            .context_radius(5)
            .iter_hunks()
        {
            println!("{}", hunk.header());
            for change in hunk.iter_changes() {
                let (marker, style) = match change.tag() {
                    ChangeTag::Delete => ('-', Style::new().red()),
                    ChangeTag::Insert => ('+', Style::new().green()),
                    ChangeTag::Equal => (' ', Style::new()),
                };
                print!("{}", style.apply_to(marker).bold());
                print!("{}", style.apply_to(change));
                if change.missing_newline() {
                    println!();
                }
            }
        }
        anyhow::bail!(
            "Schema contents are different, but the new version has \
            not been increased. If you are sure that this is what you \
            want, you can set the environment variable EXPECTORATE=overwrite \
            and re-run cargo test. Otherwise, you should update the \
            version number as well."
        );
    }

    // Rewrite the contents, having verified the update changed the version as
    // well, or the user explicitly requested an overwrite.
    std::fs::write(path, &actual_str).context("failed to rewrite schema file")
}
