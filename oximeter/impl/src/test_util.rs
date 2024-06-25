// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for testing the oximeter crate.
// Copyright 2024 Oxide Computer Company

use crate::histogram;
use crate::histogram::{Histogram, Record};
use crate::types::{Cumulative, Sample};
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

const ID: Uuid = uuid::uuid!("e00ced4d-39d1-446a-ae85-a67f05c9750b");

pub fn make_sample() -> Sample {
    let target = TestTarget::default();
    let metric = TestMetric { id: ID, good: true, datum: 1 };
    Sample::new(&target, &metric).unwrap()
}

pub fn make_missing_sample() -> Sample {
    let target = TestTarget::default();
    let metric = TestMetric { id: ID, good: true, datum: 1 };
    Sample::new_missing(&target, &metric).unwrap()
}

pub fn make_hist_sample() -> Sample {
    let target = TestTarget::default();
    let mut hist = histogram::Histogram::new(&[0.0, 5.0, 10.0]).unwrap();
    hist.sample(1.0).unwrap();
    hist.sample(2.0).unwrap();
    hist.sample(6.0).unwrap();
    let metric = TestHistogram { id: ID, good: true, datum: hist };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_test_samples() {
        let (n_projects, n_instances, n_cpus, n_samples) = (2, 2, 2, 2);
        let samples =
            generate_test_samples(n_projects, n_instances, n_cpus, n_samples);
        assert_eq!(
            samples.len(),
            n_projects * n_instances * n_cpus * n_samples
        );
    }
}
