//! Tools for interacting with the timeseries database.
// Copyright 2021 Oxide Computer Company

mod client;
pub mod model;
pub mod query;

pub use client::Client;

#[cfg(test)]
pub(crate) mod test_util {
    use crate::histogram;
    use crate::types::{Cumulative, Sample};
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    #[derive(oximeter::Target)]
    struct TestTarget {
        pub name1: String,
        pub name2: String,
        pub num: i64,
    }

    #[derive(oximeter::Metric)]
    pub struct TestMetric {
        pub id: Uuid,
        pub good: bool,
        pub value: i64,
    }

    #[derive(oximeter::Metric)]
    pub struct TestHistogram {
        pub id: Uuid,
        pub good: bool,
        pub value: histogram::Histogram<f64>,
    }

    pub fn make_sample() -> Sample {
        let target = TestTarget {
            name1: "first_name".into(),
            name2: "second_name".into(),
            num: 2,
        };
        let metric = TestMetric { id: Uuid::new_v4(), good: true, value: 1 };
        let sample = Sample::new(&target, &metric, None);
        sample
    }

    pub fn make_hist_sample() -> Sample {
        let target = TestTarget {
            name1: "first_name".into(),
            name2: "second_name".into(),
            num: 2,
        };
        let mut hist = histogram::Histogram::new(&[0.0, 5.0, 10.0]).unwrap();
        hist.sample(1.0).unwrap();
        hist.sample(2.0).unwrap();
        hist.sample(6.0).unwrap();
        let metric =
            TestHistogram { id: Uuid::new_v4(), good: true, value: hist };
        let sample = Sample::new(&target, &metric, None);
        sample
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
        pub cpu_id: i64,
        pub value: Cumulative<f64>,
    }

    pub fn generate_test_samples(
        n_projects: usize,
        n_instances: usize,
        n_cpus: usize,
        n_samples: usize,
        interval: Duration,
    ) -> Vec<Sample> {
        let n_timeseries = n_projects * n_instances * n_cpus;
        let mut samples = Vec::with_capacity(n_samples * n_timeseries);
        for _ in 0..n_projects {
            let project_id = Uuid::new_v4();
            for _ in 0..n_instances {
                let vm =
                    VirtualMachine { project_id, instance_id: Uuid::new_v4() };
                let start_time = Utc::now();
                for cpu in 0..n_cpus {
                    for sample in 0..n_samples {
                        let cpu_busy = CpuBusy {
                            cpu_id: cpu as _,
                            value: Cumulative::new(sample as _),
                        };
                        let sample = Sample::new(
                            &vm,
                            &cpu_busy,
                            Some(start_time + interval * sample as i32),
                        );
                        samples.push(sample);
                    }
                }
            }
        }
        samples
    }
}
