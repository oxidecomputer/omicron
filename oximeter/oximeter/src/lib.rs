//! Tools for generating and collecting metric data in the Oxide rack.
// Copyright 2021 Oxide Computer Company

pub use oximeter_macro_impl::*;

// Export the current crate as `oximeter`. The macros defined in `oximeter-macro-impl` generate
// code referring to symbols like `oximeter::traits::Target`. In consumers of this crate, that's
// fine, but internally there _is_ no crate named `oximeter`, it's just `self` or `crate`.
//
// See https://github.com/rust-lang/rust/pull/55275 for the PR introducing this fix, which links to
// lots of related issues and discussion.
extern crate self as oximeter;

pub mod collect;
pub mod db;
pub mod histogram;
pub mod oximeter_server;
pub mod producer_server;
pub mod traits;
pub mod types;

pub use oximeter_server::Oximeter;
pub use producer_server::ProducerServer;
pub use traits::{Metric, Producer, Target};
pub use types::{
    Datum, DatumType, Error, Field, FieldType, FieldValue, Measurement, Sample,
};

#[cfg(test)]
pub(crate) mod test_util {
    use crate::histogram;
    use crate::histogram::Histogram;
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

    pub fn make_sample() -> Sample {
        let target = TestTarget::default();
        let metric = TestMetric { id: Uuid::new_v4(), good: true, datum: 1 };
        Sample::new(&target, &metric)
    }

    pub fn make_hist_sample() -> Sample {
        let target = TestTarget::default();
        let mut hist = histogram::Histogram::new(&[0.0, 5.0, 10.0]).unwrap();
        hist.sample(1.0).unwrap();
        hist.sample(2.0).unwrap();
        hist.sample(6.0).unwrap();
        let metric =
            TestHistogram { id: Uuid::new_v4(), good: true, datum: hist };
        Sample::new(&target, &metric)
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
                let vm =
                    VirtualMachine { project_id, instance_id: Uuid::new_v4() };
                for cpu in 0..n_cpus {
                    for sample in 0..n_samples {
                        let cpu_busy = CpuBusy {
                            cpu_id: cpu as _,
                            datum: Cumulative::new(sample as f64),
                        };
                        let sample = Sample::new(&vm, &cpu_busy);
                        samples.push(sample);
                    }
                }
            }
        }
        samples
    }
}
