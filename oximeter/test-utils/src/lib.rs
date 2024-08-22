// Copyright 2024 Oxide Computer Company

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for testing the oximeter crate.

// Export the current crate as `oximeter`. The macros defined in `oximeter-macro-impl` generate
// code referring to symbols like `oximeter::traits::Target`. In consumers of this crate, that's
// fine, but internally there _is_ no crate named `oximeter`, it's just `self` or `crate`.
//
// See https://github.com/rust-lang/rust/pull/55275 for the PR introducing this fix, which links to
// lots of related issues and discussion.
extern crate self as oximeter;

use oximeter_macro_impl::{Metric, Target};
use oximeter_types::histogram;
use oximeter_types::histogram::{Histogram, Record};
use oximeter_types::traits;
use oximeter_types::types::{
    Cumulative, Datum, DatumType, FieldType, FieldValue, Measurement, Sample,
};
use oximeter_types::{Metric, Target};
use uuid::Uuid;

#[derive(Target)]
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

#[derive(Metric)]
pub struct TestMetric {
    pub id: Uuid,
    pub good: bool,
    pub datum: i64,
}

#[derive(Metric)]
pub struct TestCumulativeMetric {
    pub id: Uuid,
    pub good: bool,
    pub datum: Cumulative<i64>,
}

#[derive(Metric)]
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
    use chrono::Utc;
    use oximeter_types::{
        schema::{
            default_schema_version, AuthzScope, FieldSchema, FieldSource,
            TimeseriesSchema, Units,
        },
        TimeseriesName,
    };

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

    #[test]
    fn test_sample_struct() {
        let t = TestTarget::default();
        let m = TestMetric { id: Uuid::new_v4(), good: true, datum: 1i64 };
        let sample = Sample::new(&t, &m).unwrap();
        assert_eq!(
            sample.timeseries_name,
            format!("{}:{}", t.name(), m.name())
        );
        assert!(sample.measurement.start_time().is_none());
        assert_eq!(sample.measurement.datum(), &Datum::from(1i64));

        let m = TestCumulativeMetric {
            id: Uuid::new_v4(),
            good: true,
            datum: 1i64.into(),
        };
        let sample = Sample::new(&t, &m).unwrap();
        assert!(sample.measurement.start_time().is_some());
    }

    #[derive(Target)]
    struct MyTarget {
        id: Uuid,
        name: String,
    }

    const ID: Uuid = uuid::uuid!("ca565ef4-65dc-4ab0-8622-7be43ed72105");

    impl Default for MyTarget {
        fn default() -> Self {
            Self { id: ID, name: String::from("name") }
        }
    }

    #[derive(Metric)]
    struct MyMetric {
        happy: bool,
        datum: u64,
    }

    impl Default for MyMetric {
        fn default() -> Self {
            Self { happy: true, datum: 0 }
        }
    }

    #[test]
    fn test_timeseries_schema_from_parts() {
        let target = MyTarget::default();
        let metric = MyMetric::default();
        let schema = TimeseriesSchema::new(&target, &metric).unwrap();

        assert_eq!(schema.timeseries_name, "my_target:my_metric");
        let f = schema.schema_for_field("id").unwrap();
        assert_eq!(f.name, "id");
        assert_eq!(f.field_type, FieldType::Uuid);
        assert_eq!(f.source, FieldSource::Target);

        let f = schema.schema_for_field("name").unwrap();
        assert_eq!(f.name, "name");
        assert_eq!(f.field_type, FieldType::String);
        assert_eq!(f.source, FieldSource::Target);

        let f = schema.schema_for_field("happy").unwrap();
        assert_eq!(f.name, "happy");
        assert_eq!(f.field_type, FieldType::Bool);
        assert_eq!(f.source, FieldSource::Metric);
        assert_eq!(schema.datum_type, DatumType::U64);
    }

    #[test]
    fn test_timeseries_schema_from_sample() {
        let target = MyTarget::default();
        let metric = MyMetric::default();
        let sample = Sample::new(&target, &metric).unwrap();
        let schema = TimeseriesSchema::new(&target, &metric).unwrap();
        let schema_from_sample = TimeseriesSchema::from(&sample);
        assert_eq!(schema, schema_from_sample);
    }

    // Test that we correctly order field across a target and metric.
    //
    // In an earlier commit, we switched from storing fields in an unordered Vec
    // to using a BTree{Map,Set} to ensure ordering by name. However, the
    // `TimeseriesSchema` type stored all its fields by chaining the sorted
    // fields from the target and metric, without then sorting _across_ them.
    //
    // This was exacerbated by the error reporting, where we did in fact sort
    // all fields across the target and metric, making it difficult to tell how
    // the derived schema was different, if at all.
    //
    // This test generates a sample with a schema where the target and metric
    // fields are sorted within them, but not across them. We check that the
    // derived schema are actually equal, which means we've imposed that
    // ordering when deriving the schema.
    #[test]
    fn test_schema_field_ordering_across_target_metric() {
        let target_field = FieldSchema {
            name: String::from("later"),
            field_type: FieldType::U64,
            source: FieldSource::Target,
            description: String::new(),
        };
        let metric_field = FieldSchema {
            name: String::from("earlier"),
            field_type: FieldType::U64,
            source: FieldSource::Metric,
            description: String::new(),
        };
        let timeseries_name: TimeseriesName = "foo:bar".parse().unwrap();
        let datum_type = DatumType::U64;
        let field_schema =
            [target_field.clone(), metric_field.clone()].into_iter().collect();
        let expected_schema = TimeseriesSchema {
            timeseries_name,
            description: Default::default(),
            field_schema,
            datum_type,
            version: default_schema_version(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Count,
            created: Utc::now(),
        };

        #[derive(oximeter::Target)]
        struct Foo {
            later: u64,
        }
        #[derive(oximeter::Metric)]
        struct Bar {
            earlier: u64,
            datum: u64,
        }

        let target = Foo { later: 1 };
        let metric = Bar { earlier: 2, datum: 10 };
        let sample = Sample::new(&target, &metric).unwrap();
        let derived_schema = TimeseriesSchema::from(&sample);
        assert_eq!(derived_schema, expected_schema);
    }
}
