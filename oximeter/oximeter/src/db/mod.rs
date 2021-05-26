//! Tools for interacting with the timeseries database.
// Copyright 2021 Oxide Computer Company

pub mod client;
mod model;

#[cfg(test)]
pub(crate) mod test_util {
    use crate::histogram;
    use crate::types::Sample;
    use uuid::Uuid;

    #[derive(oximeter::Target)]
    struct TestTarget {
        pub name1: String,
        pub name2: String,
        pub num: i64,
    }

    #[oximeter::metric(I64)]
    pub struct TestMetric {
        pub id: Uuid,
        pub good: bool,
    }

    #[oximeter::metric(HistogramF64)]
    pub struct TestHistogram {
        pub id: Uuid,
        pub good: bool,
    }

    pub fn make_sample() -> Sample {
        let target = TestTarget {
            name1: "first_name".into(),
            name2: "second_name".into(),
            num: 2,
        };
        let metric = TestMetric { id: Uuid::new_v4(), good: true };
        let sample = Sample::new(&target, &metric, 1, None);
        sample
    }

    pub fn make_hist_sample() -> Sample {
        let target = TestTarget {
            name1: "first_name".into(),
            name2: "second_name".into(),
            num: 2,
        };
        let metric = TestHistogram { id: Uuid::new_v4(), good: true };
        let mut hist = histogram::Histogram::new(&[0.0, 5.0, 10.0]).unwrap();
        hist.sample(1.0).unwrap();
        hist.sample(2.0).unwrap();
        hist.sample(6.0).unwrap();
        let sample = Sample::new(&target, &metric, hist, None);
        sample
    }
}
