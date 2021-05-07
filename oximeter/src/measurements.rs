use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use oximeter_types::{FieldType, FieldValue};
use schemars::JsonSchema;
use serde::{Serialize, Deserialize};

use crate::{Fields, Metric, Target};

#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema)]
pub struct FieldInfo {
    pub name: String,
    pub field_names: Vec<String>,
    pub field_types: Vec<FieldType>,
    pub field_values: Vec<FieldValue>,
    pub key: String,
}

impl<T: Fields> From<&T> for FieldInfo {
    fn from(fields: &T) -> Self {
        Self {
            name: fields.name().to_string(),
            field_names: fields
                .field_names()
                .iter()
                .map(|&name| name.to_string())
                .collect(),
            field_types: fields.field_types().to_vec(),
            field_values: fields.field_values().to_vec(),
            key: fields.key().clone(),
        }
    }
}

/*
#[derive(Debug, Error)]
pub enum DistributionError<'a, T>
where T:
    std::fmt::Debug + Copy + Add + AddAssign + Default + PartialOrd + PartialEq + Bounded + JsonSchema + Serialize + Deserialize<'a>
{
    #[error(transparent)]
    Other(#[from] crate::distribution::DistributionError<'static, T>),
    #[error("Distribution bins must be set prior to sampling data")]
    BinsNotSet,
    #[error("Distribution bins have already been set")]
    BinsAlreadySet,
}
*/

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MeasurementInfo {
    pub target: FieldInfo,
    pub metric: FieldInfo,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
}

impl MeasurementInfo {
    pub fn new<T: Target, M: Metric>(target: &T, metric: &M) -> Self {
        let now = Utc::now();
        Self {
            target: target.into(),
            metric: metric.into(),
            created: now,
            updated: now,
        }
    }

    pub fn touch(&mut self) {
        self.updated = Utc::now();
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct Measurements {
    #[serde(skip)]
    pub cumulative_i64_lock: Arc<RwLock<()>>,
    pub cumulative_i64: BTreeMap<String, (MeasurementInfo, i64)>,

    #[serde(skip)]
    pub cumulative_f64_lock: Arc<RwLock<()>>,
    pub cumulative_f64: BTreeMap<String, (MeasurementInfo, f64)>,

    #[serde(skip)]
    pub gauge_bool_lock: Arc<RwLock<()>>,
    pub gauge_bool: BTreeMap<String, (MeasurementInfo, bool)>,

    #[serde(skip)]
    pub gauge_i64_lock: Arc<RwLock<()>>,
    pub gauge_i64: BTreeMap<String, (MeasurementInfo, i64)>,

    #[serde(skip)]
    pub gauge_f64_lock: Arc<RwLock<()>>,
    pub gauge_f64: BTreeMap<String, (MeasurementInfo, f64)>,

    #[serde(skip)]
    pub gauge_string_lock: Arc<RwLock<()>>,
    pub gauge_string: BTreeMap<String, (MeasurementInfo, String)>,

    #[serde(skip)]
    pub gauge_bytes_lock: Arc<RwLock<()>>,
    pub gauge_bytes: BTreeMap<String, (MeasurementInfo, Bytes)>,

    /*
    pub distribution_i64: Arc<RwLock<BTreeMap<String, (MeasurementInfo, Distribution<'a, i64>)>>>,
    pub distribution_f64: Arc<RwLock<BTreeMap<String, (MeasurementInfo, Distribution<'a, f64>)>>>,
    */
}

macro_rules! increment_impl {
    {$name:ident, $delegate_name:ident} => {
        pub fn $name<T: Target, M: Metric>(&mut self, target: &T, metric: &M) {
            self.$delegate_name(target, metric, 1 as _);
        }
    }
}

macro_rules! add_impl {
    {$name:ident, $lock:ident, $map:ident, $t:ty} => {
        pub fn $name<T: Target, M: Metric>(&mut self, target: &T, metric: &M, value: $t) {
            let key = format!("{}:{}", target.key(), metric.key());
            let _lock = self.$lock.write().unwrap();
            //let mut map = self.$map
            match self.$map.entry(key) {
                Entry::Vacant(v) => {
                    v.insert((MeasurementInfo::new(target, metric), value));
                }
                Entry::Occupied(mut v) => {
                    let item = v.get_mut();
                    item.0.touch();
                    item.1 += value;
                }
            }
        }
    }
}

macro_rules! set_impl {
    {$name:ident, $lock:ident, $map:ident, $t:ty, $clone:ident} => {
        pub fn $name<T: Target, M: Metric>(&mut self, target: &T, metric: &M, value: $t) {
            let key = format!("{}:{}", target.key(), metric.key());
            let _lock = self.$lock.write().unwrap();
            match self.$map.entry(key) {
                Entry::Vacant(v) => {
                    v.insert((MeasurementInfo::new(target, metric), value.$clone()));
                }
                Entry::Occupied(mut v) => {
                    let item = v.get_mut();
                    item.0.touch();
                    item.1 = value.$clone();
                }
            }
        }
    };
    {$name:ident, $lock:ident, $map:ident, $t:ty} => {
        set_impl!{$name, $lock, $map, $t, clone}
    };
}

/*
macro_rules! set_bins_impl {
    {$name:ident, $map:ident, $t:ty} => {
        pub fn $name<T: Target, M: Metric>(&mut self, target: &T, metric: &M, bins: &[$t]) -> Result<(), DistributionError<$t>> {
            let key = format!("{}:{}", target.key(), metric.key());
            let mut map = self.$map.write().unwrap();
            match map.entry(key) {
                Entry::Vacant(v) => {
                    v.insert((MeasurementInfo::new(target, metric), Distribution::new(bins)?));
                    Ok(())
                }
                Entry::Occupied(_) => {
                    Err(DistributionError::BinsAlreadySet)
                }
            }
        }
    }
}

macro_rules! sample_impl {
    {$name:ident, $map:ident, $t:ty} => {
        pub fn $name<T: Target, M: Metric>(&mut self, target: &T, metric: &M, value: $t) -> Result<(), DistributionError<$t>> {
            let key = format!("{}:{}", target.key(), metric.key());
            let mut map = self.$map.write().unwrap();
            match map.entry(key) {
                Entry::Vacant(_) => {
                    Err(DistributionError::BinsNotSet)
                }
                Entry::Occupied(mut v) => {
                    let item = v.get_mut();
                    item.0.touch();
                    item.1.sample(value)?;
                    Ok(())
                }
            }
        }
    }
}
*/

impl Measurements {
    pub fn new() -> Self {
        Self::default()
    }

    increment_impl! {increment_i64, add_i64}
    increment_impl! {increment_f64, add_f64}
    add_impl! {add_i64, cumulative_i64_lock, cumulative_i64, i64}
    add_impl! {add_f64, cumulative_f64_lock, cumulative_f64, f64}
    set_impl! {set_bool, gauge_bool_lock, gauge_bool, bool}
    set_impl! {set_i64, gauge_i64_lock, gauge_i64, i64}
    set_impl! {set_f64, gauge_f64_lock, gauge_f64, f64}
    set_impl! {set_string, gauge_string_lock, gauge_string, &str, to_string}
    set_impl! {set_bytes, gauge_bytes_lock, gauge_bytes, &Bytes}
    /*
    set_bins_impl! {set_bins_i64, distribution_i64, i64}
    set_bins_impl! {set_bins_f64, distribution_f64, f64}
    sample_impl! {sample_i64, distribution_i64, i64}
    sample_impl! {sample_f64, distribution_f64, f64}
    */
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[derive(Debug, Target)]
    struct TestTarget {
        f0: String,
        f1: i64,
    }

    #[derive(Debug, Metric)]
    struct TestMetric {
        f0: bool,
        f1: Uuid,
    }

    fn test_fields() -> (TestTarget, TestMetric) {
        (
            TestTarget {
                f0: "f0".to_string(),
                f1: 1,
            },
            TestMetric {
                f0: false,
                f1: Uuid::new_v4(),
            },
        )
    }

    macro_rules! test_add {
        {$add_method:ident, $map:ident, $first:expr, $second:expr} => {
            let mut m = Measurements::new();
            let (target, metric) = test_fields();

            m.$add_method(&target, &metric, $first);
            let expected_key = format!("{}:{}", target.key(), metric.key());
            {
                let map = m.$map.read().unwrap();
                let item = map.get(&expected_key).unwrap();
                assert_eq!(item.1, $first);
            }

            m.$add_method(&target, &metric, $second);
            {
                let map = m.$map.read().unwrap();
                let item = map.get(&expected_key).unwrap();
                assert_eq!(item.1, $first + $second);
            }

            assert!(m.$map.read().unwrap().get(&"bad-key".to_string()).is_none());
        }
    }

    #[test]
    fn test_producer_add_i64() {
        test_add! {add_i64, cumulative_i64, 1, 10}
    }

    #[test]
    fn test_producer_add_f64() {
        test_add! {add_f64, cumulative_f64, 1.0, 10.0}
    }

    macro_rules! test_set {
        {$set_method:ident, $map:ident, $first:expr, $second:expr} => {
            let mut m = Measurements::new();
            let (target, metric) = test_fields();

            m.$set_method(&target, &metric, $first);
            let expected_key = format!("{}:{}", target.key(), metric.key());
            {
                let map = m.$map.read().unwrap();
                let item = map.get(&expected_key).unwrap();
                assert_eq!(item.1, $first);
            }

            m.$set_method(&target, &metric, $second);
            {
                let map = m.$map.read().unwrap();
                let item = map.get(&expected_key).unwrap();
                assert_eq!(item.1, $second);
            }

            {
                let map = m.$map.read().unwrap();
                let item = map.get(&"bad-key".to_string());
                assert!(item.is_none());
            }
        }
    }

    #[test]
    fn test_producer_set_bool() {
        test_set! {set_bool, gauge_bool, true, false}
    }

    #[test]
    fn test_producer_set_i64() {
        test_set! {set_i64, gauge_i64, 10, 1}
    }

    #[test]
    fn test_producer_set_f64() {
        test_set! {set_f64, gauge_f64, 10.0, 1.0}
    }

    #[test]
    fn test_producer_set_string() {
        test_set! {set_string, gauge_string, "first", "second"}
    }

    #[test]
    fn test_producer_set_bytes() {
        test_set! {set_bytes, gauge_bytes, &Bytes::from("first"), &Bytes::from("second")}
    }

    /*
    macro_rules! test_sample {
        {$sample_method:ident, $set_bins:ident, $map:ident, $bins:expr, $samples:expr, $expected_counts:expr} => {
            let mut m = Measurements::new();
            let (target, metric) = test_fields();
            let expected_key = format!("{}:{}", target.key(), metric.key());
            let sample = $bins[0];
            assert!(
                matches!(m.$sample_method(&target, &metric, sample), Err(DistributionError::BinsNotSet)),
                "Inserting a sample without setting bins should fail"
            );
            m.$set_bins(&target, &metric, $bins).expect("Failed to set bins");
            assert!(
                matches!(m.$set_bins(&target, &metric, $bins), Err(DistributionError::BinsAlreadySet)),
                "Setting bins that have previously been set should be an error"
            );

            for (i, &sample) in $samples.iter().enumerate() {
                let count = i as u64 + 1;
                m.$sample_method(&target, &metric, sample).expect("Failed to insert sample");
                let map = m.$map.read().unwrap();
                assert_eq!(
                    count,
                    map.get(&expected_key)
                        .unwrap()
                        .1
                        .n_samples(),
                    "Failed to correctly account for new sample"
                );
            }

            let map = m.$map.read().unwrap();
            let distribution = &map.get(&expected_key).unwrap().1;
            for (expected, (bin, count)) in $expected_counts.iter().zip(distribution.iter()) {
                assert_eq!(
                    expected,
                    count,
                    "Expected {} items in bin {:?}, found {}\n{:#?}",
                    expected,
                    bin,
                    count,
                    distribution,
                );
            }
        }
    }

    #[test]
    fn test_producer_sample_i64() {
        let bins = vec![0i64, 10, 20];
        let samples = vec![-1i64, 1, 5];
        let expected_counts = vec![1u64, 2, 0];
        test_sample! {sample_i64, set_bins_i64, distribution_i64, &bins, &samples, &expected_counts}
    }

    #[test]
    fn test_producer_sample_f64() {
        let bins = vec![0f64, 1.0, 2.0];
        let samples = vec![-1f64, 0.1, 0.2];
        let expected_counts = vec![1u64, 2, 0];
        test_sample! {sample_f64, set_bins_f64, distribution_f64, &bins, &samples, &expected_counts}
    }
    */
}
