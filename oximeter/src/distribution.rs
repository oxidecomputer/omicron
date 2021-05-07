use std::ops::RangeInclusive;

use num_traits::Bounded;
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

pub trait DistributionSupport:
    std::fmt::Debug
    + Copy
    + Default
    + PartialOrd
    + Bounded
    + JsonSchema
    + Serialize
    + DeserializeOwned
    + Clone
    + 'static
{
    fn min_value() -> Self {
        <Self as Bounded>::min_value()
    }

    fn max_value() -> Self {
        <Self as Bounded>::max_value()
    }
}

impl DistributionSupport for i64 {}

impl DistributionSupport for f64 {
    fn min_value() -> Self {
        f64::NEG_INFINITY
    }

    fn max_value() -> Self {
        f64::INFINITY
    }
}

#[derive(Debug, Clone, Error)]
pub enum DistributionError {
    #[error("Bins may not be empty")]
    EmptyBins,
    #[error("Bins must be monotonically increasing")]
    NonmonotonicBins,
    #[error(
        "Bin edges and samples must form a total order, but an incomparable value was found: {0:?}"
    )]
    IncomparableValue(String),
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Distribution<T> {
    bins: Vec<RangeInclusive<T>>,
    counts: Vec<u64>,
    n_samples: u64,
}

impl<T> Distribution<T>
where
    T: DistributionSupport,
{
    pub fn new(right_edges: &[T]) -> Result<Self, DistributionError> {
        if right_edges.is_empty() {
            return Err(DistributionError::EmptyBins);
        }

        let mut items = right_edges.iter();
        let mut bins = Vec::with_capacity(right_edges.len() + 1);
        let mut current = items.next().unwrap(); // Already asserted to be nonempty
        bins.push(DistributionSupport::min_value()..=*current);
        for next in items {
            if current < next {
                bins.push(*current..=*next);
                current = next;
            } else if current >= next {
                return Err(DistributionError::NonmonotonicBins);
            } else {
                return Err(DistributionError::IncomparableValue(format!("{:?}", *next)));
            }
        }
        bins.push(*current..=DistributionSupport::max_value());
        Ok(Self {
            bins: bins.clone(),
            counts: vec![0; bins.len()],
            n_samples: 0,
        })
    }

    pub fn sample(&mut self, value: T) -> Result<(), DistributionError> {
        // TODO(performance): Binary search should be possible, but getting
        // the index is not straightforward.
        let index = self
            .bins
            .iter()
            .position(|bin| bin.contains(&value))
            .ok_or_else(|| DistributionError::IncomparableValue(format!("{:?}", value)))?;
        self.counts[index] += 1;
        self.n_samples += 1;
        Ok(())
    }

    pub fn bins(&self) -> &Vec<RangeInclusive<T>> {
        &self.bins
    }

    pub fn n_samples(&self) -> u64 {
        self.n_samples
    }

    pub fn len(&self) -> usize {
        self.counts.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&RangeInclusive<T>, &u64)> {
        self.bins.iter().zip(self.counts.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distribution() {
        let mut dist = Distribution::new(&vec![0, 10, 20]).unwrap();
        assert_eq!(
            dist.len(),
            4,
            "Distribution should have 1 more bin than bin edges specified"
        );
        assert_eq!(
            dist.n_samples(),
            0,
            "Distribution should init with 0 samples"
        );

        let samples = [-10i64, 0, 1, 10, 50];
        let expected_counts = [2u64, 2, 0, 1];
        for (i, sample) in samples.iter().enumerate() {
            dist.sample(*sample).unwrap();
            let count = i as u64 + 1;
            assert_eq!(
                dist.n_samples(),
                count,
                "Distribution should have {} sample(s)",
                count
            );
        }

        for ((bin, count), expected_count) in dist.iter().zip(expected_counts.iter()) {
            assert_eq!(
                count, expected_count,
                "Bin {:?} expected to have {} items, but found {}",
                bin, expected_count, count
            );
        }
    }

    #[test]
    fn test_distribution_unsorted_bins() {
        assert!(
            Distribution::new(&vec![0, -10, 1]).is_err(),
            "Expected an Err when building a distribution with unsorted bins"
        );
    }

    #[test]
    fn test_distribution_unbounded_samples() {
        let mut dist = Distribution::new(&vec![0.0, 1.0]).unwrap();
        assert!(
            dist.sample(f64::NAN).is_err(),
            "Expected an Err when sampling NaN into a distribution"
        );
        assert!(
            dist.sample(f64::NEG_INFINITY).is_ok(),
            "Expected OK when sampling negative infinity into a distribution"
        );
    }
}
