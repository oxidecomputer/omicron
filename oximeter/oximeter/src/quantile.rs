use crate::traits::HistogramSupport;
use num::traits::Float;
use num::traits::ToPrimitive;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::min;
use std::ops::Sub;
use thiserror::Error;

/// Errors related to constructing a `Quantile` instance or estimating the p-quantile.
#[derive(Debug, Clone, Error, JsonSchema, Serialize, Deserialize)]
#[serde(tag = "type", content = "content", rename_all = "snake_case")]
pub enum QuantileError {
    /// The p value must be in the range [0, 1].
    #[error("The p value must be in the range [0, 1].")]
    InvalidPValue,
    /// Quantile estimation is not possible without samples.
    #[error("Quantile estimation is not possible without samples.")]
    EmptyQuantile,
    /// A non-finite was encountered, either as a bin edge or a sample.
    #[error("Samples must be finite values, found: {0:?}")]
    NonFiniteValue(String),
}

/// Structure for estimating the p-quantile of a population.
///
/// This is based on the P² algorithm for estimating quantiles using
/// constant space.
///
/// The algorithm consists of maintaining five markers: the
/// minimum, the p/2-, p-, and (1 + p)/2 quantiles, and the maximum.
///
/// Read <https://www.cs.wustl.edu/~jain/papers/ftp/psqr.pdf> for more.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Quantile {
    /// The heights of the markers.
    #[serde(rename = "marker_heights")]
    pub marker_hghts: [f64; 5],
    /// The positions of the markers.
    #[serde(rename = "marker_positions")]
    pub marker_pos: [i64; 5],
    /// The desired marker positions.
    #[serde(rename = "desired_marker_positions")]
    pub desired_marker_pos: [f64; 5],
    /// The increment for the desired marker positions.
    #[serde(rename = "desired_marker_increments")]
    pub desired_marker_incrs: [f64; 5],
}

impl Quantile {
    /// Create a new `Quantile` instance.
    pub fn new(p: u64) -> Result<Self, QuantileError> {
        let p = p as f64 / 100.;
        if p < 0. || p > 1. {
            return Err(QuantileError::InvalidPValue);
        }

        Ok(Self {
            marker_hghts: [0.; 5],
            marker_pos: [1, 2, 3, 4, 0],
            desired_marker_pos: [1., 1. + 2. * p, 1. + 4. * p, 3. + 2. * p, 5.],
            desired_marker_incrs: [0., p / 2., p, (1. + p) / 2., 1.],
        })
    }

    /// Create a new `Quantile` instance from the given marker heights and
    /// positions.
    pub fn from(
        marker_hghts: [f64; 5],
        marker_pos: [i64; 5],
        desired_marker_pos: [f64; 5],
        desired_marker_incrs: [f64; 5],
    ) -> Self {
        Self {
            marker_hghts,
            marker_pos,
            desired_marker_pos,
            desired_marker_incrs,
        }
    }

    /// Construct a `Quantile` instance for the 50th/median percentile.
    pub fn p50() -> Self {
        Self::new(50).unwrap()
    }

    /// Construct a `Quantile` instance for the 90th percentile.
    pub fn p90() -> Self {
        Self::new(90).unwrap()
    }

    /// Construct a `Quantile` instance for the 95th percentile.
    pub fn p95() -> Self {
        Self::new(95).unwrap()
    }

    /// Construct a `Quantile` instance for the 99th percentile.
    pub fn p99() -> Self {
        Self::new(99).unwrap()
    }

    /// Get the p value as a float.
    fn _p(&self) -> f64 {
        self.desired_marker_incrs[2]
    }

    /// Get the p value as an integer.
    pub fn p(&self) -> u64 {
        (self.desired_marker_incrs[2] * 100.0) as u64
    }

    /// Estimate the p-quantile of the population.
    ///
    /// Returns an error if the sample is empty.
    pub fn estimate(&self) -> Result<f64, QuantileError> {
        // Return NaN if the sample is empty.
        if self.is_empty() {
            return Err(QuantileError::EmptyQuantile);
        }

        // Return the middle marker height if the sample size is at least 5.
        if self.len() >= 5 {
            return Ok(self.marker_hghts[2]);
        }

        let mut heights: [f64; 4] = [
            self.marker_hghts[0],
            self.marker_hghts[1],
            self.marker_hghts[2],
            self.marker_hghts[3],
        ];

        let len = self.len() as usize;
        float_ord::sort(&mut heights[..len]);

        let desired_index = (len as f64) * self._p() - 1.;
        let mut index = desired_index.ceil();
        if desired_index == index && index >= 0. {
            let index = index.round_ties_even() as usize;
            if index < len - 1 {
                // `marker_hghts[index]` and `marker_hghts[index + 1]` are
                // equally valid estimates,  by convention we take their average.
                return Ok(0.5 * self.marker_hghts[index]
                    + 0.5 * self.marker_hghts[index + 1]);
            }
        }
        index = index.max(0.);
        let mut index = index.round_ties_even() as usize;
        index = min(index, len - 1);
        Ok(self.marker_hghts[index])
    }

    /// Return the sample size.
    pub fn len(&self) -> u64 {
        self.marker_pos[4] as u64
    }

    /// Determine whether the sample is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append a value/observation to the population and adjust the heights.
    pub fn append<T>(&mut self, value: T) -> Result<(), QuantileError>
    where
        T: HistogramSupport,
    {
        if !value.is_finite() {
            return Err(QuantileError::NonFiniteValue(format!("{:?}", value)));
        }

        // we've already checked that the value is finite.
        let value = value.to_f64().unwrap();

        // n[4] is the sample size.
        if self.marker_pos[4] < 5 {
            self.marker_hghts[self.marker_pos[4] as usize] = value;
            self.marker_pos[4] += 1;
            if self.marker_pos[4] == 5 {
                float_ord::sort(&mut self.marker_hghts);
            }
            return Ok(());
        }

        // Find cell k.
        let mut k: usize;
        if value < self.marker_hghts[0] {
            self.marker_hghts[0] = value;
            k = 0;
        } else {
            k = 4;
            for i in 1..5 {
                if value < self.marker_hghts[i] {
                    k = i;
                    break;
                }
            }
            if self.marker_hghts[4] < value {
                self.marker_hghts[4] = value;
            }
        };

        // Increment all positions greater than k.
        for i in k..5 {
            self.marker_pos[i] += 1;
        }
        for i in 0..5 {
            self.desired_marker_pos[i] += self.desired_marker_incrs[i];
        }

        // Adjust height of markers.
        for i in 1..4 {
            // unwrap is safe because we know the exact marker positions as literals.
            let d = self.desired_marker_pos[i]
                - self.marker_pos[i].to_f64().unwrap();
            if d >= 1. && self.marker_pos[i + 1] - self.marker_pos[i] > 1
                || d <= -1. && self.marker_pos[i - 1] - self.marker_pos[i] < -1
            {
                let d = Float::signum(d);
                let q_new = self.parabolic(i, d);
                if self.marker_hghts[i - 1] < q_new
                    && q_new < self.marker_hghts[i + 1]
                {
                    self.marker_hghts[i] = q_new;
                } else {
                    self.marker_hghts[i] = self.linear(i, d);
                }
                let delta = d.round_ties_even() as i64;
                debug_assert_eq!(delta.abs(), 1);
                self.marker_pos[i] += delta;
            }
        }

        Ok(())
    }

    /// Subtract another `Quantile` instance from this one.
    pub fn sub(&self, other: &Quantile) -> Quantile {
        /// Nested function to subtract elements of two arrays and return the
        /// result as an array.
        fn sub_arrays<T>(arr1: &[T; 5], arr2: &[T; 5]) -> [T; 5]
        where
            T: Sub<Output = T> + Copy,
        {
            // Initialize with the first element of arr1 (or any default value).
            let mut result = [arr1[0]; 5];
            for i in 0..5 {
                result[i] = arr1[i] - arr2[i];
            }
            result
        }

        Quantile {
            marker_hghts: sub_arrays(&self.marker_hghts, &other.marker_hghts),
            marker_pos: sub_arrays(&self.marker_pos, &other.marker_pos),
            desired_marker_pos: sub_arrays(
                &self.desired_marker_pos,
                &other.desired_marker_pos,
            ),
            desired_marker_incrs: sub_arrays(
                &self.desired_marker_incrs,
                &other.desired_marker_incrs,
            ),
        }
    }

    /// Parabolic prediction for marker height.
    fn parabolic(&self, i: usize, d: f64) -> f64 {
        let term1 =
            d / (self.marker_pos[i + 1] - self.marker_pos[i - 1]) as f64;
        let term2 = ((self.marker_pos[i] - self.marker_pos[i - 1]) as f64 + d)
            * (self.marker_hghts[i + 1] - self.marker_hghts[i])
            / (self.marker_pos[i + 1] - self.marker_pos[i]) as f64;
        let term3 = ((self.marker_pos[i + 1] - self.marker_pos[i]) as f64 - d)
            * (self.marker_hghts[i] - self.marker_hghts[i - 1])
            / (self.marker_pos[i] - self.marker_pos[i - 1]) as f64;

        self.marker_hghts[i] + term1 * (term2 + term3)
    }

    /// Linear prediction for marker height.
    fn linear(&self, i: usize, d: f64) -> f64 {
        let index = if d < 0. { i - 1 } else { i + 1 };
        self.marker_hghts[i]
            + d * (self.marker_hghts[index] - self.marker_hghts[i])
                / (self.marker_pos[index] - self.marker_pos[i]) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oximeter::test_util::assert_almost_eq;

    #[test]
    fn test_float_observations() {
        let observations = [
            0.02, 0.5, 0.74, 3.39, 0.83, 22.37, 10.15, 15.43, 38.62, 15.92,
            34.60, 10.28, 1.47, 0.40, 0.05, 11.39, 0.27, 0.42, 0.09, 11.37,
        ];
        let mut q = Quantile::p50();
        for &o in observations.iter() {
            q.append(o).unwrap();
        }
        assert_eq!(q.marker_pos, [1, 6, 10, 16, 20]);
        assert_eq!(q.desired_marker_pos, [1., 5.75, 10.50, 15.25, 20.0]);
        assert_eq!(q.len(), 20);
        assert_eq!(q.p(), 50);
        assert_almost_eq!(q.estimate().unwrap(), 4.2462394088036435, 2e-15);
    }

    #[test]
    fn test_integer_observations() {
        let observations = 1..=100;
        let mut q = Quantile::new(30).unwrap();
        for o in observations {
            q.append(o).unwrap();
        }

        assert_eq!(q.marker_pos, [1, 15, 30, 65, 100]);
        assert_eq!(
            q.desired_marker_pos,
            [
                1.0,
                15.850000000000026,
                30.70000000000005,
                65.34999999999992,
                100.0
            ]
        );
        assert_eq!(q.len(), 100);
        assert_eq!(q.p(), 30);
        assert_eq!(q.estimate().unwrap(), 30.0);
    }

    #[test]
    fn test_p50() {
        let observations = 1..=100;
        let mut q = Quantile::p50();
        for o in observations {
            q.append(o).unwrap();
        }
        assert_eq!(q.p(), 50);
        assert_eq!(q.estimate().unwrap(), 50.0);
    }

    #[test]
    fn test_p90() {
        let observations = 1..=100;
        let mut q = Quantile::p90();
        for o in observations {
            q.append(o).unwrap();
        }
        assert_eq!(q.p(), 90);
        assert_eq!(q.estimate().unwrap(), 90.0);
    }

    #[test]
    fn test_p95() {
        let observations = 1..=100;
        let mut q = Quantile::p95();
        for o in observations {
            q.append(o).unwrap();
        }
        assert_eq!(q.p(), 95);
        assert_eq!(q.estimate().unwrap(), 95.0);
    }

    #[test]
    fn test_p99() {
        let observations = 1..=100;
        let mut q = Quantile::p99();
        for o in observations {
            q.append(o).unwrap();
        }
        assert_eq!(q.p(), 99);
        assert_eq!(q.estimate().unwrap(), 97.0);
    }

    #[test]
    fn test_empty_sample() {
        let q = Quantile::p50();
        assert!(q.is_empty());
        assert!(q.estimate().is_err());
    }

    #[test]
    fn test_invalid_p_value() {
        assert!(Quantile::new(101).is_err());
        assert!(Quantile::new(u64::MAX).is_err());
    }
}
