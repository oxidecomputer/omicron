// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Data structure for expressing quantile estimation.
//! This is based on the P² heuristic algorithm for dynamic
//! calculation of the median and other quantiles. The estimates
//! are produced dynamically as the observations are generated.
//! The observations are not stored; therefore, the algorithm has
//! a very small and fixed storage requirement regardless of the
//! number of observations.
//!
//! Read the [paper](https://www.cs.wustl.edu/~jain/papers/ftp/psqr.pdf)
//! for more specifics.

// Copyright 2024 Oxide Computer Company

use crate::traits::HistogramSupport;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

const FILLED_MARKER_LEN: usize = 5;

/// Errors related to constructing a `Quantile` instance or estimating the
/// p-quantile.
#[derive(
    Debug, Clone, Error, JsonSchema, Serialize, Deserialize, PartialEq,
)]
#[serde(tag = "type", content = "content", rename_all = "snake_case")]
pub enum QuantileError {
    /// The p value must be in the range [0, 1].
    #[error("The p value must be in the range [0, 1].")]
    InvalidPValue,
    /// Quantile estimation is not possible without samples.
    #[error("Quantile estimation is not possible without any samples.")]
    InsufficientSampleSize,
    /// A non-finite was encountered, either as a bin edge or a sample.
    #[error("Samples must be finite values, not Infinity or NaN.")]
    NonFiniteValue,
}

/// Structure for estimating the p-quantile of a population.
///
/// This is based on the P² algorithm for estimating quantiles using
/// constant space.
///
/// The algorithm consists of maintaining five markers: the
/// minimum, the p/2-, p-, and (1 + p)/2 quantiles, and the maximum.
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Quantile {
    /// The p value for the quantile.
    p: f64,
    /// The heights of the markers.
    marker_heights: [f64; FILLED_MARKER_LEN],
    /// The positions of the markers.
    ///
    /// We track sample size in the 5th position, as useful observations won't
    /// start until we've filled the heights at the 6th sample anyway
    /// This does deviate from the paper, but it's a more useful representation
    /// that works according to the paper's algorithm.
    marker_positions: [u64; FILLED_MARKER_LEN],
    /// The desired marker positions.
    desired_marker_positions: [f64; FILLED_MARKER_LEN],
}

impl Quantile {
    /// Create a new `Quantile` instance.
    ///
    /// Returns a result containing the `Quantile` instance or an error.
    ///
    /// # Errors
    ///
    /// Returns [`QuantileError::InvalidPValue`] if the p value is not in the
    /// range [0, 1].
    ///
    /// # Examples
    ///
    /// ```
    /// # // Rename the types crate so the doctests can refer to the public
    /// # // `oximeter` crate, not the private impl.
    /// # use oximeter_types as oximeter;
    /// use oximeter::Quantile;
    /// let q = Quantile::new(0.5).unwrap();
    ///
    /// assert_eq!(q.p(), 0.5);
    /// assert_eq!(q.len(), 0);
    /// ```
    pub fn new(p: f64) -> Result<Self, QuantileError> {
        if p < 0. || p > 1. {
            return Err(QuantileError::InvalidPValue);
        }

        Ok(Self {
            p,
            marker_heights: [0.; FILLED_MARKER_LEN],
            // We start with a sample size of 0.
            marker_positions: [1, 2, 3, 4, 0],
            // 1-indexed, which is like the paper, but
            // used to keep track of the sample size without
            // needing to do a separate count, use a Vec,
            // or do any other kind of bookkeeping.
            desired_marker_positions: [
                1.,
                1. + 2. * p,
                1. + 4. * p,
                3. + 2. * p,
                5.,
            ],
        })
    }

    /// Create a new `Quantile` instance from the given a p-value, marker
    /// heights and positions.
    ///
    /// # Examples
    /// ```
    /// # // Rename the types crate so the doctests can refer to the public
    /// # // `oximeter` crate, not the private impl.
    /// # use oximeter_types as oximeter;
    /// use oximeter::Quantile;
    /// let q = Quantile::from_parts(
    ///    0.5,
    ///    [0., 1., 2., 3., 4.],
    ///    [1, 2, 3, 4, 5],
    ///    [1., 3., 5., 7., 9.],
    /// );
    /// ```
    pub fn from_parts(
        p: f64,
        marker_heights: [f64; FILLED_MARKER_LEN],
        marker_positions: [u64; FILLED_MARKER_LEN],
        desired_marker_positions: [f64; FILLED_MARKER_LEN],
    ) -> Self {
        Self { p, marker_heights, marker_positions, desired_marker_positions }
    }

    /// Construct a `Quantile` instance for the 50th/median percentile.
    pub fn p50() -> Self {
        Self::new(0.5).unwrap()
    }

    /// Construct a `Quantile` instance for the 90th percentile.
    pub fn p90() -> Self {
        Self::new(0.9).unwrap()
    }

    /// Construct a `Quantile` instance for the 95th percentile.
    pub fn p95() -> Self {
        Self::new(0.95).unwrap()
    }

    /// Construct a `Quantile` instance for the 99th percentile.
    pub fn p99() -> Self {
        Self::new(0.99).unwrap()
    }

    /// Get the p value as a float.
    pub fn p(&self) -> f64 {
        self.p
    }

    /// Return the sample size.
    pub fn len(&self) -> u64 {
        self.marker_positions[4]
    }

    /// Determine if the number of samples in the population are empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the marker heights.
    pub fn marker_heights(&self) -> [f64; FILLED_MARKER_LEN] {
        self.marker_heights
    }

    /// Return the marker positions.
    pub fn marker_positions(&self) -> [u64; FILLED_MARKER_LEN] {
        self.marker_positions
    }

    /// Return the desired marker positions.
    pub fn desired_marker_positions(&self) -> [f64; FILLED_MARKER_LEN] {
        self.desired_marker_positions
    }

    /// Estimate the p-quantile of the population.
    ///
    /// This is step B.4 in the P² algorithm.
    ///
    /// Returns a result containing the estimated p-quantile or an error.
    ///
    /// # Errors
    ///
    /// Returns [`QuantileError::InsufficientSampleSize`] if the sample size
    /// is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # // Rename the types crate so the doctests can refer to the public
    /// # // `oximeter` crate, not the private impl.
    /// # use oximeter_types as oximeter;
    /// use oximeter::Quantile;
    /// let mut q = Quantile::new(0.5).unwrap();
    /// for o in 1..=100 {
    ///    q.append(o).unwrap();
    /// }
    /// assert_eq!(q.estimate().unwrap(), 50.0);
    /// ```
    pub fn estimate(&self) -> Result<f64, QuantileError> {
        if self.is_empty() {
            return Err(QuantileError::InsufficientSampleSize);
        }

        if self.len() >= FILLED_MARKER_LEN as u64 {
            return Ok(self.marker_heights[2]);
        }

        // Try to find an index in heights that is correlated with the p value
        // when we have less than 5 samples, but more than 0.
        let mut heights = self.marker_heights;
        float_ord::sort(&mut heights);
        let idx = (heights.len() as f64 - 1.) * self.p();
        return Ok(heights[idx.round() as usize]);
    }

    /// Append a value/observation to the population and adjust the heights.
    ///
    /// This comprises steps B.1, B.2, B.3 (adjust heights) in the P² algorithm,
    /// including finding the cell k containing the input value and updating the
    /// current and desired marker positions.
    ///
    /// Returns an empty result or an error.
    ///
    /// # Errors
    ///
    /// Returns [`QuantileError::NonFiniteValue`] if the value is not finite
    /// when casting to a float.
    ///
    /// # Examples
    ///
    /// ```
    /// # // Rename the types crate so the doctests can refer to the public
    /// # // `oximeter` crate, not the private impl.
    /// # use oximeter_types as oximeter;
    /// use oximeter::Quantile;
    /// let mut q = Quantile::new(0.9).unwrap();
    /// q.append(10).unwrap();
    /// assert_eq!(q.len(), 1);
    /// ```
    pub fn append<T>(&mut self, value: T) -> Result<(), QuantileError>
    where
        T: HistogramSupport,
    {
        if !value.is_finite() {
            return Err(QuantileError::NonFiniteValue);
        }
        // We've already checked that the value is finite.
        let value_f = value.to_f64().unwrap();

        if self.len() < FILLED_MARKER_LEN as u64 {
            self.marker_heights[self.len() as usize] = value_f;
            self.marker_positions[4] += 1;
            if self.len() == FILLED_MARKER_LEN as u64 {
                float_ord::sort(&mut self.marker_heights);
                self.adaptive_init();
            }
            return Ok(());
        }

        // Find the cell k containing the new value.
        let k = match self.find_cell(value_f) {
            Some(4) => {
                self.marker_heights[4] = value_f;
                3
            }
            Some(i) => i,
            None => {
                self.marker_heights[0] = value_f;
                0
            }
        };

        // Handle rounding issues as described in
        // <https://aakinshin.net/posts/p2-quantile-estimator-rounding-issue>.
        let count = self.len() as f64;
        self.desired_marker_positions[1] = count * (self.p() / 2.) + 1.;
        self.desired_marker_positions[2] = count * self.p() + 1.;
        self.desired_marker_positions[3] = count * ((1. + self.p()) / 2.) + 1.;
        self.desired_marker_positions[4] = count + 1.;

        for i in k + 1..FILLED_MARKER_LEN {
            self.marker_positions[i] += 1;
        }

        // Adjust height of markers adaptively to be more optimal for
        // not just higher quantiles, but also lower ones.
        //
        // This is a deviation from the paper, taken from
        // <https://aakinshin.net/posts/p2-quantile-estimator-adjusting-order>.
        if self.p >= 0.5 {
            for i in 1..4 {
                self.adjust_heights(i)
            }
        } else {
            for i in (1..4).rev() {
                self.adjust_heights(i)
            }
        }

        Ok(())
    }

    /// Find the higher marker cell whose height is lower than the observation.
    ///
    /// Returns `None` if the value is less than the initial marker height.
    fn find_cell(&mut self, value: f64) -> Option<usize> {
        if value < self.marker_heights[0] {
            None
        } else {
            Some(
                self.marker_heights
                    .partition_point(|&height| height <= value)
                    .saturating_sub(1),
            )
        }
    }

    /// Adjust the heights of the markers if necessary.
    ///
    /// Step B.3 in the P² algorithm. Should be used within a loop
    /// after appending a value to the population.
    fn adjust_heights(&mut self, i: usize) {
        let d =
            self.desired_marker_positions[i] - self.marker_positions[i] as f64;

        if (d >= 1.
            && self.marker_positions[i + 1] > self.marker_positions[i] + 1)
            || (d <= -1.
                && self.marker_positions[i - 1] < self.marker_positions[i] - 1)
        {
            let d_signum = d.signum();
            let q_prime = self.parabolic(i, d_signum);
            if self.marker_heights[i - 1] < q_prime
                && q_prime < self.marker_heights[i + 1]
            {
                self.marker_heights[i] = q_prime;
            } else {
                let q_prime = self.linear(i, d_signum);
                self.marker_heights[i] = q_prime;
            }

            // Update marker positions based on the sign of d.
            if d_signum < 0. {
                self.marker_positions[i] -= 1;
            } else {
                self.marker_positions[i] += 1;
            }
        }
    }

    /// An implementation to adaptively initialize the marker heights and
    /// positions, particularly useful for extreme quantiles (e.g., 0.99)
    /// when estimating on a small sample size.
    ///
    /// Read <https://aakinshin.net/posts/p2-quantile-estimator-initialization>
    /// for more.
    fn adaptive_init(&mut self) {
        self.desired_marker_positions[..FILLED_MARKER_LEN]
            .copy_from_slice(&self.marker_heights[..FILLED_MARKER_LEN]);

        self.marker_positions[1] = (1. + 2. * self.p()).round() as u64;
        self.marker_positions[2] = (1. + 4. * self.p()).round() as u64;
        self.marker_positions[3] = (3. + 2. * self.p()).round() as u64;
        self.marker_heights[1] = self.desired_marker_positions
            [self.marker_positions[1] as usize - 1];
        self.marker_heights[2] = self.desired_marker_positions
            [self.marker_positions[2] as usize - 1];
        self.marker_heights[3] = self.desired_marker_positions
            [self.marker_positions[3] as usize - 1];
    }

    /// Parabolic prediction for marker height.
    fn parabolic(&self, i: usize, d_signum: f64) -> f64 {
        let pos_diff1 = (self.marker_positions[i + 1] as i64
            - self.marker_positions[i - 1] as i64)
            as f64;

        let pos_diff2 = (self.marker_positions[i + 1] as i64
            - self.marker_positions[i] as i64) as f64;

        let pos_diff3 = (self.marker_positions[i] as i64
            - self.marker_positions[i - 1] as i64)
            as f64;

        let term1 = d_signum / pos_diff1;
        let term2 = ((self.marker_positions[i] - self.marker_positions[i - 1])
            as f64
            + d_signum)
            * (self.marker_heights[i + 1] - self.marker_heights[i])
            / pos_diff2;
        let term3 = ((self.marker_positions[i + 1] - self.marker_positions[i])
            as f64
            - d_signum)
            * (self.marker_heights[i] - self.marker_heights[i - 1])
            / pos_diff3;

        self.marker_heights[i] + term1 * (term2 + term3)
    }

    /// Linear prediction for marker height.
    fn linear(&self, i: usize, d_signum: f64) -> f64 {
        let idx = if d_signum < 0. { i - 1 } else { i + 1 };
        self.marker_heights[i]
            + d_signum * (self.marker_heights[idx] - self.marker_heights[i])
                / (self.marker_positions[idx] as i64
                    - self.marker_positions[i] as i64) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;
    use rand::{Rng, SeedableRng};

    fn test_quantile_impl(
        p: f64,
        observations: u64,
        assert_on: Option<f64>,
    ) -> Quantile {
        let mut q = Quantile::new(p).unwrap();
        for o in 1..=observations {
            q.append(o).unwrap();
        }
        assert_eq!(q.p(), p);
        assert_eq!(q.estimate().unwrap(), assert_on.unwrap_or(p * 100.));
        q
    }

    #[test]
    fn test_min_p() {
        let observations = [3, 6, 7, 8, 8, 10, 13, 15, 16, 20];

        let mut q = Quantile::new(0.0).unwrap();
        //assert_eq!(q.p(), 0.1);
        for &o in observations.iter() {
            q.append(o).unwrap();
        }
        assert_eq!(q.estimate().unwrap(), 3.);
    }

    /// Compared with C# implementation of P² algorithm.
    #[test]
    fn test_max_p() {
        let observations = [3, 6, 7, 8, 8, 10, 13, 15, 16, 20];

        let mut q = Quantile::new(1.).unwrap();
        assert_eq!(q.p(), 1.);

        for &o in observations.iter() {
            q.append(o).unwrap();
        }

        assert_eq!(q.estimate().unwrap(), 11.66543209876543);
    }

    /// Example observations from the P² paper.
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
        assert_eq!(q.marker_positions, [1, 6, 10, 16, 20]);
        assert_eq!(q.desired_marker_positions, [0.02, 5.75, 10.5, 15.25, 20.0]);
        assert_eq!(q.p(), 0.5);
        assert_eq!(q.len(), 20);
        assert_relative_eq!(q.estimate().unwrap(), 4.2462394088036435,);
    }

    #[test]
    fn test_rounding() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut estimator = Quantile::new(0.6).unwrap();

        for _ in 0..100 {
            let x: f64 = rng.random();
            estimator.append(x).unwrap();
        }

        assert_relative_eq!(
            estimator.estimate().unwrap(),
            0.552428024067269,
            epsilon = f64::EPSILON
        );
    }

    #[test]
    fn test_integer_observations() {
        let observations = 1..=100;
        let mut q = Quantile::new(0.3).unwrap();
        for o in observations {
            q.append(o).unwrap();
        }
        assert_eq!(q.marker_positions, [1, 15, 30, 65, 100]);
        assert_eq!(
            q.desired_marker_positions,
            [1.0, 15.85, 30.7, 65.35000000000001, 100.0]
        );

        assert_eq!(q.p(), 0.3);
        assert_eq!(q.estimate().unwrap(), 30.0);
    }

    #[test]
    fn test_empty_observations() {
        let q = Quantile::p50();
        assert_eq!(
            q.estimate().err().unwrap(),
            QuantileError::InsufficientSampleSize
        );
    }

    #[test]
    fn test_non_filled_observations() {
        let mut q = Quantile::p99();
        let observations = [-10., 0., 1., 10.];
        for &o in observations.iter() {
            q.append(o).unwrap();
        }
        assert_eq!(q.estimate().unwrap(), 10.);
    }

    #[test]
    fn test_default_percentiles() {
        test_quantile_impl(0.5, 100, None);
        test_quantile_impl(0.9, 100, None);
        test_quantile_impl(0.95, 100, None);
        test_quantile_impl(0.99, 100, Some(97.));
    }

    #[test]
    fn test_invalid_p_value() {
        assert_eq!(
            Quantile::new(1.01).err().unwrap(),
            QuantileError::InvalidPValue
        );
        assert_eq!(
            Quantile::new(f64::MAX).err().unwrap(),
            QuantileError::InvalidPValue
        );
    }

    #[test]
    fn test_find_cells() {
        let mut q = test_quantile_impl(0.5, 5, Some(3.));
        assert_eq!(q.find_cell(0.), None);
        assert_eq!(q.find_cell(7.), Some(4));
        assert_eq!(q.find_cell(4.), Some(3));
        assert_eq!(q.find_cell(3.5), Some(2));
    }
}
