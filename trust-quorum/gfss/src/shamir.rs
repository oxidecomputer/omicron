// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shamir secret sharing over GF(2^8)

use digest::Digest;
use rand::TryRngCore;
use rand::{Rng, rngs::OsRng};
use secrecy::SecretBox;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::gf256::{self, Gf256};
use crate::polynomial::Polynomial;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum SplitError {
    #[error("splitting requires at least a threshold of 2")]
    ThresholdToSmall,
    #[error("total shares {n} must be >= threshold {k}")]
    TooFewTotalShares { n: u8, k: u8 },
}

#[derive(
    Debug,
    Clone,
    thiserror::Error,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub enum CombineError {
    #[error("must be at least 2 shares to combine")]
    TooFewShares,
    #[error("all shares must have distinct x-coordinates")]
    DuplicateXCoordinates,
    #[error("all shares must be of equivalent length")]
    InvalidShareLengths,
    #[error("compute share cannot be called with x=0")]
    InvalidShareId,
}

/// A parsed type where the threshold has been validated to be greater than 2
/// and less than or equal to 255, and where it's guaranteed to be lower than
/// the total number of shares created via calls to `split_secret`.
#[derive(Debug, Clone, Copy)]
pub struct ValidThreshold(u8);

impl ValidThreshold {
    /// Create a threshold that is known to be valid
    ///
    /// `n` is the total number of shares, and `k` is the threshold
    pub fn new(n: u8, k: u8) -> Result<ValidThreshold, SplitError> {
        if k < 2 {
            return Err(SplitError::ThresholdToSmall);
        }
        if n < k {
            return Err(SplitError::TooFewTotalShares { n, k });
        }

        Ok(ValidThreshold(k))
    }

    /// Return the underlying threshold as a `u8`
    pub fn inner(&self) -> u8 {
        self.0
    }
}

/// A set of shares that have been validated
///
/// * There are at least 2 shares
/// * Each share has a unique x-coordinate
/// * All shares have an equal number of y-coordinates
pub struct ValidShares<'a>(&'a [Share]);

impl<'a> ValidShares<'a> {
    pub fn new(shares: &'a [Share]) -> Result<ValidShares<'a>, CombineError> {
        if shares.len() < 2 {
            return Err(CombineError::TooFewShares);
        }

        let len = shares[0].y_coordinates.len();
        for (i, share) in shares.iter().enumerate() {
            if share.y_coordinates.len() != len {
                return Err(CombineError::InvalidShareLengths);
            }
            // We only allow constant time comparison of coordinates, so we must
            // compare each share with the share that follows it.
            //
            // It might be cheaper to store the x-coordinate in a set and check
            // for duplicates on insert, but we can't do that because we don't
            // impl `Ord` or `Hash` on `Gf256`.
            for share2 in shares.iter().skip(i + 1) {
                if share.x_coordinate.ct_eq(&share2.x_coordinate).into() {
                    return Err(CombineError::DuplicateXCoordinates);
                }
            }
        }

        Ok(ValidShares(shares))
    }

    // Return the number of y-coordinates in each share
    //
    // This is the length of our secret.
    pub fn num_y_coordinates(&self) -> usize {
        self.0[0].y_coordinates.len()
    }
}

#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct Share {
    pub x_coordinate: Gf256,
    pub y_coordinates: Box<[Gf256]>,
}

impl Share {
    // Return a cryptographic hash of a Share using the parameterized
    // algorithm.
    pub fn digest<D: Digest>(&self, output: &mut [u8]) {
        let mut hasher = D::new();
        hasher.update([*self.x_coordinate.as_ref()]);
        // Implementing AsRef<[u8]> for Box<[Gf256]> doesn't work due to
        // coherence rules. To get around that we'd need a transparent newtype
        // for the y_coordinates and some unsafe code, which we're loathe to do.
        let mut ys: Vec<u8> =
            self.y_coordinates.iter().map(|y| *y.as_ref()).collect();
        hasher.update(&ys);
        output.copy_from_slice(&hasher.finalize());
        ys.zeroize();
    }
}

impl std::fmt::Debug for Share {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyShareGf256").finish()
    }
}

pub struct SecretShares {
    pub threshold: ValidThreshold,
    pub shares: SecretBox<Vec<Share>>,
}

/// Split a secret into `n` key shares with a threshold of `k` shares.
pub fn split_secret(
    secret: &[u8],
    n: u8,
    k: u8,
) -> Result<SecretShares, SplitError> {
    // We hardcode this for security.
    let mut rng = OsRng.unwrap_err();
    split_secret_impl(secret, n, k, &mut rng)
}

/// An internal method that allows us to use a different RNG during testing
fn split_secret_impl<R: Rng>(
    secret: &[u8],
    n: u8,
    k: u8,
    rng: &mut R,
) -> Result<SecretShares, SplitError> {
    let threshold = ValidThreshold::new(n, k)?;

    // Construct `secret.len()` polynomials of order `k-1`
    //
    // Each polynomial gives us one byte for each share and
    // we need a total of `secret.len()` bytes.
    let polynomials: Vec<_> = secret
        .iter()
        .map(|s| {
            Polynomial::random_with_constant_term(
                rng,
                threshold,
                Gf256::new(*s),
            )
        })
        .collect();

    // Evaluate the polynomials n times for each key share
    //
    // The polynomial evaluated at x=0 is our secret, so we always start
    // from x=1 when creating our shares.
    let shares: Vec<_> = (1..=n)
        .map(|i| {
            let x_coordinate = Gf256::new(i);
            let y_coordinates: Box<[Gf256]> =
                polynomials.iter().map(|p| p.eval(x_coordinate)).collect();
            Share { x_coordinate, y_coordinates }
        })
        .collect();

    Ok(SecretShares { threshold, shares: SecretBox::new(Box::new(shares)) })
}

/// Combine the shares to reconstruct the secret
///
/// The secret is the concatenation of the y-coordinates at `x=0`.
pub fn compute_secret(
    shares: &[Share],
) -> Result<SecretBox<[u8]>, CombineError> {
    let shares = ValidShares::new(shares)?;
    let share = interpolate_polynomials(shares, gf256::ZERO);
    let y_coordinates: Box<[u8]> =
        share.y_coordinates.iter().map(|y| y.into_u8()).collect();
    Ok(SecretBox::new(y_coordinates))
}

/// Combine the shares to compute an unknown share at the given x-coordinate.
///
/// Returns an error if `x=0`, as that is the secret. Callers
/// that need the secret should call `compute_secret`.
pub fn compute_share(
    shares: &[Share],
    x: Gf256,
) -> Result<Share, CombineError> {
    if x.ct_eq(&gf256::ZERO).into() {
        return Err(CombineError::InvalidShareId);
    }
    let shares = ValidShares::new(shares)?;
    Ok(interpolate_polynomials(shares, x))
}

/// Interpolate the points for each polynomial to find the value `y = f(x)`
/// and then concatenate them and return the corresponding [`Share`].
///
/// Calling this function for `x=0` reveals the secret.
fn interpolate_polynomials(shares: ValidShares, x: Gf256) -> Share {
    // Our output value: `f(x)` for all polynomials
    let mut output =
        vec![gf256::ZERO; shares.num_y_coordinates()].into_boxed_slice();

    // We enumerate so that we don't have to use constant time equality
    // comparing each x value. The order of the polynomials is public, so this
    // does not leak any information.
    for (i, share_i) in shares.0.iter().enumerate() {
        let Share { x_coordinate: x_i, y_coordinates } = share_i;
        // Compute the lagrange basis for x_i
        let mut li = gf256::ONE;
        for (j, share_j) in shares.0.iter().enumerate() {
            if i != j {
                li *=
                    (x - share_j.x_coordinate) / (*x_i - share_j.x_coordinate);
            }
        }

        // Multiply each lagrange basis times the y-coordinates and accumulate
        // the output.
        for (y, y_i) in output.iter_mut().zip(y_coordinates) {
            *y += li * y_i;
        }
    }

    Share { x_coordinate: x, y_coordinates: output }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;
    use crate::test_utils::test_rng_strategy;
    use proptest::{collection::size_range, test_runner::TestRng};
    use secrecy::ExposeSecret;
    use test_strategy::{Arbitrary, proptest};

    #[derive(Arbitrary, Debug)]
    pub struct TestInput {
        #[strategy(0..255u8)]
        threshold: u8,
        #[strategy(0..255u8)]
        total_shares: u8,
        #[any(size_range(2..64).lift())]
        secret: Vec<u8>,
        #[strategy(test_rng_strategy())]
        rng: TestRng,
    }

    #[proptest]
    fn split_and_combine(mut input: TestInput) {
        match split_secret_impl(
            &input.secret,
            input.total_shares,
            input.threshold,
            &mut input.rng,
        ) {
            Ok(shares) => {
                // Combining at least k shares succeeds and returns our secret
                let n = input.threshold as usize;
                let k = shares.threshold.0 as usize;
                let input_secret = input.secret.as_slice();
                let secret =
                    compute_secret(&shares.shares.expose_secret()[0..k])
                        .expect("combining succeeds");
                assert_eq!(secret.expose_secret(), input_secret);
                let secret =
                    compute_secret(&shares.shares.expose_secret()[n - k..])
                        .expect("combining succeeds");
                assert_eq!(secret.expose_secret(), input_secret);

                if k > 2 {
                    // Combining fewer than k shares returns nonsense
                    let secret = compute_secret(
                        &shares.shares.expose_secret()[0..k - 1],
                    )
                    .expect("combining succeeds");
                    assert_ne!(secret.expose_secret(), input_secret);
                } else {
                    // Attempting to combine too few shares fails
                    assert!(
                        compute_secret(
                            &shares.shares.expose_secret()[0..k - 1]
                        )
                        .is_err()
                    );
                }

                // Trying to combine shares with a mismatched number
                // of y-coordinates fails.
                //
                // We have to use mem::swap, because we can't change a boxed
                // slice's length
                let mut copy: Vec<_> = shares.shares.expose_secret().to_vec();
                let y_coordinates = &mut copy.get_mut(0).unwrap().y_coordinates;
                let len = y_coordinates.len();
                let mut y_coordinates_bad: Box<[Gf256]> =
                    y_coordinates.iter().cloned().take(len - 1).collect();
                mem::swap(&mut *y_coordinates, &mut y_coordinates_bad);
                assert!(compute_secret(&copy).is_err());

                // Duplicate an x-coordinate. This should also cause an error.
                let mut copy: Vec<_> = shares.shares.expose_secret().to_vec();
                let first_share = copy.get_mut(0).unwrap();
                // The first share is at x-coordinate 1
                first_share.x_coordinate = Gf256::new(2);
                assert!(compute_secret(&copy).is_err());
            }
            Err(SplitError::ThresholdToSmall) => {
                assert!(input.threshold < 2);
            }
            Err(SplitError::TooFewTotalShares { n, k }) => {
                assert!(k > n);
            }
        }
    }

    #[test]
    fn test_share_reconstruction() {
        let secret = b"some-secret";
        let shares = split_secret(&secret[..], 10, 7).unwrap();

        // Generate enough shares to recompute the secret
        let computed: Vec<_> = (20..27u8)
            .map(|x| {
                let shares = shares.shares.expose_secret();
                compute_share(&shares, Gf256::new(x)).unwrap()
            })
            .collect();

        // Recompute secret from computed shares
        let computed_secret = compute_secret(&computed).unwrap();

        assert_eq!(&secret[..], computed_secret.expose_secret());
    }
}
