// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shamir secret sharing over GF(2^8)

use rand::{Rng, rngs::OsRng};
use secrecy::Secret;
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

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum CombineError {
    #[error("must be at least 2 shares to combine")]
    TooFewShares,
    #[error("all shares must have distinct x-coordinates")]
    DuplicateXCoordinates,
    #[error("all shares must be of equivalent length")]
    InvalidShareLengths,
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

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct Share {
    pub x_coordinate: Gf256,
    pub y_coordinates: Vec<Gf256>,
}

pub struct SecretShares {
    pub threshold: ValidThreshold,
    pub shares: Secret<Vec<Share>>,
}

/// Split a secret into `n` key shares with a threshold of `k` shares.
pub fn split_secret(
    secret: &[u8],
    n: u8,
    k: u8,
) -> Result<SecretShares, SplitError> {
    // We hardcode this for security.
    let mut rng = OsRng;
    split_secret_inner(secret, n, k, &mut rng)
}

/// An internal method that allows us to use a different RNG during testing
fn split_secret_inner<R: Rng>(
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
            let y_coordinates =
                polynomials.iter().map(|p| p.eval(x_coordinate)).collect();
            Share { x_coordinate, y_coordinates }
        })
        .collect();

    Ok(SecretShares { threshold, shares: Secret::new(shares) })
}

/// Combine the shares to reconstruct the secret
///
/// The secret is the concatenation of the y-coordinates at x=0.
pub fn combine_shares(
    shares: &[Share],
) -> Result<Secret<Vec<u8>>, CombineError> {
    let shares = ValidShares::new(shares)?;
    let share = interpolate_polynomials(shares, gf256::ZERO);
    Ok(Secret::new(share.y_coordinates.iter().map(|y| y.into_u8()).collect()))
}

/// Interpolate the points for each polynomial to find the value `y = f(x)`
/// and then concatenate them and return the corrseponding [`Share`].
///
/// Calling this function for `x=0` reveals the secret.
pub fn interpolate_polynomials(shares: ValidShares, x: Gf256) -> Share {
    // Our output value: `f(x)` for all polynomials
    let mut output = vec![gf256::ZERO; shares.num_y_coordinates()];

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
    use super::*;
    use proptest::collection::size_range;
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
    }

    // TODO: pass in the proptest rng to `split_secret`
    #[proptest]
    fn split_and_combine(input: TestInput) {
        match split_secret(&input.secret, input.total_shares, input.threshold) {
            Ok(shares) => {
                // Combining at least k shares succeeds and returns our secret
                let n = input.threshold as usize;
                let k = shares.threshold.0 as usize;
                let secret =
                    combine_shares(&shares.shares.expose_secret()[0..k])
                        .expect("combining succeeds");
                assert_eq!(*secret.expose_secret(), input.secret);
                let secret =
                    combine_shares(&shares.shares.expose_secret()[n - k..])
                        .expect("combining succeeds");
                assert_eq!(*secret.expose_secret(), input.secret);

                if k > 2 {
                    // Combining fewer than k shares returns nonsense
                    let secret = combine_shares(
                        &shares.shares.expose_secret()[0..k - 1],
                    )
                    .expect("combining succeeds");
                    assert_ne!(*secret.expose_secret(), input.secret);
                } else {
                    // Attempting to combine too few shares fails
                    assert!(
                        combine_shares(
                            &shares.shares.expose_secret()[0..k - 1]
                        )
                        .is_err()
                    );
                }

                // Trying to combine shares with a mismatched number
                // of y-coordinates fails
                let mut copy: Vec<_> =
                    shares.shares.expose_secret().iter().cloned().collect();
                let z = copy.get_mut(0).unwrap().y_coordinates.pop().unwrap();
                assert!(combine_shares(&copy).is_err());

                // Put back the popped share, then duplicate an x-coordinate
                // This should also cause an error
                let first_share = copy.get_mut(0).unwrap();
                first_share.y_coordinates.push(z);
                // The first share is at x-coordinate 1
                first_share.x_coordinate = Gf256::new(2);
                assert!(combine_shares(&copy).is_err());
            }
            Err(SplitError::ThresholdToSmall) => {
                assert!(input.threshold < 2);
            }
            Err(SplitError::TooFewTotalShares { n, k }) => {
                assert!(k > n);
            }
        }
    }
}
