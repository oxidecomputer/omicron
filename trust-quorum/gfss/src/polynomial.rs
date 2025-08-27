// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generated polynomials over GF(2^8) used for secret splitting

use rand::{Rng, distr};
use std::fmt::Display;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::gf256::{self, Gf256};
use crate::shamir::ValidThreshold;

/// A polynomial of degree `k-1` with random coefficients in `GF(256)` for
/// non-constant terms, and a seeded constant term.
///
/// The constant term is one byte of our secret. A separate polynomial is
/// constucted for each byte of the secret.
///
/// We only store the coefficients. The coeffecients are pushed from low (`0`) to
/// high (`k-1`). We include all coefficients, even zero ones, for a few reasons:
///   1. So that we don't also have to track exponents, as that is implicit in
///      the length of the inner boxed slice.
///   2. To allow direct indexing if desired.
///   3. To not provide any data dependent performance differences in
///      construction. The degree of the polynomial can be considered public,
///      since each share holder must know how many shares it needs to
///      reconstruct the secret. However, only pushing non-zero values could
///      allow an attacker to infer the values of those coefficients or that
///      some coefficients are zero.
///
/// The highest degree coefficient must not be zero, or else the degree of the
/// polynomial would not be `k-1`, and fewer than `k` shares would be able to
/// reconstruct the polynomial. We therefore ensure that the coefficient of
/// the `x^(k-1)` term is nonzero, while allowing a value of zero for all other
/// terms.
#[derive(Debug, Zeroize, ZeroizeOnDrop)]
pub struct Polynomial(Box<[Gf256]>);

impl Polynomial {
    pub fn random_with_constant_term<R: Rng + Sized>(
        rng: &mut R,
        k: ValidThreshold,
        constant_term: Gf256,
    ) -> Polynomial {
        let degree = (k.inner() - 1) as usize;
        // We need to store the 0th term which is a constant, so we add back 1
        // to the degree.
        let mut inner: Box<[Gf256]> =
            rng.sample_iter(distr::StandardUniform).take(degree + 1).collect();

        // Overwrite the constant term
        inner[0] = constant_term;

        // Ensure the highest term coefficient is nonzero.
        //
        // This is not a sensitive operation. It is public information that this
        // term must not be zero, and so looping for as long as it takes to get
        // a nonzero byte doesn't provide the attacker with any new information.
        //
        // However, we still want our comparison to be constant time so we use
        // `ct_eq`.
        while inner[degree].ct_eq(&gf256::ZERO).into() {
            inner[degree] = rng.random();
        }

        Polynomial(inner)
    }

    /// Evaluate the polynomial function at the given `x` coordinate
    /// and return the resulting `y` coordinate.
    ///
    /// This is an O(n) version using Horner's method
    pub fn eval(&self, x: Gf256) -> Gf256 {
        let mut iter = self.0.iter().rev();
        let mut y = *(iter.next().unwrap());
        for coefficient in iter {
            y = y * x + coefficient
        }
        y
    }
}

impl Display for Polynomial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // We don't print the plus sign the first time through, and we only
        // print it when we know there is a nonzero coefficient.
        let mut print_plus_sign = false;
        for (exponent, coefficient) in self.0.iter().enumerate().rev() {
            if coefficient.ct_eq(&gf256::ZERO).into() {
                continue;
            }
            if print_plus_sign {
                write!(f, " + ")?;
            }
            if exponent == 0 {
                write!(f, "{coefficient}")?;
            } else if exponent == 1 {
                write!(f, "{coefficient}x")?;
            } else {
                write!(f, "{coefficient}x^{exponent}")?;
                print_plus_sign = true;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gf256::Gf256;
    use crate::test_utils::test_rng_strategy;
    use proptest::test_runner::TestRng;
    use test_strategy::{Arbitrary, proptest};

    impl Polynomial {
        /// Evaluate the polynomial function at the given `x` coordinate
        /// and return the resulting `y` coordinate.
        ///
        /// This is the `0(n^2)` naive method used for testing.
        fn naive_eval(&self, x: Gf256) -> Gf256 {
            let mut y = Gf256::new(0);
            for (exponent, coefficient) in self.0.iter().enumerate() {
                // Safety: We only construct a `Polynomial` by passing in a
                // `ValidThreshold` which always fits in a u8 and is the number of
                // degrees of our polynomial + 1.
                //
                // We could call `try_into.unwrap()` but that adds instructions and
                // could potentially leave some unanticipated timing side effects
                // that could enable side channel analysis if our multiplication and
                // exponentiation is not as constant time as we think.
                y += *coefficient * pow(x, exponent as u8);
            }
            y
        }
    }

    /// Exponentiation via repeated multiplication
    ///
    /// This is a very slow, but clearly correct implementation. We use it to
    /// implement `naive_eval`, which is a model implementation used to check
    /// the correctness of the production "Horner's rule" implementation for
    /// polynomial evaluation.
    fn pow(base: Gf256, exponent: u8) -> Gf256 {
        let mut result = Gf256::new(1);
        for _ in 0..exponent as usize {
            result *= base;
        }
        result
    }

    #[derive(Arbitrary, Debug)]
    pub struct TestInput {
        #[strategy(0..255u8)]
        secret: u8,
        #[strategy(2..255u8)]
        threshold: u8,
        #[strategy(test_rng_strategy())]
        rng: TestRng,
    }

    #[proptest]
    fn test_polynomial_eval(mut input: TestInput) {
        let n = input.threshold;
        let p = Polynomial::random_with_constant_term(
            &mut input.rng,
            ValidThreshold::new(n, input.threshold).unwrap(),
            Gf256::new(input.secret),
        );
        for i in 0..255 {
            let horner = p.eval(Gf256::new(i));
            let naive = p.naive_eval(Gf256::new(i));
            assert_eq!(horner.ct_eq(&naive).unwrap_u8(), 1);
        }
    }
}
