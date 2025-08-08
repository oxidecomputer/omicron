// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of Finite Field GF(2^8) aka GF(256)
//!
//! We use the Rijndael (AES) polynomial (x^8 + x^4 + x^3 + x + 1) as our
//! irreducible polynomial.
//!
//! We only implement enough operations so that we can implement
//! shamir secret sharing. This keeps the surface area small and
//! minimizes the amount of auditing we need to do.
//!
//! For a basic overview of galois fields, the docs from the
//! [gf256 crate](https://docs.rs/gf256/0.3.0/gf256/gf/index.html)
//! are excellent.
//!
//! For a more comprehensive introduction to the math, these
//! [lecture notes](https://web.stanford.edu/~marykw/classes/CS250_W19/readings/Forney_Introduction_to_Finite_Fields.pdf)
//! are handy.

// Don't tell me what operations to use in my implementations
#![expect(clippy::suspicious_arithmetic_impl)]

use core::fmt::{self, Binary, Display, Formatter, LowerHex, UpperHex};
use core::ops::{Add, AddAssign, Div, Mul, MulAssign, Sub};
use rand09::Rng;
use rand09::distr::{Distribution, StandardUniform};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use zeroize::Zeroize;

/// An element in a finite field of prime power 2^8
///
/// We explicitly don't derive the equality operators to prevent ourselves from
/// accidentally using those instead of the constant time ones.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, Zeroize, Serialize, Deserialize)]
pub struct Gf256(u8);

impl AsRef<u8> for Gf256 {
    fn as_ref(&self) -> &u8 {
        &self.0
    }
}

impl Gf256 {
    pub fn new(n: u8) -> Gf256 {
        Gf256(n)
    }

    /// Return the underlying u8.
    pub fn into_u8(self) -> u8 {
        self.0
    }

    /// Return the multiplicative inverse (`self^-1`) of self
    ///
    /// ```text
    /// self * self^-1 = 1
    /// ```
    ///
    /// By Fermat's little theorem: `self^-1 = self^254 for GF(2^8)`.
    /// We calculate `self^254` in a simple, unrolled fashion.
    ///
    /// This strategy was borrowed from <https://github.com/dsprenkels/sss/blob/16c3fdb175497b25eb90b966991fa7ff19fbdcfe/hazmat.c#L247-L266>
    #[rustfmt::skip]
    pub fn invert(&self) -> Gf256 {
        let mut result = *self * *self;     // self^2
        let temp_4 = result * result;       // self^4
        result = temp_4 * temp_4;           // self^8
        let temp_9 = result * *self;        // self^9
        result *= result;                   // self^16;
        result *= temp_9;                   // self^25;
        let temp_50 = result * result;      // self^50;
        result = temp_50 * temp_50;         // self^100;
        result *= result;                   // self^200;
        result = result * temp_50 * temp_4; // self^254

        result
    }
}

pub const ZERO: Gf256 = Gf256(0);
pub const ONE: Gf256 = Gf256(1);

impl Display for Gf256 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl LowerHex for Gf256 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:02x}", self.0)
    }
}

impl UpperHex for Gf256 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:02X}", self.0)
    }
}

impl Binary for Gf256 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:08b}", self.0)
    }
}

impl Distribution<Gf256> for StandardUniform {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Gf256 {
        Gf256(rng.random())
    }
}

impl ConstantTimeEq for Gf256 {
    fn ct_eq(&self, other: &Self) -> subtle::Choice {
        self.0.ct_eq(&other.0)
    }
}

#[cfg(feature = "danger_partial_eq_ct_wrapper")]
impl PartialEq for Gf256 {
    fn eq(&self, other: &Self) -> bool {
        self.ct_eq(&other).into()
    }
}
#[cfg(feature = "danger_partial_eq_ct_wrapper")]
impl Eq for Gf256 {}

impl Add for Gf256 {
    type Output = Self;

    /// This is a fundamental operation.
    ///
    /// In `GF(2^n)`, we always do carryless addition `mod 2` which ends up
    /// being exactly equal to bitwise xor.
    fn add(self, rhs: Self) -> Self::Output {
        Gf256(self.0 ^ rhs.0)
    }
}

impl Add<&Gf256> for Gf256 {
    type Output = Self;
    fn add(self, rhs: &Gf256) -> Self::Output {
        self + *rhs
    }
}

impl AddAssign for Gf256 {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs
    }
}

impl MulAssign for Gf256 {
    fn mul_assign(&mut self, rhs: Self) {
        *self = *self * rhs
    }
}

impl Sub for Gf256 {
    type Output = Self;

    /// Subtraction is identical to addition in `GF(2^n)`
    fn sub(self, rhs: Self) -> Self {
        Gf256(self.0 ^ rhs.0)
    }
}

impl Div for Gf256 {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        self * rhs.invert()
    }
}

/// This is an efficient carryless multiplication + reduction algorithm.
///
/// It is essentially long multiplication of polynomial elements in GF(2^8)
/// represented as unsigned bytes (u8). For every bit set in `rhs` we multiply
/// our original value by `x^i` where i is the bit position of `rhs` and `x` is
/// our placeholder variable for GF(2^8) operations, and then add the result to
/// an accumulator initialized to zero. If our accumulator exceeds 255, meaning
/// it is no longer inside the finite field, we reduce it modulo our irreducible
/// (prime) polynomial:
///
/// ```text
/// m(x) = x^8 + x^4 + x^3 + x + 1
/// ```
///
/// As an example, with our accumulator named `product`:
///
/// ```text
/// product = 0
/// self = 0x21 = 0b0010_0001 = x^5 + 1
/// rhs  = 0x12 = 0b0001_0010 = x^4 + x
///
/// step 1 = ((x^5 + 1) * x) mod m(x)
///        = x^6 + x
///
/// product += step1
///
/// step 2 = ((x^5 + 1) * x^4) mod m(x)
///        = (x^9 + x^4) mod (x^8 + x^4 + x^3 +x + 1)
///        = x^5 + x^2 + x
///
/// product += step2
///         = x^6 + x + x^5 + x^2 + x
///         = x^6 + x^5 + x^2
///         = 0b0110_0100
///         = 0x64
/// ```
///
/// See the `test_docs_example` unit test at the bottom of this file to confirm
/// this math.
///
/// In this algorithm, we use the value `0x1b`, which is our irreducible
/// polynomial without the high term. We do this because of the following
/// equality in GF(2^8), which follows from long-division of polynomials:
///
/// ```text
/// `x^8 mod m(x) = x^4 + x^3 + x + 1 = 0x1b`
/// ```
///
/// A rationale and description of this algorithm can be found in sections 7.9
/// and 7.10 of the lecture notes at
/// <https://engineering.purdue.edu/kak/compsec/NewLectures/Lecture7.pdf>
///
/// It is also explained on a wikipedia section about the
/// [AES finite field](https://en.wikipedia.org/wiki/Finite_field_arithmetic#Rijndael's_(AES)_finite_field)
///
/// This is roughly the same algorithm used in
/// [vsss-rs](https://github.com/mikelodder7/vsss-rs) and many other existing
/// implementations.
impl Mul for Gf256 {
    type Output = Self;
    fn mul(mut self, mut rhs: Self) -> Self::Output {
        let mut product = 0u8;
        for _ in 0..8 {
            // If the LSB of rhs is 1, then add `self` to `product`.
            product ^= 0x0u8.wrapping_sub(rhs.0 & 1) & self.0;

            // Divide rhs polynomial by `x`.
            // Note that we just utilized this bit we right shifted away in the
            // previous line.
            rhs.0 >>= 1;

            // Track if the high bit is currently set in `self`. If it is, then
            // we have an `x^7` term, and multiplying by `x` will require a
            // modulo reduction to stay within our field.
            let carry: u8 = self.0 >> 7;

            // Shift `self` left a bit.
            // This multiplies `self` by `x`.
            self.0 <<= 1;

            // If there was a carry, then we need to add `x^8 mod m(x)`, which
            // equals `0x1b`, to `self`.
            self.0 ^= 0x0u8.wrapping_sub(carry) & 0x1b;

            // Note that we haven't actually added `self` to `product` yet, as
            // we don't know if the corresponding bit in `rhs` was set.
            // That happens the next time through the loop.
        }

        Gf256(product)
    }
}

impl Mul<&Gf256> for Gf256 {
    type Output = Gf256;
    fn mul(self, rhs: &Self) -> Self::Output {
        self * *rhs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_multiplication() {
        // This file contains the output of all products of (0..255)x(0..255)
        // as produced by a snapshot of vsss_rs version 5.10 and validated via
        // gf256 version 0.3.0.
        let products_tabbed: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_input/gf256_multiplication_test_vector.txt"
        ));

        let mut products = products_tabbed.split("\t");
        for i in 0..=255 {
            for j in 0..=255 {
                let a = Gf256(i);
                let b = Gf256(j);
                let c = a * b;
                assert_eq!(
                    c.0,
                    products.next().unwrap().parse::<u8>().unwrap()
                );
            }
        }
    }

    #[test]
    fn test_all_multiplicative_inverses() {
        // Can't divide by zero
        for i in 1..=255u8 {
            let a = Gf256::new(i);
            assert_eq!((a * a.invert()).ct_eq(&ONE).unwrap_u8(), 1);

            // Division is the same as multiplying by the inverse
            assert_eq!((a / a).ct_eq(&ONE).unwrap_u8(), 1);
        }
    }

    #[test]
    fn test_docs_example() {
        let a = Gf256::new(0x21);
        let b = Gf256::new(0x12);

        let expected = Gf256::new(0x64);
        let product = a * b;

        println!("product = {:#x?}", product.0);

        assert_eq!(expected.ct_eq(&product).unwrap_u8(), 1);
    }
}
