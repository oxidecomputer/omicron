// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shamir secret sharing over GF(2^8)
//!
//! We use the Rijndael polynomial (x^8 + x^4 + x^3 + x + 1) as our
//! irreducible polynomial.

pub mod gf256;
pub mod polynomial;
pub mod shamir;
