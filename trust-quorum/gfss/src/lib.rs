// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shamir secret sharing over GF(2^8)
//!
//! We use the Rijndael polynomial (x^8 + x^4 + x^3 + x + 1) as our
//! irreducible polynomial.
//!
//! Our particular use case for shamir secret sharing is for "rack unlock" of
//! trust quorum as described in RFD 238. This use case is somewhat unique in
//! that its goal is to prevent recovery of encrypted data at rest from casual
//! theft of a subset of U.2 drives and/or sleds in a rack. Our threat model
//! here is one of physical attack where a malicious actor can steal drives with
//! brief access to the rack. While not strictly out of scope, network attacks
//! are difficult for a number of reasons:
//!
//!  * Secret creation and share splitting happens on the sled-agent, which is
//!    not directly accessible to the outside world.
//!  * Secret shares are transferred over encrypted and attested channels on the
//!    `bootstrap` network which is not available outside the rack.
//!  * Share distribution and key rotation are both rare operations. They only
//!    occur when a rack or sled reboots or the trust quorum memberhsip changes.
//!  * Trust quorum membership and rack/sled reboot operations are only
//!    available to rack operators.
//!
//! For all of the above reasons, timing or or cache attacks on this
//! implementation are rather difficult and unlikely to succeed. There are
//! likely more fruitful venues of attack elsewehere. Nonetheless, this
//! implementation makes a best effort attempt to provide constant time
//! operations for a defense in depth strategy. All sensitive operations are
//! branchless and data dependent operations are not performed on secret data.
//! However, this implementation is in rust, and both compiler optimization
//! and architecture implementations may
//! [thwart our best efforts](https://eprint.iacr.org/2025/435).
//!
//! As we move towards Zen4+ architectures we may be able to limit our reliance
//! on best effort rust attempts via hardware operations like [GFNI](https://
//! en.wikipedia.org/wiki/ AVX-512#GFNI) for multiplication. Old hardware will
//! still rely on software implementations. Furthermore, our interpolation
//! algorithm is likely to remain in rust and not assembly for the time being.

pub mod gf256;
pub mod polynomial;
pub mod shamir;
