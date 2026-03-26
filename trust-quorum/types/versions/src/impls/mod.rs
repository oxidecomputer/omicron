// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional implementations for trust quorum types.
//!
//! Per RFD 619, inherent methods, Display implementations, and other
//! functional code lives here, using `latest::` identifiers. However, pulling inherent methods on,
//! e.g. `Configuration` would end up transitively pulling in a lot of the rest of the trust quorum
//! protocol, which we *don't* want, so instead the inherent methods have been converted to free
//! functions when necessary.

mod epoch;
mod salt;
