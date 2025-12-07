// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Type migrations for the DNS server API.
//!
//! This crate contains versioned types for the DNS server API. Types are
//! organized by the API version they were introduced in, following the
//! principles outlined in [RFD 619](https://rfd.shared.oxide.computer/rfd/0619).

pub mod v1;
pub mod v2;
