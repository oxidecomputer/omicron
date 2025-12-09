// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Versioned types for the DNS server API.
//!
//! # Organization
//!
//! Types are organized based on the rules outlined in [RFD
//! 619](https://rfd.shared.oxide.computer/rfd/0619).

pub mod latest;
#[path = "initial/mod.rs"]
pub mod v1;
#[path = "soa_and_ns/mod.rs"]
pub mod v2;
