// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Type migrations for the DNS server API.
//!
//! This crate contains versioned types for the DNS server API. Types are
//! organized by the API version they were introduced in, following the
//! principles outlined in RFD 619.
//!
//! ## Organization
//!
//! - `v1`: Types introduced in API version 1.0.0 (INITIAL)
//! - `v2`: Types introduced in API version 2.0.0 (SOA_AND_NS)
//!
//! Each version module contains types that were first introduced in that
//! version. Conversion code between versions lives in the later version's
//! module.

pub mod v1;
pub mod v2;
