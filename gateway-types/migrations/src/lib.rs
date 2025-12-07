// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Type migrations for the Gateway API.
//!
//! This crate contains all published types for the Gateway API, organized by
//! the first API version in which they were introduced. Types that change
//! between versions are defined in the version module where they changed.

pub mod v1;
pub mod v2;
