// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `STOP_TIMEOUT_SECS` of the Nexus external API.
//!
//! This swaps the `shutdown_policy` on `Instance` and related types with a
//! `stop_timeout_secs` which is a single setting for generated clients rather
//! than a pair of "enum variant" and "maybe a timeout if the variant has one".

pub mod instance;
