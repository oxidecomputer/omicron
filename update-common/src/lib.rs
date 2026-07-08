// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common update types and code shared between wicketd and Nexus.
//!
//! NOTE: This crate is soft-frozen. It represents the path that Wicket and
//! Nexus used to take to load Tufaceous v1 repositories, and remains in use in
//! `cargo xtask releng` to verify generated repositories can still be read by
//! this code. When we stop generating v1 repositories, this crate should be
//! deleted.

pub mod artifacts;
pub mod errors;
mod tuf_repo;
