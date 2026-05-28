// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The mechanism layer of support bundle collection.
//!
//! This crate provides the data-gathering primitives used to assemble a
//! support bundle. It is consumed by both the Nexus background task
//! (which manages bundle state in the `support_bundle` table) and by
//! `omdb` (which collects bundles ad-hoc, including when Nexus is down).
//!
//! See `README.md` in this crate for a developer-oriented overview of
//! the step framework.

mod cache;
pub mod collection;
pub mod perfetto;
mod step;
mod steps;
pub mod zip;

pub use collection::BundleCollection;
pub use collection::BundleInfo;
