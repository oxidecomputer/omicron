//! Tools for generating and collecting metric data in the Oxide rack.
// Copyright 2021 Oxide Computer Company

pub use oximeter_macro_impl::*;

// Export the current crate as `oximeter`. The macros defined in `oximeter-macro-impl` generate
// code referring to symbols like `oximeter::traits::Target`. In consumers of this crate, that's
// fine, but internally there _is_ no crate named `oximeter`, it's just `self` or `crate`.
//
// See https://github.com/rust-lang/rust/pull/55275 for the PR introducing this fix, which links to
// lots of related issues and discussion.
extern crate self as oximeter;

pub mod histogram;
pub mod traits;
pub mod types;

pub use traits::{Metric, Producer, Target};
pub use types::{Error, FieldType, FieldValue, MeasurementType};
