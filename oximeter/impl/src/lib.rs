// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

pub use oximeter_macro_impl::*;

// Export the current crate as `oximeter`. The macros defined in `oximeter-macro-impl` generate
// code referring to symbols like `oximeter::traits::Target`. In consumers of this crate, that's
// fine, but internally there _is_ no crate named `oximeter`, it's just `self` or `crate`.
//
// See https://github.com/rust-lang/rust/pull/55275 for the PR introducing this fix, which links to
// lots of related issues and discussion.
extern crate self as oximeter;

pub mod histogram;
pub mod quantile;
pub mod schema;
pub mod test_util;
pub mod traits;
pub mod types;

pub use quantile::Quantile;
pub use quantile::QuantileError;
pub use schema::FieldSchema;
pub use schema::TimeseriesName;
pub use schema::TimeseriesSchema;
pub use traits::Metric;
pub use traits::Producer;
pub use traits::Target;
pub use types::Datum;
pub use types::DatumType;
pub use types::Field;
pub use types::FieldType;
pub use types::FieldValue;
pub use types::Measurement;
pub use types::MetricsError;
pub use types::Sample;

/// Construct the timeseries name for a Target and Metric.
pub fn timeseries_name<T, M>(
    target: &T,
    metric: &M,
) -> Result<TimeseriesName, MetricsError>
where
    T: Target,
    M: Metric,
{
    TimeseriesName::try_from(format!("{}:{}", target.name(), metric.name()))
}
