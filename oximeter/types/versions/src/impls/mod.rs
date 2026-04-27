// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for the latest versions of types.

mod producer;
pub mod traits;

use crate::latest::schema::TimeseriesName;
use crate::latest::types::MetricsError;

/// Construct the timeseries name for a Target and Metric.
pub fn timeseries_name<T, M>(
    target: &T,
    metric: &M,
) -> Result<TimeseriesName, MetricsError>
where
    T: traits::Target,
    M: traits::Metric,
{
    TimeseriesName::try_from(format!("{}:{}", target.name(), metric.name()))
}
