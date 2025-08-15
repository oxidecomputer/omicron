// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Core types for OxQL.

use chrono::{DateTime, Utc};
use std::time::Duration;

pub mod point;
pub mod table;

pub use self::table::Table;
pub use self::table::TableOutput;
pub use self::table::Timeseries;

/// Describes the time alignment for an OxQL query.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Alignment {
    /// The end time of the query, which the temporal reference point.
    pub end_time: DateTime<Utc>,
    /// The alignment period, the interval on which values are produced.
    pub period: Duration,
}
