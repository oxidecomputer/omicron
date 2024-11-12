// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Constants used for common column names.

// Copyright 2024 Oxide Computer Company

pub const TIMESERIES_NAME: &str = "timeseries_name";
pub const TIMESERIES_KEY: &str = "timeseries_key";
pub const DATUM_TYPE: &str = "datum_type";
pub const FIELDS_DOT_NAME: &str = "fields.name";
pub const FIELDS_DOT_SOURCE: &str = "fields.source";
pub const FIELDS_DOT_TYPE: &str = "fields.type";
pub const CREATED: &str = "created";
pub const START_TIME: &str = "start_time";
pub const TIMESTAMP: &str = "timestamp";
pub const FIELD_NAME: &str = "field_name";
pub const FIELD_VALUE: &str = "field_value";
pub const DATUM: &str = "datum";
pub const BINS: &str = "bins";
pub const COUNTS: &str = "counts";
pub const MIN: &str = "min";
pub const MAX: &str = "max";
pub const SUM_OF_SAMPLES: &str = "sum_of_samples";
pub const SQUARED_MEAN: &str = "squared_mean";
pub const P50_MARKER_HEIGHTS: &str = "p50_marker_heights";
pub const P50_MARKER_POSITIONS: &str = "p50_marker_positions";
pub const P50_DESIRED_MARKER_POSITIONS: &str = "p50_desired_marker_positions";
pub const P90_MARKER_HEIGHTS: &str = "p90_marker_heights";
pub const P90_MARKER_POSITIONS: &str = "p90_marker_positions";
pub const P90_DESIRED_MARKER_POSITIONS: &str = "p90_desired_marker_positions";
pub const P99_MARKER_HEIGHTS: &str = "p99_marker_heights";
pub const P99_MARKER_POSITIONS: &str = "p99_marker_positions";
pub const P99_DESIRED_MARKER_POSITIONS: &str = "p99_desired_marker_positions";

/// Supported quantiles for histograms.
#[derive(Clone, Copy, Debug, strum::EnumIter)]
pub enum Quantile {
    P50,
    P90,
    P99,
}

impl Quantile {
    /// Return the marker height column name.
    pub const fn marker_heights(self) -> &'static str {
        match self {
            Quantile::P50 => P50_MARKER_HEIGHTS,
            Quantile::P90 => P90_MARKER_HEIGHTS,
            Quantile::P99 => P99_MARKER_HEIGHTS,
        }
    }

    /// Return the marker position column name.
    pub const fn marker_positions(self) -> &'static str {
        match self {
            Quantile::P50 => P50_MARKER_POSITIONS,
            Quantile::P90 => P90_MARKER_POSITIONS,
            Quantile::P99 => P99_MARKER_POSITIONS,
        }
    }

    /// Return the desired marker position column name.
    pub const fn desired_marker_positions(self) -> &'static str {
        match self {
            Quantile::P50 => P50_DESIRED_MARKER_POSITIONS,
            Quantile::P90 => P90_DESIRED_MARKER_POSITIONS,
            Quantile::P99 => P99_DESIRED_MARKER_POSITIONS,
        }
    }
}
