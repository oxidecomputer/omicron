// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for generating and collecting metric data in the Oxide rack.
//!
//! Overview
//! --------
//!
//! `oximeter` is a crate for working with metric data. It includes tools for defining new metrics
//! in Rust code, exporting them to a collection server, storing them in a ClickHouse database, and
//! retrieving the data through queries. The system is designed to enable reporting of lots of
//! metric and telemetry information from the Oxide rack, and into Omicron, the Oxide control
//! plane.
//!
//! In general, a _metric_ is a quantity or value exposed by some software. For example, this might
//! be "requests per second" from a service, or the current temperature on a DIMM temperature
//! sensor. A _target_ is the thing being measured -- the service or the DIMM, in these cases. Both
//! targets and metrics may have key-value pairs associated with them, called _fields_.
//!
//! Motivating example
//! ------------------
//!
//! ```rust
//! use uuid::Uuid;
//! use oximeter::{types::Cumulative, Metric, Target};
//!
//! #[derive(Target)]
//! struct HttpServer {
//!     name: String,
//!     id: Uuid,
//! }
//!
//! #[derive(Metric)]
//! struct TotalRequests {
//!     route: String,
//!     method: String,
//!     response_code: i64,
//!     #[datum]
//!     total: Cumulative<i64>,
//! }
//! ```
//!
//! In this case, our target is some HTTP server, which we identify with the fields "name" and
//! "id". The metric of interest is the total count of requests, by route/method/response code,
//! over time. The [`types::Cumulative`] type keeps track of cumulative scalar values, an integer
//! in this case.
//!
//! Datum, measurement, and samples
//! -------------------------------
//!
//! In the above example, the field `total` in the `TotalRequests` struct is annotated with the
//! `#[datum]` helper. This identifies this field as the _datum_, or underlying quantity or value
//! that the metric captures. (Note that the field may also just be _named_ `datum`. These two
//! methods for identifying the field of interest are the same.) A datum may be:
//!
//! - a **gauge**, an instantaneous value at some point in time, or
//! - **cumulative**, an aggregated value over some defined time interval.
//!
//! The supported data types are identified by the [`traits::Datum`] trait.
//!
//! When the current value for a metric is to be retrieved, the [`Metric::measure`] method is
//! called. This generates a [`Measurement`], which contains the value of the datum, along with the
//! current timestamp.
//!
//! While the measurement captures the timestamped datum, it must be combined with information
//! about the target and metric to be meaningful. This is represented by a [`Sample`], which
//! includes the measurement and the collection of fields from both the target and metric.
//!
//! Producers
//! ---------
//!
//! How are samples generated? Consumers should define a type that implements the [`Producer`]
//! trait. This is a simple interface for generating an iterator over `Sample`s. A producer can
//! generate any number of samples, from any number of targets and metrics. For example, we might
//! have a producer that keeps track of all requests the above `HttpServer` receives, by storing
//! one instance of the `TotalRequests` for each unique combination of route, method, and response
//! code. This producer could then produce a collection of `Sample`s, one from each request counter
//! it tracks.
//!
//! Exporting data
//! --------------
//!
//! Data can be exported from a software component, and stored in the control plane's timeseries
//! database. The easiest way to export data is to use an
//! [`oximeter_producer::Server`](../producer/struct.Server). Once running, any `Producer` may be
//! registered with the server. The server will register itself for periodic data collection with
//! the rest of the Oxide control plane. When its configurable API endpoint is hit, it will call
//! `produce` method of all registered producers, and submit the resulting data for storage in the
//! telemetry database.
//!
//! Keep in mind that, although the data may be collected on one interval, `Producer`s may opt to
//! sample the data on any interval they choose. Consumers may wish to sample data every second,
//! but only submit that once per minute to the rest of the control plane. Similarly, different
//! `Producer`s may be registered with the same `ProducerServer`, each with potentially different
//! sampling intervals.

// Copyright 2023 Oxide Computer Company

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
pub fn timeseries_name<T, M>(target: &T, metric: &M) -> String
where
    T: Target,
    M: Metric,
{
    format!("{}:{}", target.name(), metric.name())
}
