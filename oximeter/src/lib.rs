//! Tools for generating and collecting metric data in the Oxide rack.
// Copyright 2021 Oxide Computer Company

pub use oximeter_macro_impl::{MetricKind, MetricType};
pub use oximeter_metric::*;
pub use oximeter_target::*;

mod collector;
pub mod distribution;
mod producer;
mod types;
mod traits;

pub use collector::{Collector, CollectionToken};
pub use producer::{Counter, Distribution, Producer};
pub use types::{Error, FieldType, FieldValue, Measurement, Sample};
pub use traits::{Target, Metric};
