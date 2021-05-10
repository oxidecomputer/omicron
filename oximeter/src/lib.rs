//! Tools for generating and collecting metric data in the Oxide rack.
// Copyright 2021 Oxide Computer Company

pub use oximeter_macro_impl::MeasurementType;
pub use oximeter_metric::*;
pub use oximeter_target::*;

pub mod distribution;
pub mod traits;
pub mod types;

pub use traits::{Metric, Target};
pub use types::{Error, FieldType, FieldValue};
