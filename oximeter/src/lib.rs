pub use oximeter_macro_impl::{MetricKind, MetricType};
pub use oximeter_metric::*;
pub use oximeter_target::*;

mod collector;
pub mod distribution;
mod producer;
mod types;

pub use collector::Collector;
pub use producer::{Counter, Distribution, Producer};
pub use types::{Error, FieldType, FieldValue, Measurement, Sample};

/// The `Target` trait identifies a metric source by a sequence of fields.
pub trait Target {
    fn name(&self) -> &'static str;
    fn field_names(&self) -> &'static [&'static str];
    fn field_types(&self) -> &'static [FieldType];
    fn field_values(&self) -> Vec<FieldValue>;
    fn key(&self) -> String;
}

/// The `Metric` trait identifies a measured feature of a target.
pub trait Metric {
    fn name(&self) -> &'static str;
    fn field_names(&self) -> &'static [&'static str];
    fn field_types(&self) -> &'static [FieldType];
    fn field_values(&self) -> Vec<FieldValue>;
    fn key(&self) -> String;
    fn metric_kind(&self) -> MetricKind;
    fn metric_type(&self) -> MetricType;
}
