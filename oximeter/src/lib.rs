//! Tools for generating and collecting metric data in the Oxide rack.
// Copyright 2021 Oxide Computer Company

pub use oximeter_macro_impl::{MetricKind, MetricType};
pub use oximeter_metric::*;
pub use oximeter_target::*;

mod collector;
pub mod distribution;
mod producer;
mod types;

pub use collector::{Collector, CollectionToken};
pub use producer::{Counter, Distribution, Producer};
pub use types::{Error, FieldType, FieldValue, Measurement, Sample};

/// The `Target` trait identifies a metric source by a sequence of fields.
///
/// A target is a single source of metric data, identified by a sequence of named and typed field
/// values. Users can write a single struct definition and derive this trait. The methods here
/// provide some introspection into the struct, listing its fields and their values. The struct
/// definition can be thought of as a schema, and an instance of that struct as identifying an
/// individual target.
///
/// Target fields may have one of a set of supported types: bool, i64, String, IpAddr, or Uuid. Any
/// number of fields greater than zero is supported.
///
/// Examples
/// --------
///
/// ```rust
/// use oximeter::{Target, FieldType};
/// use uuid::Uuid;
///
/// #[derive(Target)]
/// struct VirtualMachine {
///     name: String,
///     id: Uuid,
/// }
///
/// fn main() {
///     let vm = VirtualMachine { name: String::from("a-name"), id: Uuid::new_v4() };
///     assert_eq!(vm.name(), "virtual_machine");
///     assert_eq!(vm.field_names()[0], "name");
///     assert_eq!(vm.field_types()[1], FieldType::Uuid);
/// }
/// ```
///
/// Targets may implement other methods, if the user wishes, but the fields must be one of the
/// supported types.
///
/// ```compile_fail
/// #[derive(oximeter::Target)]
/// struct Bad {
///     bad: f64,
/// }
/// ```
///
/// See the [tests](./tests) directory for many examples of how to use the type.
pub trait Target {
    /// Return the name of the target, which is the snake_case form of the struct's name.
    fn name(&self) -> &'static str;

    /// Return the names of the target's fields, in the order in which they're defined.
    fn field_names(&self) -> &'static [&'static str];

    /// Return the types of the target's fields.
    fn field_types(&self) -> &'static [FieldType];

    /// Return the values of the target's fields.
    fn field_values(&self) -> Vec<FieldValue>;

    /// Return the key for this target.
    ///
    /// Targets are uniquely identified by their name and the sequence of _values_ of their fields.
    /// These are converted to strings and joined by the `":"` character.
    fn key(&self) -> String;
}

/// The `Metric` trait identifies a measured feature of a target.
///
/// Metrics provide data about a single feature of a target resource. Similar to targets, metrics
/// conform to a simple schema, defined by their fields. In addition, a metric has a _kind_. A
/// _gauge_ is an instantaneous measurement of a metric, and may be a numeric or string, or a blob
/// of uninterpreted bytes. A _cumulative_ metric reflects a value that accumulates over time, and
/// must be numeric. It can be of either integer (`i64`) or floating-point type (`f64`) or a
/// distribution over either. See the [`distribution::Distribution`] type for more
/// details.
///
/// Example
/// -------
///
/// The struct below might be used to accumulate the total number of requests handled by a server.
/// Those requests may be broken out in several different ways, in this case by path or endpoint,
/// request method, and the status code with which the server responded.
///
/// ```rust
/// #[oximeter::metric("cumulative", "i64")]
/// struct RequestCount {
///     path: String,
///     method: String,
///     response_code: i64
/// }
/// ```
///
/// The `Metric` trait provides the same methods for introspection as the [`Target`] trait, such as
/// listing the field names and values. Additionally, it specifies the kind and type of the metric.
pub trait Metric {
    /// Return the name of the metric, which is the snake_case form of the struct's name.
    fn name(&self) -> &'static str;

    /// Return the names of the metric's fields, in the order in which they're defined.
    fn field_names(&self) -> &'static [&'static str];

    /// Return the types of the metric's fields.
    fn field_types(&self) -> &'static [FieldType];

    /// Return the values of the metric's fields.
    fn field_values(&self) -> Vec<FieldValue>;

    /// Return the key for this metric.
    ///
    /// Targets are uniquely identified by their name and the sequence of _values_ of their fields.
    /// These are converted to strings and joined by the `":"` character. Note that the metric name
    /// occurs _last_.
    fn key(&self) -> String;

    /// Return the kind of this metric.
    fn metric_kind(&self) -> MetricKind;

    /// Return the data type of this metric.
    fn metric_type(&self) -> MetricType;
}
