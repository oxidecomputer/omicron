use bytes::Bytes;

use crate::distribution::Distribution;
use crate::types::{Cumulative, Sample};
use crate::{FieldType, FieldValue, MeasurementType};

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

pub trait Metric {
    type Measurement: DataPoint;

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

    /// Return the data type of a measurement for this this metric.
    fn measurement_type(&self) -> MeasurementType;
}

/// The `DataPoint` trait identifies types that may be used as measurements or samples for a
/// timeseries.
pub trait DataPoint {}

impl DataPoint for bool {}
impl DataPoint for i64 {}
impl DataPoint for f64 {}
impl DataPoint for String {}
impl DataPoint for Bytes {}
impl DataPoint for Cumulative<i64> {}
impl DataPoint for Cumulative<f64> {}
impl DataPoint for Distribution<i64> {}
impl DataPoint for Distribution<f64> {}

pub trait Producer {
    fn produce(&mut self) -> Box<dyn Iterator<Item = Sample>>;
}
