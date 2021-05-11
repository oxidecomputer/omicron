//! Traits used to describe metric data and its sources.
// Copyright 2021 Oxide Computer Company

use bytes::Bytes;

use crate::histogram::Histogram;
use crate::types::{Cumulative, Sample};
use crate::{Error, FieldType, FieldValue, MeasurementType};

/// The `Target` trait identifies a source of metric data by a sequence of fields.
///
/// A target is a single source of metric data, identified by a sequence of named and typed field
/// values. Users can write a single struct definition and derive this trait. The methods here
/// provide some introspection into the struct, listing its fields and their values. The struct
/// definition can be thought of as a schema, and an instance of that struct as identifying an
/// individual target.
///
/// Target fields may have one of a set of supported types: `bool`, `i64`, `String`, `IpAddr`, or
/// `Uuid`. Any number of fields greater than zero is supported.
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
/// let vm = VirtualMachine { name: String::from("a-name"), id: Uuid::new_v4() };
///
/// // The "name" of the target is the struct name in snake_case.
/// assert_eq!(vm.name(), "virtual_machine");
///
/// // The field names are the names of the struct field, in order.
/// assert_eq!(vm.field_names()[0], "name");
///
/// // Each field has a specified type and value
/// assert_eq!(vm.field_types()[1], FieldType::Uuid);
/// assert_eq!(vm.field_values()[0], "a-name".into());
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
/// The trait is similar to the `Target` trait, providing metadata about the metric's name and
/// fields. In addition, a `Metric` has an associated measurement type, which must be one of the
/// supported [`DataPoint`] types. This provides type safety, ensuring that the produced
/// measurements are of the correct type for a metric.
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
///
/// Individual samples are produced by client code, and associated with a target and metric by
/// constructing a [`Sample`](crate::types::Sample).
pub trait DataPoint {}

impl DataPoint for bool {}
impl DataPoint for i64 {}
impl DataPoint for f64 {}
impl DataPoint for String {}
impl DataPoint for Bytes {}
impl DataPoint for Cumulative<i64> {}
impl DataPoint for Cumulative<f64> {}
impl DataPoint for Histogram<i64> {}
impl DataPoint for Histogram<f64> {}

/// A trait for generating samples from a target and metric.
///
/// The `Producer` trait connects a target and metric with actual measurements from them. Types
/// that implement this trait are expected to collect data from their targets, and return that in
/// the [`Producer::produce`] function.
///
/// Measurements can be generated on-demand, when the `produce` method is called, or types may
/// choose to collect the data at another time, cache it, and report it via that function. Types
/// may produce any number of measurements, from any number of targets and metrics. The targets and
/// metrics need not have the same type, either. Data is returned as an iterator over [`Sample`]s,
/// which can be constructed from any [`Target`] or [`Metric`].
///
/// Example
/// -------
/// ```rust
/// use oximeter::{metric, Error, Metric, Producer, Target};
/// use oximeter::types::{Measurement, Sample, Cumulative};
///
/// #[derive(Clone, Target)]
/// pub struct Server {
///     pub name: String,
/// }
///
/// #[metric(CumulativeI64)]
/// #[derive(Clone)]
/// pub struct RequestCount {
///     pub route: String,
///     pub method: String,
///     pub response_code: i64
/// }
///
/// fn route_handler(_route: &str, _method: &str) -> i64 {
///     // Actually handle the request
///     200
/// }
///
/// pub struct RequestCounter {
///     target: Server,
///     metric: RequestCount,
///     counter: Cumulative<i64>,
/// }
///
/// impl RequestCounter {
///     pub fn new(target: &Server, metric: &RequestCount) -> Self {
///         Self {
///             target: target.clone(),
///             metric: metric.clone(),
///             counter: Cumulative::new(0),
///         }
///     }
///
///     pub fn bump(&mut self) {
///         self.counter.increment();
///     }
/// }
///
/// impl Producer for RequestCounter {
///     fn produce(&mut self) -> Result<Box<dyn Iterator<Item = Sample>>, Error> {
///         let sample = Sample::new(
///             &self.target,
///             &self.metric,
///             self.counter,
///             None, // Use current timestamp
///         );
///         Ok(Box::new(vec![sample].into_iter()))
///     }
/// }
///
/// fn main() {
///     let server = Server { name: "Nexus".to_string() };
///     let request_count = RequestCount {
///         route: "/".to_string(),
///         method: "HEAD".to_string(),
///         response_code: 200,
///     };
///     let mut producer = RequestCounter::new(&server, &request_count);
///
///     // No requests yet, there should be zero samples
///     let sample = producer.produce().unwrap().next().unwrap();
///     assert_eq!(sample.measurement, Measurement::CumulativeI64(Cumulative::new(0)));
///
///     // await some request..
///     let response_code = route_handler("/", "GET");
///     if response_code == 200 {
///         producer.bump();
///     } // Handle other responses
///
///     // The incremented counter is reflected in the new sample.
///     let sample = producer.produce().unwrap().next().unwrap();
///     assert_eq!(sample.measurement, Measurement::CumulativeI64(Cumulative::new(1)));
/// }
/// ```
pub trait Producer {
    /// Return the currently available samples from the monitored targets and metrics.
    fn produce(&mut self) -> Result<Box<dyn Iterator<Item = Sample>>, Error>;
}

#[cfg(test)]
mod tests {
    use crate::types;
    use crate::{
        metric, Error, FieldType, FieldValue, MeasurementType, Metric,
        Producer, Target,
    };
    use std::boxed::Box;

    #[derive(Clone, Target)]
    struct Targ {
        pub good: bool,
        pub id: i64,
    }

    #[metric(i64)]
    #[derive(Clone)]
    struct Met {
        pub good: bool,
        pub id: i64,
    }

    struct Prod {
        pub target: Targ,
        pub metric: Met,
        pub value: i64,
    }

    impl Producer for Prod {
        fn produce(
            &mut self,
        ) -> Result<Box<dyn Iterator<Item = types::Sample>>, Error> {
            Ok(Box::new(
                vec![types::Sample::new(
                    &self.target,
                    &self.metric,
                    self.value,
                    None,
                )]
                .into_iter(),
            ))
        }
    }

    #[test]
    fn test_target_trait() {
        let t = Targ { good: false, id: 2 };

        assert_eq!(t.name(), "targ");
        assert_eq!(t.key(), "targ:false:2");
        assert_eq!(t.field_names(), &["good", "id"]);
        assert_eq!(t.field_types(), &[FieldType::Bool, FieldType::I64]);
        assert_eq!(
            t.field_values(),
            &[FieldValue::Bool(false), FieldValue::I64(2)]
        );
    }

    #[test]
    fn test_metric_trait() {
        let m = Met { good: false, id: 2 };

        assert_eq!(m.name(), "met");
        assert_eq!(m.key(), "false:2:met");
        assert_eq!(m.field_names(), &["good", "id"]);
        assert_eq!(m.field_types(), &[FieldType::Bool, FieldType::I64]);
        assert_eq!(
            m.field_values(),
            &[FieldValue::Bool(false), FieldValue::I64(2)]
        );
        assert_eq!(m.measurement_type(), MeasurementType::I64);
    }

    #[test]
    fn test_producer_trait() {
        let t = Targ { good: false, id: 2 };
        let m = Met { good: false, id: 2 };
        let mut p = Prod { target: t.clone(), metric: m.clone(), value: 0 };
        let sample = p.produce().unwrap().next().unwrap();
        assert_eq!(sample.target.key, t.key());
        assert_eq!(sample.metric.key, m.key());
        assert_eq!(sample.measurement, types::Measurement::I64(0));
        p.value += 10;
        let sample = p.produce().unwrap().next().unwrap();
        assert_eq!(sample.measurement, types::Measurement::I64(10));
    }
}
