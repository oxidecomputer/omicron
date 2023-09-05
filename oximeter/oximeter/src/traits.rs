// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Traits used to describe metric data and its sources.
// Copyright 2021 Oxide Computer Company

use crate::histogram::Histogram;
use crate::types;
use crate::types::{Measurement, Sample};
use crate::{DatumType, Field, FieldType, FieldValue, MetricsError};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use num_traits::{One, Zero};
use std::ops::{Add, AddAssign};

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
    fn field_types(&self) -> Vec<FieldType>;

    /// Return the values of the target's fields.
    fn field_values(&self) -> Vec<FieldValue>;

    /// Return the target's fields, both name and value.
    fn fields(&self) -> Vec<Field> {
        self.field_names()
            .iter()
            .zip(self.field_values())
            .map(|(name, value)| Field { name: name.to_string(), value })
            .collect()
    }
}

/// The `Metric` trait identifies a measured feature of a target.
///
/// The trait is similar to the `Target` trait, providing metadata about the metric's name and
/// fields. In addition, a `Metric` has an associated datum type, which must be one of the
/// supported [`Datum`] types. This provides type safety, ensuring that the produced
/// measurements are of the correct type for a metric.
///
/// Users should derive the [`oximeter::Metric`] trait on a struct. The struct's fields are the
/// fields (names and types) of the metric itself. This generates the trait implementation from the
/// type's declaration.
///
/// One field of the struct is special, describing the actual measured data that the metric
/// represents. This should be a field named `datum`, or another field (with any name you choose)
/// annotated with the `#[datum]` attribute. This field represents the underlying data for the
/// metric, and must be one of the supported types, implementing the [`Datum`] trait. This can
/// be any of: `i64`, `f64`, `bool`, `String`, or `Bytes` for gauges, and `Cumulative<T>` or
/// `Histogram<T>` for cumulative metrics, where `T` is `i64` or `f64`.
///
/// The value of the metric's data is _measured_ by using the `measure()` method, which returns a
/// [`Measurement`]. This describes a timestamped data point for the metric.
///
/// Example
/// -------
/// ```rust
/// use chrono::Utc;
/// use oximeter::Metric;
///
/// // A gauge with a floating-point value.
/// #[derive(Metric)]
/// struct MyMetric {
///     name: String,
///     datum: f64,
/// }
///
/// let met = MyMetric { name: "name".into(), datum: 0.0 };
/// assert_eq!(met.datum_type(), oximeter::DatumType::F64);
/// let measurement = met.measure(Utc::now());
/// assert!(measurement.start_time().is_none());
/// assert_eq!(measurement.datum(), &oximeter::Datum::F64(0.0));
/// ```
///
/// A compiler error will be generated if the attribute is applied to a struct whose fields are of
/// an unsupported type.
///
/// ```compile_fail
/// #[derive(Metric)]
/// pub struct BadType {
///     field: f32,
/// }
/// ```
pub trait Metric {
    /// The type of datum produced by this metric.
    type Datum: Datum;

    /// Return the name of the metric, which is the snake_case form of the struct's name.
    fn name(&self) -> &'static str;

    /// Return the names of the metric's fields, in the order in which they're defined.
    fn field_names(&self) -> &'static [&'static str];

    /// Return the types of the metric's fields.
    fn field_types(&self) -> Vec<FieldType>;

    /// Return the values of the metric's fields.
    fn field_values(&self) -> Vec<FieldValue>;

    /// Return the metrics's fields, both name and value.
    fn fields(&self) -> Vec<Field> {
        self.field_names()
            .iter()
            .zip(self.field_values())
            .map(|(name, value)| Field { name: name.to_string(), value })
            .collect()
    }

    /// Return the data type of a measurement for this this metric.
    fn datum_type(&self) -> DatumType;

    /// Return the current value of the underlying metric itself.
    fn datum(&self) -> &Self::Datum;

    /// Return a mutable reference to the underlying metric itself.
    fn datum_mut(&mut self) -> &mut Self::Datum;

    /// Sample the underlying metric, with a caller-supplied timestamp.
    fn measure(&self, timestamp: DateTime<Utc>) -> Measurement;

    /// Return true if the metric is cumulative, else false.
    fn is_cumulative(&self) -> bool {
        self.datum_type().is_cumulative()
    }

    /// Return the start time over which this metric's data is valid, or None.
    ///
    /// Gauges, which measure an instantaneous point in time, always represent the sampled resource
    /// at a single, specific timestamp. Cumulative metrics reflect accumulated data about a
    /// resource over a window of time. This method returns `None` for gauge metrics, and the
    /// start time of the sampled data for cumulative metrics.
    fn start_time(&self) -> Option<DateTime<Utc>>;
}

/// The `Datum` trait identifies types that may be used as the underlying data points or samples
/// for a metric.
///
/// Any type implementing this trait may be used in the `datum` field of the [`Metric`] trait.
pub trait Datum: Clone {
    /// Returns the start time, if data of this type is cumulative, or `None` if data represents a
    /// gauge (instantaneous measurement).
    fn start_time(&self) -> Option<DateTime<Utc>> {
        None
    }

    /// Return the [`DatumType`] variant for this type.
    fn datum_type(&self) -> DatumType;
}

impl Datum for bool {
    fn datum_type(&self) -> DatumType {
        DatumType::Bool
    }
}

impl Datum for i64 {
    fn datum_type(&self) -> DatumType {
        DatumType::I64
    }
}

impl Datum for f64 {
    fn datum_type(&self) -> DatumType {
        DatumType::F64
    }
}

impl Datum for String {
    fn datum_type(&self) -> DatumType {
        DatumType::String
    }
}

impl Datum for Bytes {
    fn datum_type(&self) -> DatumType {
        DatumType::Bytes
    }
}

impl Datum for types::Cumulative<i64> {
    fn start_time(&self) -> Option<DateTime<Utc>> {
        Some(types::Cumulative::start_time(&self))
    }
    fn datum_type(&self) -> DatumType {
        DatumType::CumulativeI64
    }
}

impl Datum for types::Cumulative<f64> {
    fn start_time(&self) -> Option<DateTime<Utc>> {
        Some(types::Cumulative::start_time(&self))
    }
    fn datum_type(&self) -> DatumType {
        DatumType::CumulativeF64
    }
}

impl Datum for Histogram<i64> {
    fn start_time(&self) -> Option<DateTime<Utc>> {
        Some(self.start_time())
    }
    fn datum_type(&self) -> DatumType {
        DatumType::HistogramI64
    }
}

impl Datum for Histogram<f64> {
    fn start_time(&self) -> Option<DateTime<Utc>> {
        Some(self.start_time())
    }
    fn datum_type(&self) -> DatumType {
        DatumType::HistogramF64
    }
}

/// A trait identifying types used in [`types::Cumulative`] data.
pub trait Cumulative: Datum + Add + AddAssign + Copy + One + Zero {}

impl Cumulative for i64 {}
impl Cumulative for f64 {}

/// A trait identifying types used as gauges
pub trait Gauge: Datum {}

impl Gauge for String {}
impl Gauge for bool {}
impl Gauge for i64 {}
impl Gauge for f64 {}

pub use crate::histogram::HistogramSupport;

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
/// use oximeter::{Datum, MetricsError, Metric, Producer, Target};
/// use oximeter::types::{Measurement, Sample, Cumulative};
///
/// // The `Server` target identifies some HTTP service being monitored.
/// #[derive(Clone, Debug, Target)]
/// pub struct Server {
///     pub name: String,
/// }
///
/// // The `RequestCount` metric describes the cumulative count of requests the server has
/// // organized by their routes, the HTTP method, and the response code.
/// #[derive(Clone, Debug, Metric)]
/// pub struct RequestCount {
///     route: String,
///     method: String,
///     response_code: i64,
///     #[datum]
///     count: Cumulative<i64>
/// }
///
/// fn route_handler(_route: &str, _method: &str) -> i64 {
///     // Actually handle the request
///     200
/// }
///
/// // The `RequestCounter` type implements the `Producer` trait, to generate samples of the
/// // target/metric being monitored.
/// #[derive(Debug, Clone)]
/// pub struct RequestCounter {
///     target: Server,
///     metric: RequestCount,
/// }
///
/// impl RequestCounter {
///     pub fn new(target: &Server, metric: &RequestCount) -> Self {
///         Self {
///             target: target.clone(),
///             metric: metric.clone(),
///         }
///     }
///
///     pub fn bump(&mut self) {
///         self.metric.datum_mut().increment();
///     }
/// }
///
/// impl Producer for RequestCounter {
///     fn produce(&mut self) -> Result<Box<dyn Iterator<Item = Sample>>, MetricsError> {
///         let sample = Sample::new(&self.target, &self.metric).unwrap();
///         Ok(Box::new(vec![sample].into_iter()))
///     }
/// }
///
/// fn main() {
///     let server = Server { name: "Nexus".to_string() };
///
///     let request_count = RequestCount {
///         route: "/".to_string(),
///         method: "HEAD".to_string(),
///         response_code: 200,
///         count: Cumulative::new(0),
///     };
///     let mut producer = RequestCounter::new(&server, &request_count);
///
///     // No requests yet, there should be zero samples
///     let sample = producer.produce().unwrap().next().unwrap();
///     let datum = sample.measurement.datum();
///     match datum {
///         Datum::CumulativeI64(ref d) => assert_eq!(d.value(), 0),
///         _ => panic!("Expected a CumulativeI64 datum"),
///     }
///
///     // await some request..
///     let response_code = route_handler("/", "GET");
///     if response_code == 200 {
///         producer.bump();
///     } // Handle other responses
///
///     // The incremented counter is reflected in the new sample.
///     let sample = producer.produce().unwrap().next().unwrap();
///     let datum = sample.measurement.datum();
///     match datum {
///         Datum::CumulativeI64(ref d) => assert_eq!(d.value(), 1),
///         _ => panic!("Expected a CumulativeI64 datum"),
///     }
/// }
/// ```
pub trait Producer: Send + Sync + std::fmt::Debug + 'static {
    /// Return the currently available samples from the monitored targets and metrics.
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample>>, MetricsError>;
}

#[cfg(test)]
mod tests {
    use crate::types;
    use crate::{
        Datum, DatumType, FieldType, FieldValue, Metric, MetricsError,
        Producer, Target,
    };
    use std::boxed::Box;

    #[derive(Debug, Clone, Target)]
    struct Targ {
        pub happy: bool,
        pub tid: i64,
    }

    #[derive(Debug, Clone, Metric)]
    struct Met {
        good: bool,
        id: i64,
        datum: i64,
    }

    #[derive(Debug, Clone)]
    struct Prod {
        pub target: Targ,
        pub metric: Met,
    }

    impl Producer for Prod {
        fn produce(
            &mut self,
        ) -> Result<Box<dyn Iterator<Item = types::Sample>>, MetricsError>
        {
            Ok(Box::new(
                vec![types::Sample::new(&self.target, &self.metric)?]
                    .into_iter(),
            ))
        }
    }

    #[test]
    fn test_target_trait() {
        let t = Targ { happy: false, tid: 2 };

        assert_eq!(t.name(), "targ");
        assert_eq!(t.field_names(), &["happy", "tid"]);
        assert_eq!(t.field_types(), &[FieldType::Bool, FieldType::I64]);
        assert_eq!(
            t.field_values(),
            &[FieldValue::Bool(false), FieldValue::I64(2)]
        );
    }

    #[test]
    fn test_metric_trait() {
        let m = Met { good: false, id: 2, datum: 0 };
        assert_eq!(m.name(), "met");
        assert_eq!(m.field_names(), &["good", "id"]);
        assert_eq!(m.field_types(), &[FieldType::Bool, FieldType::I64]);
        assert_eq!(
            m.field_values(),
            &[FieldValue::Bool(false), FieldValue::I64(2)]
        );
        assert_eq!(m.datum_type(), DatumType::I64);
        assert!(m.start_time().is_none());
    }

    #[test]
    fn test_producer_trait() {
        let t = Targ { happy: false, tid: 2 };
        let m = Met { good: false, id: 2, datum: 0 };
        let mut p = Prod { target: t.clone(), metric: m.clone() };
        let sample = p.produce().unwrap().next().unwrap();
        assert_eq!(
            sample.timeseries_name,
            format!("{}:{}", t.name(), m.name())
        );
        assert_eq!(sample.measurement.datum(), &Datum::I64(0));
        p.metric.datum += 10;
        let sample = p.produce().unwrap().next().unwrap();
        assert_eq!(sample.measurement.datum(), &Datum::I64(10));
    }
}
