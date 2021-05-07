//! Types used to describe targets, metrics, and measurements.
// Copyright 2021 Oxide Computer Company

use std::boxed::Box;
use std::collections::BTreeMap;
use std::net::IpAddr;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::distribution;
use crate::{Metric, MetricKind, MetricType, Target};

/// The `FieldType` identifies the type of a target or metric field.
#[derive(Clone, Copy, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub enum FieldType {
    String,
    I64,
    IpAddr,
    Uuid,
    Bool,
}

/// The `FieldValue` contains the value of a target or metric field.
#[derive(Clone, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub enum FieldValue {
    String(String),
    I64(i64),
    IpAddr(IpAddr),
    Uuid(Uuid),
    Bool(bool),
}

/// A measurement is a single sample of a metric.
#[derive(Clone, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub enum Measurement {
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Bytes),
    DistributionI64(distribution::Distribution<i64>),
    DistributionF64(distribution::Distribution<f64>),
}

impl Measurement {
    /// Return the [`MetricType`] for this measurement.
    pub fn metric_type(&self) -> MetricType {
        match self {
            Measurement::Bool(_) => MetricType::Bool,
            Measurement::I64(_) => MetricType::I64,
            Measurement::F64(_) => MetricType::F64,
            Measurement::String(_) => MetricType::String,
            Measurement::Bytes(_) => MetricType::Bytes,
            Measurement::DistributionI64(_) => MetricType::DistributionI64,
            Measurement::DistributionF64(_) => MetricType::DistributionF64,
        }
    }
}

impl From<i64> for Measurement {
    fn from(value: i64) -> Self {
        Measurement::I64(value)
    }
}

impl From<f64> for Measurement {
    fn from(value: f64) -> Self {
        Measurement::F64(value)
    }
}

impl From<String> for Measurement {
    fn from(value: String) -> Self {
        Measurement::String(value)
    }
}

impl From<&str> for Measurement {
    fn from(value: &str) -> Self {
        Measurement::String(value.to_string())
    }
}

impl From<&Bytes> for Measurement {
    fn from(value: &Bytes) -> Self {
        Measurement::Bytes(value.clone())
    }
}

impl From<distribution::Distribution<i64>> for Measurement {
    fn from(value: distribution::Distribution<i64>) -> Measurement {
        Measurement::DistributionI64(value)
    }
}

impl From<distribution::Distribution<f64>> for Measurement {
    fn from(value: distribution::Distribution<f64>) -> Measurement {
        Measurement::DistributionF64(value)
    }
}

/// Errors related to the generation or collection of metrics.
#[derive(Debug, Clone, Error)]
pub enum Error {
    /// An error occurred registering or building a collection of metrics.
    #[error("Error building metric collection: {0}")]
    InvalidCollection(String),
    /// A collection of metrics is already registered.
    #[error("The metric collection is already registered")]
    CollectionAlreadyRegistered,
    /// A collection of metrics is not registered.
    #[error("The collection is not registered")]
    CollectionNotRegistered,
    /// An error occurred calling the registered
    /// [`Producer::setup_collection`](crate::producer::Producer::setup_collection) method.
    #[error("Failed to set up collection of metric: {0}")]
    CollectionSetupFailed(String),
    /// An error occurred calling the registered
    /// [`Producer::collect`](crate::producer::Producer::collect) method.
    #[error("Error collecting measurement: {0}")]
    MeasurementError(String),
    /// The [`Producer::collect`](crate::producer::Producer::collect) method return an unexpected
    /// type for a metric.
    #[error("The producer function returned an unexpected type, expected {0:?}, found {1:?})")]
    ProducerTypeMismatch(MetricType, MetricType),
    /// An error related to creating or sampling a [`distribution::Distribution`] metric.
    #[error("{0}")]
    DistributionError(#[from] distribution::DistributionError),
}

/// Complete information about a single sample for a metric.
///
/// The `Sample` struct contains all information about a single sample from a target and metric,
/// including the field names, types, and values; the timestamp; and the actual measurement itself.
/// This is the main type used to report metrics from a client to the `oximeter` server.
#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
pub struct Sample {
    /// The timestamp at which the measurement was taken.
    pub timestamp: DateTime<Utc>,

    /// The name of the target for this sample.
    pub target_name: String,

    /// The sequence of target fields, including their names and values.
    pub target_fields: BTreeMap<String, FieldValue>,

    /// The name of the metric for this sample.
    pub metric_name: String,

    /// The sequence of metric fields, including their names and values.
    pub metric_fields: BTreeMap<String, FieldValue>,

    /// The kind of metric this sample corresponds to.
    pub metric_kind: MetricKind,

    /// The type of this measurement.
    pub metric_type: MetricType,

    /// The measurement or data point itself.
    pub measurement: Measurement,
}

impl Sample {
    pub(crate) fn new(
        target: &Box<dyn Target>,
        metric: &Box<dyn Metric>,
        measurement: &Measurement,
        timestamp: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            timestamp: timestamp.unwrap_or_else(Utc::now),
            target_name: target.name().to_string(),
            target_fields: target
                .field_names()
                .iter()
                .map(|s| s.to_string())
                .zip(target.field_values())
                .collect(),
            metric_name: metric.name().to_string(),
            metric_fields: metric
                .field_names()
                .iter()
                .map(|s| s.to_string())
                .zip(metric.field_values())
                .collect(),
            metric_kind: metric.metric_kind(),
            metric_type: metric.metric_type(),
            measurement: measurement.clone(),
        }
    }
}
