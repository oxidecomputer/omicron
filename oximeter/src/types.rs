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

#[derive(Clone, Copy, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub enum FieldType {
    String,
    I64,
    IpAddr,
    Uuid,
    Bool,
}

#[derive(Clone, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub enum FieldValue {
    String(String),
    I64(i64),
    IpAddr(IpAddr),
    Uuid(Uuid),
    Bool(bool),
}

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

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("Error building metric collection: {0}")]
    InvalidCollection(String),
    #[error("The metric collection is already registered")]
    CollectionAlreadyRegistered,
    #[error("The collection is not registered")]
    CollectionNotRegistered,
    #[error("Failed to set up collection of metric: {0}")]
    CollectionSetupFailed(String),
    #[error("Error collecting measurement: {0}")]
    MeasurementError(String),
    #[error("The producer function returned an unexpected type, expected {0:?}, found {1:?})")]
    ProducerTypeMismatch(MetricType, MetricType),
    #[error("{0}")]
    DistributionError(#[from] distribution::DistributionError),
}

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
pub struct Sample {
    pub timestamp: DateTime<Utc>,
    pub target_name: String,
    pub target_fields: BTreeMap<String, FieldValue>,
    pub metric_name: String,
    pub metric_fields: BTreeMap<String, FieldValue>,
    pub metric_kind: MetricKind,
    pub metric_type: MetricType,
    pub measurement: Measurement,
}

impl Sample {
    pub(crate) fn new(
        target: &Box<dyn Target>,
        metric: &Box<dyn Metric>,
        measurement: &Measurement,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
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
