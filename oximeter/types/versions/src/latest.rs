// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest versions of all published types.

pub mod collector {
    pub use crate::v1::collector::CollectorInfo;
}

pub mod histogram {
    pub use crate::v1::histogram::Bin;
    pub use crate::v1::histogram::BinRange;
    pub use crate::v1::histogram::Bits;
    pub use crate::v1::histogram::Histogram;
    pub use crate::v1::histogram::HistogramAdditiveWidth;
    pub use crate::v1::histogram::HistogramError;
    pub use crate::v1::histogram::HistogramSupport;
    pub use crate::v1::histogram::LogLinearBins;
    pub use crate::v1::histogram::QuantizationError;
    pub use crate::v1::histogram::Record;
}

pub mod producer {
    pub use crate::v1::producer::FailedCollection;
    pub use crate::v1::producer::ProducerDetails;
    pub use crate::v1::producer::ProducerIdPathParams;
    pub use crate::v1::producer::ProducerPage;
    pub use crate::v1::producer::SuccessfulCollection;
}

pub mod quantile {
    pub use crate::v1::quantile::Quantile;
    pub use crate::v1::quantile::QuantileError;
}

pub mod schema {
    pub use crate::v1::schema::AuthzScope;
    pub use crate::v1::schema::FieldSchema;
    pub use crate::v1::schema::FieldSource;
    pub use crate::v1::schema::TimeseriesDescription;
    pub use crate::v1::schema::TimeseriesKey;
    pub use crate::v1::schema::TimeseriesName;
    pub use crate::v1::schema::TimeseriesSchema;
    pub use crate::v1::schema::Units;
    pub use crate::v1::schema::default_schema_version;
}

pub mod traits {
    pub use crate::impls::traits::Cumulative;
    pub use crate::impls::traits::Datum;
    pub use crate::impls::traits::Gauge;
    pub use crate::impls::traits::Metric;
    pub use crate::impls::traits::Producer;
    pub use crate::impls::traits::Target;
}

pub mod types {
    pub use crate::v1::types::Cumulative;
    pub use crate::v1::types::Datum;
    pub use crate::v1::types::DatumType;
    pub use crate::v1::types::Field;
    pub use crate::v1::types::FieldType;
    pub use crate::v1::types::FieldValue;
    pub use crate::v1::types::Measurement;
    pub use crate::v1::types::MetricsError;
    pub use crate::v1::types::MissingDatum;
    pub use crate::v1::types::ProducerRegistry;
    pub use crate::v1::types::ProducerResultsItem;
    pub use crate::v1::types::Sample;
}
