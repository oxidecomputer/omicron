// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Schema-related types for the Oximeter collector.
//!
//! Changes in this version:
//!
//! * Add [`Units::Joules`].
//! * New version of [`TimeseriesSchema`] to pick up the new [`Units`].

use crate::v1::schema::AuthzScope;
use crate::v1::schema::FieldSchema;
use crate::v1::schema::TimeseriesDescription;
use crate::v1::schema::TimeseriesName;
use crate::v1::types::DatumType;
use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;
use std::num::NonZeroU8;

#[derive(Debug, thiserror::Error)]
#[error("new Units value (`Joules`) cannot be represented in old API version")]
pub struct UnitsConversionJoulesError;

/// Measurement units for timeseries samples.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
// TODO-completeness: Decide whether and how to handle dimensional analysis
// during queries, if needed.
pub enum Units {
    /// No meaningful units, e.g. a dimensionless quanity.
    None,
    Count,
    Bytes,
    Seconds,
    Nanoseconds,
    Volts,
    Amps,
    Watts,
    DegreesCelsius,
    /// Rotations per minute.
    Rpm,
    Joules,
}

impl TryFrom<Units> for crate::v1::schema::Units {
    type Error = UnitsConversionJoulesError;

    fn try_from(value: Units) -> Result<Self, Self::Error> {
        match value {
            Units::None => Ok(Self::None),
            Units::Count => Ok(Self::Count),
            Units::Bytes => Ok(Self::Bytes),
            Units::Seconds => Ok(Self::Seconds),
            Units::Nanoseconds => Ok(Self::Nanoseconds),
            Units::Volts => Ok(Self::Volts),
            Units::Amps => Ok(Self::Amps),
            Units::Watts => Ok(Self::Watts),
            Units::DegreesCelsius => Ok(Self::DegreesCelsius),
            Units::Rpm => Ok(Self::Rpm),
            Units::Joules => Err(UnitsConversionJoulesError),
        }
    }
}

/// The schema for a timeseries.
///
/// This includes the name of the timeseries, as well as the datum type of its metric and the
/// schema for each field.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct TimeseriesSchema {
    pub timeseries_name: TimeseriesName,
    pub description: TimeseriesDescription,
    pub field_schema: BTreeSet<FieldSchema>,
    pub datum_type: DatumType,
    pub version: NonZeroU8,
    pub authz_scope: AuthzScope,
    pub units: Units,
    pub created: DateTime<Utc>,
}

impl TryFrom<TimeseriesSchema> for crate::v1::schema::TimeseriesSchema {
    type Error = UnitsConversionJoulesError;

    fn try_from(value: TimeseriesSchema) -> Result<Self, Self::Error> {
        let units = value.units.try_into()?;

        Ok(Self {
            timeseries_name: value.timeseries_name,
            description: value.description,
            field_schema: value.field_schema,
            datum_type: value.datum_type,
            version: value.version,
            authz_scope: value.authz_scope,
            units,
            created: value.created,
        })
    }
}
