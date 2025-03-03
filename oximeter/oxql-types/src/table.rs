// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Definitions of timeseries and groups of them, a [`Table`].

// Copyright 2024 Oxide Computer Company

use crate::Alignment;
use crate::point::DataType;
use crate::point::MetricType;
use crate::point::Points;
use crate::point::ValueArray;
use crate::point::Values;
use anyhow::Error;
use highway::HighwayHasher;
use oximeter_types::FieldValue;
use oximeter_types::schema::TimeseriesKey;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::hash::Hash;
use std::hash::Hasher;

/// A timeseries contains a timestamped set of values from one source.
///
/// This includes the typed key-value pairs that uniquely identify it, and the
/// set of timestamps and data values from it.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct Timeseries {
    pub fields: BTreeMap<String, FieldValue>,
    pub points: Points,
    #[serde(skip)]
    pub(crate) alignment: Option<Alignment>,
}

impl Timeseries {
    /// Construct a new timeseries, from its fields.
    ///
    /// It holds no points or type information. That will be enforced by the
    /// points type as they are added.
    pub fn new(
        fields: impl Iterator<Item = (String, FieldValue)>,
        data_type: DataType,
        metric_type: MetricType,
    ) -> Result<Self, Error> {
        let fields: BTreeMap<_, _> = fields.collect();
        anyhow::ensure!(!fields.is_empty(), "Fields cannot be empty");
        Ok(Self {
            fields,
            points: Points::empty(data_type, metric_type),
            alignment: None,
        })
    }

    pub fn key(&self) -> TimeseriesKey {
        // NOTE: The key here is _not_ stable, like the one used in the database
        // itself to identify timeseries. That's OK, however, because we do not
        // serialize this value anywhere -- it's used entirely for the lifetime
        // of one query, and then thrown away, and only needs to be consistent
        // for that long.
        let mut hasher = HighwayHasher::default();
        for (name, value) in self.fields.iter() {
            name.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Return the alignment of this timeseries, if any.
    pub fn alignment(&self) -> Option<Alignment> {
        self.alignment
    }

    /// Set the alignment of this timeseries.
    pub fn set_alignment(&mut self, alignment: Alignment) {
        self.alignment = Some(alignment);
    }

    /// Return a copy of the timeseries, keeping only the provided fields.
    ///
    /// An error is returned if the timeseries does not contain those fields.
    pub fn copy_with_fields(
        &self,
        kept_fields: &[&str],
    ) -> Result<Self, Error> {
        let mut fields = BTreeMap::new();
        for field in kept_fields {
            let Some(f) = self.fields.get(*field) else {
                anyhow::bail!("Timeseries does not contain field '{}'", field);
            };
            fields.insert(field.to_string(), f.clone());
        }
        Ok(Self {
            fields,
            points: self.points.clone(),
            alignment: self.alignment,
        })
    }

    /// Return a copy of the timeseries, keeping only the provided points.
    ///
    /// Returns `None` if `kept_points` is empty.
    pub fn copy_with_points(&self, kept_points: Points) -> Option<Self> {
        if kept_points.is_empty() {
            return None;
        }
        Some(Self {
            fields: self.fields.clone(),
            points: kept_points,
            alignment: self.alignment,
        })
    }

    // Return `true` if the schema in `other` matches that of `self`.
    fn matches_schema(&self, other: &Timeseries) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }
        for (f0, f1) in self.fields.iter().zip(other.fields.iter()) {
            // Check the field names.
            if f0.0 != f1.0 {
                return false;
            }
            // And types.
            if f0.1.field_type() != f1.1.field_type() {
                return false;
            }
        }

        // And the type info is the same as well.
        if !self
            .points
            .data_types()
            .zip(other.points.data_types())
            .all(|(x, y)| x == y)
        {
            return false;
        }
        self.points
            .metric_types()
            .zip(other.points.metric_types())
            .all(|(x, y)| x == y)
    }

    /// Return a new timeseries, with the points cast to the provided list of
    /// data types.
    ///
    /// This returns an error if the points cannot be so cast, or the
    /// dimensionality of the types requested differs from the dimensionality of
    /// the points themselves.
    pub fn cast(&self, types: &[DataType]) -> Result<Timeseries, Error> {
        let fields = self.fields.clone();
        Ok(Self {
            fields,
            points: self.points.cast(types)?,
            alignment: self.alignment,
        })
    }

    /// Return a new timeseries, with the points limited to the provided range.
    pub fn limit(&self, start: usize, end: usize) -> Self {
        let input_points = &self.points;

        // Slice the various data arrays.
        let start_times =
            input_points.start_times().map(|s| s[start..end].to_vec());
        let timestamps = input_points.timestamps()[start..end].to_vec();
        let values = input_points
            .values
            .iter()
            .map(|vals| {
                let values = match &vals.values {
                    ValueArray::Integer(inner) => {
                        ValueArray::Integer(inner[start..end].to_vec())
                    }
                    ValueArray::Double(inner) => {
                        ValueArray::Double(inner[start..end].to_vec())
                    }
                    ValueArray::Boolean(inner) => {
                        ValueArray::Boolean(inner[start..end].to_vec())
                    }
                    ValueArray::String(inner) => {
                        ValueArray::String(inner[start..end].to_vec())
                    }
                    ValueArray::IntegerDistribution(inner) => {
                        ValueArray::IntegerDistribution(
                            inner[start..end].to_vec(),
                        )
                    }
                    ValueArray::DoubleDistribution(inner) => {
                        ValueArray::DoubleDistribution(
                            inner[start..end].to_vec(),
                        )
                    }
                };
                Values { values, metric_type: vals.metric_type }
            })
            .collect();
        let points = Points::new(start_times, timestamps, values);
        Self { fields: self.fields.clone(), points, alignment: self.alignment }
    }
}

/// A table represents one or more timeseries with the same schema.
///
/// A table is the result of an OxQL query. It contains a name, usually the name
/// of the timeseries schema from which the data is derived, and any number of
/// timeseries, which contain the actual data.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct Table {
    // The name of the table.
    //
    // This starts as the name of the timeseries schema the data is derived
    // from, but can be modified as operations are done.
    pub name: String,
    // The set of timeseries in the table, ordered by key.
    timeseries: BTreeMap<TimeseriesKey, Timeseries>,
}

impl Table {
    /// Create a new table, with no timeseries.
    pub fn new(name: impl AsRef<str>) -> Self {
        Self { name: name.as_ref().to_string(), timeseries: BTreeMap::new() }
    }

    /// Create a table from a set of timeseries.
    pub fn from_timeseries(
        name: impl AsRef<str>,
        t: impl Iterator<Item = Timeseries>,
    ) -> Result<Self, Error> {
        let mut out = Self::new(name);
        for each in t {
            out.insert(each)?;
        }
        Ok(out)
    }

    /// Return the name of the table.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return the number of timeseries in this table.
    pub fn n_timeseries(&self) -> usize {
        self.timeseries.len()
    }

    /// Return the list of timeseries in this table, ordered by key.
    pub fn timeseries(&self) -> impl ExactSizeIterator<Item = &Timeseries> {
        self.timeseries.values()
    }

    // Check that the schema of `other` matches `self`.
    //
    // That means the fields have the same names and types, and the timeseries
    // have the same type info.
    fn matches_schema(&self, other: &Timeseries) -> bool {
        if let Some((_, first)) = self.timeseries.first_key_value() {
            first.matches_schema(other)
        } else {
            // Table is empty.
            true
        }
    }

    /// Get a timeseries matching the provided key, if any.
    pub fn get_mut(&mut self, key: TimeseriesKey) -> Option<&mut Timeseries> {
        self.timeseries.get_mut(&key)
    }

    /// Insert a new timeseries into the table.
    ///
    /// If the timeseries already exists, an error is returned. Use
    /// [`Table::replace()`] to replace an existing timeseries.
    ///
    /// It is an error if the timeseries does not have the same schema as the
    /// others in the table (if any).
    pub fn insert(&mut self, timeseries: Timeseries) -> Result<(), Error> {
        anyhow::ensure!(
            self.matches_schema(&timeseries),
            "Timeseries in a table must have the same schema",
        );
        let key = timeseries.key();
        let Entry::Vacant(e) = self.timeseries.entry(key) else {
            return Err(anyhow::anyhow!(
                "Timeseries with key {} already exists",
                key,
            ));
        };
        e.insert(timeseries);
        Ok(())
    }

    /// Replace a timeseries in the table.
    pub fn replace(&mut self, timeseries: Timeseries) {
        let key = timeseries.key();
        let _ = self.timeseries.insert(key, timeseries);
    }

    /// Add multiple timeseries to the table.
    ///
    /// An error is returned if any timeseries already exist.
    pub fn extend(
        &mut self,
        timeseries: impl Iterator<Item = Timeseries>,
    ) -> Result<(), Error> {
        for t in timeseries {
            self.insert(t)?;
        }
        Ok(())
    }

    /// Return the number of timeseries in the table.
    pub fn len(&self) -> usize {
        self.timeseries.len()
    }

    /// Return a mutable iterator over timeseries in the table.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Timeseries> {
        self.timeseries.values_mut()
    }

    /// Return an iterator over timeseries in the table.
    pub fn iter(&self) -> impl Iterator<Item = &Timeseries> {
        self.timeseries.values()
    }

    /// Consume the table and return an iterator over its timeseries.
    pub fn into_iter(self) -> impl Iterator<Item = Timeseries> {
        self.timeseries.into_values()
    }

    /// Return `true` if all the timeseries in this table are aligned, with the
    /// same alignment information.
    ///
    /// If there are no timeseries, `false` is returned.
    pub fn is_aligned(&self) -> bool {
        let mut timeseries = self.timeseries.values();
        let Some(t) = timeseries.next() else {
            return false;
        };
        let Some(alignment) = t.alignment else {
            return false;
        };
        timeseries.all(|t| t.alignment == Some(alignment))
    }

    /// Return the alignment of this table, if all timeseries are aligned with
    /// the same alignment.
    pub fn alignment(&self) -> Option<Alignment> {
        if self.is_aligned() {
            Some(
                self.timeseries.first_key_value().unwrap().1.alignment.unwrap(),
            )
        } else {
            None
        }
    }
}
