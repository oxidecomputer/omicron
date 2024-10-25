// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Trait for deserializing an array of values from a `Block`.

use crate::native::block::Block;
use crate::native::block::DataType;
use crate::native::block::ValueArray;
use crate::native::Error;
use oximeter::AuthzScope;
use oximeter::FieldSchema;
use oximeter::TimeseriesDescription;
use oximeter::TimeseriesSchema;
use oximeter::Units;
use std::collections::BTreeSet;
use std::num::NonZeroU8;

/// Trait for deserializing an array of items from a ClickHouse data block.
pub trait FromBlock: Sized {
    /// Deserialize an array of `Self`s from a block.
    fn from_block(block: &Block) -> Result<Vec<Self>, Error>;
}

// TODO-cleanup: This is probably a good candidate for a derive-macro, which
// expands to the code that checks that names / types in the block match those
// of the fields in the struct itself.
impl FromBlock for TimeseriesSchema {
    fn from_block(block: &Block) -> Result<Vec<Self>, Error> {
        if block.is_empty() {
            return Ok(vec![]);
        }
        let n_rows =
            usize::try_from(block.n_rows).map_err(|_| Error::BlockTooLarge)?;
        let mut out = Vec::with_capacity(n_rows);
        let ValueArray::String(timeseries_names) =
            block.column_values("timeseries_name")?
        else {
            return Err(Error::UnexpectedColumnType);
        };
        let ValueArray::Array {
            values: field_names,
            inner_type: DataType::String,
        } = block.column_values("fields.name")?
        else {
            return Err(Error::UnexpectedColumnType);
        };
        let ValueArray::Array {
            values: field_types,
            inner_type: DataType::Enum8(field_type_variants),
        } = block.column_values("fields.type")?
        else {
            return Err(Error::UnexpectedColumnType);
        };
        let ValueArray::Array {
            values: field_sources,
            inner_type: DataType::Enum8(field_source_variants),
        } = block.column_values("fields.source")?
        else {
            return Err(Error::UnexpectedColumnType);
        };
        let ValueArray::Enum8 {
            variants: datum_type_variants,
            values: datum_types,
        } = block.column_values("datum_type")?
        else {
            return Err(Error::UnexpectedColumnType);
        };
        let ValueArray::DateTime64 { values: created, .. } =
            block.column_values("created")?
        else {
            return Err(Error::UnexpectedColumnType);
        };

        for row in 0..n_rows {
            let ValueArray::String(names) = &field_names[row] else {
                unreachable!();
            };
            let ValueArray::Enum8 { values: row_field_types, .. } =
                &field_types[row]
            else {
                unreachable!();
            };
            let ValueArray::Enum8 { values: row_field_sources, .. } =
                &field_sources[row]
            else {
                unreachable!();
            };
            let mut field_schema = BTreeSet::new();
            let n_fields = names.len();
            for field in 0..n_fields {
                let schema = FieldSchema {
                    name: names[field].clone(),
                    field_type: field_type_variants[&row_field_types[field]]
                        .parse()
                        .map_err(|_| {
                            Error::Serde(format!(
                                "Failed to deserialize field type from database: {:?}",
                                field_type_variants[&row_field_types[field]]
                            ))
                        })?,
                    source: field_source_variants[&row_field_sources[field]]
                        .parse()
                        .map_err(|_| {
                            Error::Serde(format!(
                                "Failed to deserialize field source from database: {:?}",
                                field_source_variants[&row_field_sources[field]]))
                        })?,
                    description: String::new(),
                };
                field_schema.insert(schema);
            }
            let schema = TimeseriesSchema {
                timeseries_name:
                    timeseries_names[row].clone().parse().map_err(|_| {
                        Error::Serde(format!(
                            "Failed to deserialize timeseries name from database: {:?}",
                            &timeseries_names[row]
                        ))
                    })?,
                description: TimeseriesDescription::default(),
                field_schema,
                datum_type: datum_type_variants[&datum_types[row]]
                    .parse()
                    .map_err(|_| {
                        Error::Serde(format!(
                            "Failed to deserialize datum type from database: {:?}",
                            &datum_type_variants[&datum_types[row]]
                        ))
                    })?,
                version: unsafe { NonZeroU8::new_unchecked(1) },
                authz_scope: AuthzScope::Fleet,
                units: Units::None,
                created: created[row].to_utc(),
            };
            out.push(schema);
        }
        Ok(out)
    }
}
