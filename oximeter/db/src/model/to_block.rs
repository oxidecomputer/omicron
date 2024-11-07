// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Trait for serializing an array of values into a `Block`.

use crate::native::block::Block;
use crate::native::block::Column;
use crate::native::block::DataType;
use crate::native::block::Precision;
use crate::native::block::ValueArray;
use crate::native::Error;
use chrono::TimeZone as _;
use chrono_tz::Tz;
use indexmap::IndexMap;
use oximeter::TimeseriesSchema;

include!(concat!(env!("OUT_DIR"), "/enum_defs.rs"));

/// Trait for serializing an array of items to a ClickHouse data block.
pub trait ToBlock: Sized {
    /// Serialize an array of `Self`s to a block.
    fn to_block(items: &[Self]) -> Result<Block, Error>;
}

// TODO-cleanup: This is probably a good candidate for a derive-macro, which
// expands to the code that checks that names / types in the block match those
// of the fields in the struct itself.
impl ToBlock for TimeseriesSchema {
    fn to_block(items: &[Self]) -> Result<Block, Error> {
        let n_items = items.len();
        let mut timeseries_names = Vec::with_capacity(n_items);
        let mut field_names = Vec::with_capacity(n_items);
        let mut field_types = Vec::with_capacity(n_items);
        let mut field_sources = Vec::with_capacity(n_items);
        let mut datum_types = Vec::with_capacity(n_items);
        let mut created = Vec::with_capacity(n_items);
        for item in items.iter() {
            timeseries_names.push(item.timeseries_name.to_string());
            let n_fields = item.field_schema.len();
            let mut row_field_names = Vec::with_capacity(n_fields);
            let mut row_field_types = Vec::with_capacity(n_fields);
            let mut row_field_sources = Vec::with_capacity(n_fields);
            for field in item.field_schema.iter() {
                row_field_names.push(field.name.clone());
                let ty = TYPE_ENUM_REV_MAP.get(&field.field_type).unwrap();
                row_field_types.push(*ty);
                let src = SOURCE_ENUM_REV_MAP.get(&field.source).unwrap();
                row_field_sources.push(*src);
            }
            field_names.push(ValueArray::String(row_field_names));
            field_types.push(ValueArray::Enum8 {
                variants: TYPE_ENUM_MAP.clone(),
                values: row_field_types,
            });
            field_sources.push(ValueArray::Enum8 {
                variants: SOURCE_ENUM_MAP.clone(),
                values: row_field_sources,
            });
            datum_types
                .push(*DATUM_TYPE_ENUM_REV_MAP.get(&item.datum_type).unwrap());
            created.push(Tz::UTC.from_utc_datetime(&item.created.naive_utc()));
        }
        Ok(Block {
            name: String::new(),
            info: Default::default(),
            n_columns: 6,
            n_rows: u64::try_from(n_items).map_err(|_| Error::BlockTooLarge)?,
            columns: IndexMap::from([
                (
                    String::from("timeseries_name"),
                    Column::from(ValueArray::String(timeseries_names)),
                ),
                (
                    String::from("fields.name"),
                    Column::from(ValueArray::Array {
                        inner_type: DataType::String,
                        values: field_names,
                    }),
                ),
                (
                    String::from("fields.type"),
                    Column::from(ValueArray::Array {
                        inner_type: TYPE_ENUM_DATA_TYPE.clone(),
                        values: field_types,
                    }),
                ),
                (
                    String::from("fields.source"),
                    Column::from(ValueArray::Array {
                        inner_type: SOURCE_ENUM_DATA_TYPE.clone(),
                        values: field_sources,
                    }),
                ),
                (
                    String::from("datum_type"),
                    Column::from(ValueArray::Enum8 {
                        variants: DATUM_TYPE_ENUM_MAP.clone(),
                        values: datum_types,
                    }),
                ),
                (
                    String::from("created"),
                    Column::from(ValueArray::DateTime64 {
                        values: created,
                        precision: Precision::new(9).unwrap(),
                        tz: Tz::UTC,
                    }),
                ),
            ]),
        })
    }
}
