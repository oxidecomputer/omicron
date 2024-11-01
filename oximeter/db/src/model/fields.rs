// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Methods for reading / writing oximeter fields to the database.

// Copyright 2024 Oxide Computer Company

use crate::native::block::Block;
use crate::native::block::Column;
use crate::native::block::DataType;
use crate::native::block::ValueArray;
use crate::query::field_table_name;
use indexmap::IndexMap;
use oximeter::FieldType;
use oximeter::FieldValue;
use oximeter::Sample;
use std::collections::BTreeMap;

/// Extract `Block`s for all fields in a `Sample`.
///
/// This returns a data block for each field table, which can be inserted into
/// the database.
pub(crate) fn extract_fields_as_block(
    sample: &Sample,
) -> BTreeMap<String, Block> {
    let mut out = BTreeMap::new();
    let timeseries_key = crate::timeseries_key(sample);
    for field in sample.fields() {
        let field_type = field.value.field_type();
        let table_name = field_table_name(field_type);
        let entry = out.entry(table_name).or_insert_with(|| Block {
            name: String::new(),
            info: Default::default(),
            columns: empty_columns_for_field(field_type),
        });

        // Push the timeseries name, key, and field name.
        let Ok(ValueArray::String(timeseries_names)) =
            entry.column_values_mut("timeseries_name")
        else {
            unreachable!();
        };
        timeseries_names.push(sample.timeseries_name.to_string());
        let Ok(ValueArray::UInt64(keys)) =
            entry.column_values_mut("timeseries_key")
        else {
            unreachable!();
        };
        keys.push(timeseries_key);
        let Ok(ValueArray::String(field_names)) =
            entry.column_values_mut("field_name")
        else {
            unreachable!();
        };
        field_names.push(field.name.clone());

        // Push the field value, which depends on the type.
        let values = entry.column_values_mut("field_value").unwrap();
        match (field.value, values) {
            (FieldValue::String(x), ValueArray::String(values)) => {
                values.push(x.to_string())
            }
            (FieldValue::I8(x), ValueArray::Int8(values)) => values.push(x),
            (FieldValue::U8(x), ValueArray::UInt8(values)) => values.push(x),
            (FieldValue::I16(x), ValueArray::Int16(values)) => values.push(x),
            (FieldValue::U16(x), ValueArray::UInt16(values)) => values.push(x),
            (FieldValue::I32(x), ValueArray::Int32(values)) => values.push(x),
            (FieldValue::U32(x), ValueArray::UInt32(values)) => values.push(x),
            (FieldValue::I64(x), ValueArray::Int64(values)) => values.push(x),
            (FieldValue::U64(x), ValueArray::UInt64(values)) => values.push(x),
            (FieldValue::IpAddr(x), ValueArray::Ipv6(values)) => {
                let addr = match x {
                    std::net::IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                    std::net::IpAddr::V6(v6) => v6,
                };
                values.push(addr);
            }
            (FieldValue::Uuid(x), ValueArray::Uuid(values)) => values.push(x),
            (FieldValue::Bool(x), ValueArray::Bool(values)) => values.push(x),
            (_, _) => unreachable!(),
        }
    }
    out
}

/// Construct an empty set of columns for a field table of the given type.
fn empty_columns_for_field(field_type: FieldType) -> IndexMap<String, Column> {
    IndexMap::from([
        (
            String::from("timeseries_name"),
            Column::from(ValueArray::empty(&DataType::String)),
        ),
        (
            String::from("timeseries_key"),
            Column::from(ValueArray::empty(&DataType::UInt64)),
        ),
        (
            String::from("field_name"),
            Column::from(ValueArray::empty(&DataType::String)),
        ),
        (
            String::from("field_value"),
            Column::from(ValueArray::empty(&DataType::from(field_type))),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::extract_fields_as_block;
    use crate::native::block::ValueArray;
    use oximeter::Sample;

    #[derive(oximeter::Target)]
    struct SomeTarget {
        name: String,
        id: uuid::Uuid,
        other_name: String,
    }

    #[derive(oximeter::Metric, Default)]
    struct SomeMetric {
        yet_another_name: String,
        datum: u64,
    }

    #[test]
    fn test_extract_fields_as_block() {
        let t = SomeTarget {
            name: String::from("bill"),
            id: uuid::Uuid::new_v4(),
            other_name: String::from("ted"),
        };
        let m = SomeMetric { yet_another_name: String::from("tim"), datum: 0 };
        let sample = Sample::new(&t, &m).unwrap();
        let blocks = extract_fields_as_block(&sample);
        assert_eq!(blocks.len(), 2, "Should extract blocks for 2 field tables");
        let block = blocks
            .get("fields_string")
            .expect("Should have created a block for the fields_string table");
        assert_eq!(
            block.n_columns(),
            4,
            "Blocks for the field tables should list each column in the table schema",
        );
        assert_eq!(
            block.n_rows(),
            3,
            "Should have extracted 3 rows for the string field table"
        );

        let strings = block
            .column_values("field_value")
            .expect("Should have a column named `field_value`");
        let ValueArray::String(strings) = strings else {
            panic!("Expected an array of strings, found: {strings:?}");
        };
        let mut strings = strings.clone();
        strings.sort();
        assert_eq!(
            strings,
            &["bill", "ted", "tim"],
            "Incorrect field values for the string fields"
        );

        let block = blocks
            .get("fields_uuid")
            .expect("Should have created a block for the fields_uuid table");
        assert_eq!(
            block.n_columns(),
            4,
            "Blocks for the field tables should list each column in the table schema",
        );
        assert_eq!(
            block.n_rows(),
            1,
            "Should have extracted 1 row for the UUID field table"
        );
        let ids = block
            .column_values("field_value")
            .expect("Should have a column named `field_value`");
        let ValueArray::Uuid(ids) = ids else {
            panic!("Expected an array of strings, found: {ids:?}");
        };
        assert_eq!(ids, &[t.id], "Incorrect field values for the UUID fields");
    }
}
