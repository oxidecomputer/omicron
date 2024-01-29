// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The OxQL AST.

// Copyright 2024 Oxide Computer Company

/*
impl TableTransformation for Filter {
    fn apply(&self, tables: &[Table]) -> Result<Vec<Table>, Error> {
        if tables.len() != 1 {
            return Err(Error::InvalidTableCount {
                op: "filter",
                expected: 1,
                found: tables.len(),
            });
        }
        let table = tables.get(0).unwrap();
        let mut out = Vec::with_capacity(table.timeseries.len());
        'timeseries: for timeseries in table.timeseries.iter() {
            // Apply the filter to all the fields of the timeseries.
            for (name, value) in timeseries.fields.iter() {
                match self.filter_field(name, value) {
                    Some(true) => continue,
                    Some(false) => break 'timeseries,
                    None => {
                        return Err(Error::InvalidFieldType {
                            field_name: name.to_string(),
                            expected: value.field_type(),
                        })
                    }
                }
            }

            // And also to the measurements.
            let new_values = self.filter_values(&timeseries.values)?;
            out.push(Timeseries {
                fields: timeseries.fields.clone(),
                values: new_values,
            });
        }
        Ok(vec![Table { timeseries: out }])
    }
}
*/

