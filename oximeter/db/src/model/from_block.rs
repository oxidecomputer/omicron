// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Trait for deserializing an array of values from a `Block`.

use super::columns;
use crate::native::block::Block;
use crate::native::block::DataType;
use crate::native::block::ValueArray;
use crate::native::Error;
use chrono::TimeZone as _;
use oximeter::histogram::Histogram;
use oximeter::types::Cumulative;
use oximeter::types::MissingDatum;
use oximeter::AuthzScope;
use oximeter::Datum;
use oximeter::DatumType;
use oximeter::FieldSchema;
use oximeter::Measurement;
use oximeter::Quantile;
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
        let n_rows = block.n_rows();
        let mut out = Vec::with_capacity(n_rows);
        let ValueArray::String(timeseries_names) =
            block.column_values(columns::TIMESERIES_NAME)?
        else {
            return Err(Error::UnexpectedColumnType);
        };
        let ValueArray::Array {
            values: field_names,
            inner_type: DataType::String,
        } = block.column_values(columns::FIELDS_DOT_NAME)?
        else {
            return Err(Error::UnexpectedColumnType);
        };
        let ValueArray::Array {
            values: field_types,
            inner_type: DataType::Enum8(field_type_variants),
        } = block.column_values(columns::FIELDS_DOT_TYPE)?
        else {
            return Err(Error::UnexpectedColumnType);
        };
        let ValueArray::Array {
            values: field_sources,
            inner_type: DataType::Enum8(field_source_variants),
        } = block.column_values(columns::FIELDS_DOT_SOURCE)?
        else {
            return Err(Error::UnexpectedColumnType);
        };
        let ValueArray::Enum8 {
            variants: datum_type_variants,
            values: datum_types,
        } = block.column_values(columns::DATUM_TYPE)?
        else {
            return Err(Error::UnexpectedColumnType);
        };
        let ValueArray::DateTime64 { values: created, .. } =
            block.column_values(columns::CREATED)?
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

impl FromBlock for Measurement {
    fn from_block(block: &Block) -> Result<Vec<Self>, Error> {
        let data = Datum::from_block(block)?;
        let ValueArray::DateTime64 { values: timestamps, .. } =
            block.column_values(columns::TIMESTAMP)?
        else {
            return Err(Error::UnexpectedColumnType);
        };
        Ok(timestamps
            .iter()
            .zip(data)
            .map(|(timestamp, datum)| {
                let timestamp =
                    chrono::Utc.from_utc_datetime(&timestamp.naive_utc());
                Measurement::new(timestamp, datum)
            })
            .collect())
    }
}

impl FromBlock for Datum {
    fn from_block(block: &Block) -> Result<Vec<Self>, Error> {
        // Use the existence of various columns to figure out which kind of
        // datum we're extracting.
        //
        // The presence of a `datum` column implies a scalar, since histograms
        // have `bins` and `counts` columns instead.
        if let Ok(datum_col) = block.column_values(columns::DATUM) {
            // The presense of a `start_time` column implies a cumulative
            // scalar, and anything else is a gauge.
            if let Ok(start_time_col) = block.column_values(columns::START_TIME)
            {
                extract_cumulative_scalar_from_block(datum_col, start_time_col)
            } else {
                extract_gauge_from_block(datum_col)
            }
        } else {
            extract_histogram_from_block(block)
        }
    }
}

/// Extract the columns of a specified histogram datum from the provided block.
macro_rules! extract_histogram_columns {
    (
        $datum_type:path,
        $block:ident,
        $n_rows:ident,
        $start_time:ident,
        $bin_type:path,
        $bins:ident,
        $counts:ident,
        $sum_type:path,
        $squared_mean:ident,
        $p50_marker_heights:ident,
        $p50_marker_positions:ident,
        $p50_desired_marker_positions:ident,
        $p90_marker_heights:ident,
        $p90_marker_positions:ident,
        $p90_desired_marker_positions:ident,
        $p99_marker_heights:ident,
        $p99_marker_positions:ident,
        $p99_desired_marker_positions:ident
    ) => {
        let mut out = Vec::with_capacity($n_rows);
        let $bin_type(min) = $block.column_values(columns::MIN)? else {
            return Err(Error::UnexpectedColumnType)?;
        };
        let $bin_type(max) = $block.column_values(columns::MAX)? else {
            return Err(Error::UnexpectedColumnType)?;
        };
        let $sum_type(sum_of_samples) =
            $block.column_values(columns::SUM_OF_SAMPLES)?
        else {
            return Err(Error::UnexpectedColumnType)?;
        };
        for i in 0..$n_rows {
            let row_start_time =
                chrono::Utc.from_utc_datetime(&$start_time[i].naive_utc());

            // If there are zero bins in this row (an empty array), then this
            // represents a missing datum. Construct one with the right type,
            // and insert that instead.
            let $bin_type(row_bins) = &$bins[i] else {
                unreachable!();
            };
            if row_bins.is_empty() {
                let missing_datum =
                    MissingDatum::new($datum_type, Some(row_start_time))
                        .unwrap();
                out.push(Datum::from(missing_datum));
                continue;
            }

            let ValueArray::UInt64(row_counts) = &$counts[i] else {
                unreachable!();
            };
            let ValueArray::Float64(row_p50_heights) = &$p50_marker_heights[i]
            else {
                unreachable!();
            };
            let ValueArray::UInt64(row_p50_positions) =
                &$p50_marker_positions[i]
            else {
                unreachable!();
            };
            let ValueArray::Float64(row_p50_desired) =
                &$p50_desired_marker_positions[i]
            else {
                unreachable!();
            };
            let p50 = Quantile::from_parts(
                0.5,
                row_p50_heights.as_slice().try_into().unwrap(),
                row_p50_positions.as_slice().try_into().unwrap(),
                row_p50_desired.as_slice().try_into().unwrap(),
            );
            let ValueArray::Float64(row_p90_heights) = &$p90_marker_heights[i]
            else {
                unreachable!();
            };
            let ValueArray::UInt64(row_p90_positions) =
                &$p90_marker_positions[i]
            else {
                unreachable!();
            };
            let ValueArray::Float64(row_p90_desired) =
                &$p90_desired_marker_positions[i]
            else {
                unreachable!();
            };
            let p90 = Quantile::from_parts(
                0.9,
                row_p90_heights.as_slice().try_into().unwrap(),
                row_p90_positions.as_slice().try_into().unwrap(),
                row_p90_desired.as_slice().try_into().unwrap(),
            );
            let ValueArray::Float64(row_p99_heights) = &$p99_marker_heights[i]
            else {
                unreachable!();
            };
            let ValueArray::UInt64(row_p99_positions) =
                &$p99_marker_positions[i]
            else {
                unreachable!();
            };
            let ValueArray::Float64(row_p99_desired) =
                &$p99_desired_marker_positions[i]
            else {
                unreachable!();
            };
            let p99 = Quantile::from_parts(
                0.99,
                row_p99_heights.as_slice().try_into().unwrap(),
                row_p99_positions.as_slice().try_into().unwrap(),
                row_p99_desired.as_slice().try_into().unwrap(),
            );
            let hist = Histogram::from_parts(
                row_start_time,
                row_bins.clone(),
                row_counts.clone(),
                min[i],
                max[i],
                sum_of_samples[i],
                $squared_mean[i],
                p50,
                p90,
                p99,
            )
            .unwrap();
            out.push(Datum::from(hist));
        }
        return Ok(out);
    };
}

/// Extract a list of histogram `Datum`s from a data block.
///
/// This pulls out all the columns we expect to have in a histogram row, using
/// their extracted type to figure out which kind of histogram to build.
///
/// # Panics
///
/// This panics if the bins column doesn't have one of the supported histogram
/// types.
fn extract_histogram_from_block(block: &Block) -> Result<Vec<Datum>, Error> {
    // Extract the fields that all share the same type.
    let ValueArray::Array { inner_type: DataType::UInt64, values: counts } =
        block.column_values(columns::COUNTS)?
    else {
        return Err(Error::UnexpectedColumnType);
    };
    let ValueArray::Float64(squared_mean) =
        block.column_values(columns::SQUARED_MEAN)?
    else {
        return Err(Error::UnexpectedColumnType);
    };
    let ValueArray::Array {
        inner_type: DataType::Float64,
        values: p50_marker_heights,
    } = block.column_values(columns::P50_MARKER_HEIGHTS)?
    else {
        return Err(Error::UnexpectedColumnType);
    };
    let ValueArray::Array {
        inner_type: DataType::UInt64,
        values: p50_marker_positions,
    } = block.column_values(columns::P50_MARKER_POSITIONS)?
    else {
        return Err(Error::UnexpectedColumnType);
    };
    let ValueArray::Array {
        inner_type: DataType::Float64,
        values: p50_desired_marker_positions,
    } = block.column_values(columns::P50_DESIRED_MARKER_POSITIONS)?
    else {
        return Err(Error::UnexpectedColumnType);
    };
    let ValueArray::Array {
        inner_type: DataType::Float64,
        values: p90_marker_heights,
    } = block.column_values(columns::P90_MARKER_HEIGHTS)?
    else {
        return Err(Error::UnexpectedColumnType);
    };
    let ValueArray::Array {
        inner_type: DataType::UInt64,
        values: p90_marker_positions,
    } = block.column_values(columns::P90_MARKER_POSITIONS)?
    else {
        return Err(Error::UnexpectedColumnType);
    };
    let ValueArray::Array {
        inner_type: DataType::Float64,
        values: p90_desired_marker_positions,
    } = block.column_values(columns::P90_DESIRED_MARKER_POSITIONS)?
    else {
        return Err(Error::UnexpectedColumnType);
    };
    let ValueArray::Array {
        inner_type: DataType::Float64,
        values: p99_marker_heights,
    } = block.column_values(columns::P99_MARKER_HEIGHTS)?
    else {
        return Err(Error::UnexpectedColumnType);
    };
    let ValueArray::Array {
        inner_type: DataType::UInt64,
        values: p99_marker_positions,
    } = block.column_values(columns::P99_MARKER_POSITIONS)?
    else {
        return Err(Error::UnexpectedColumnType);
    };
    let ValueArray::Array {
        inner_type: DataType::Float64,
        values: p99_desired_marker_positions,
    } = block.column_values(columns::P99_DESIRED_MARKER_POSITIONS)?
    else {
        return Err(Error::UnexpectedColumnType);
    };
    let ValueArray::DateTime64 { values: start_time, .. } =
        block.column_values(columns::START_TIME)?
    else {
        return Err(Error::UnexpectedColumnType)?;
    };

    // Now extract the bins, from which we also learn the expected types of the
    // other columns.
    let ValueArray::Array { inner_type, values: bins } =
        block.column_values(columns::BINS)?
    else {
        return Err(Error::UnexpectedColumnType)?;
    };
    let n_rows = block.n_rows();
    match inner_type {
        DataType::UInt8 => {
            extract_histogram_columns!(
                DatumType::HistogramU8,
                block,
                n_rows,
                start_time,
                ValueArray::UInt8,
                bins,
                counts,
                ValueArray::Int64,
                squared_mean,
                p50_marker_heights,
                p50_marker_positions,
                p50_desired_marker_positions,
                p90_marker_heights,
                p90_marker_positions,
                p90_desired_marker_positions,
                p99_marker_heights,
                p99_marker_positions,
                p99_desired_marker_positions
            );
        }
        DataType::UInt16 => {
            extract_histogram_columns!(
                DatumType::HistogramU16,
                block,
                n_rows,
                start_time,
                ValueArray::UInt16,
                bins,
                counts,
                ValueArray::Int64,
                squared_mean,
                p50_marker_heights,
                p50_marker_positions,
                p50_desired_marker_positions,
                p90_marker_heights,
                p90_marker_positions,
                p90_desired_marker_positions,
                p99_marker_heights,
                p99_marker_positions,
                p99_desired_marker_positions
            );
        }
        DataType::UInt32 => {
            extract_histogram_columns!(
                DatumType::HistogramU32,
                block,
                n_rows,
                start_time,
                ValueArray::UInt32,
                bins,
                counts,
                ValueArray::Int64,
                squared_mean,
                p50_marker_heights,
                p50_marker_positions,
                p50_desired_marker_positions,
                p90_marker_heights,
                p90_marker_positions,
                p90_desired_marker_positions,
                p99_marker_heights,
                p99_marker_positions,
                p99_desired_marker_positions
            );
        }
        DataType::UInt64 => {
            extract_histogram_columns!(
                DatumType::HistogramU64,
                block,
                n_rows,
                start_time,
                ValueArray::UInt64,
                bins,
                counts,
                ValueArray::Int64,
                squared_mean,
                p50_marker_heights,
                p50_marker_positions,
                p50_desired_marker_positions,
                p90_marker_heights,
                p90_marker_positions,
                p90_desired_marker_positions,
                p99_marker_heights,
                p99_marker_positions,
                p99_desired_marker_positions
            );
        }
        DataType::Int8 => {
            extract_histogram_columns!(
                DatumType::HistogramI8,
                block,
                n_rows,
                start_time,
                ValueArray::Int8,
                bins,
                counts,
                ValueArray::Int64,
                squared_mean,
                p50_marker_heights,
                p50_marker_positions,
                p50_desired_marker_positions,
                p90_marker_heights,
                p90_marker_positions,
                p90_desired_marker_positions,
                p99_marker_heights,
                p99_marker_positions,
                p99_desired_marker_positions
            );
        }
        DataType::Int16 => {
            extract_histogram_columns!(
                DatumType::HistogramI16,
                block,
                n_rows,
                start_time,
                ValueArray::Int16,
                bins,
                counts,
                ValueArray::Int64,
                squared_mean,
                p50_marker_heights,
                p50_marker_positions,
                p50_desired_marker_positions,
                p90_marker_heights,
                p90_marker_positions,
                p90_desired_marker_positions,
                p99_marker_heights,
                p99_marker_positions,
                p99_desired_marker_positions
            );
        }
        DataType::Int32 => {
            extract_histogram_columns!(
                DatumType::HistogramI32,
                block,
                n_rows,
                start_time,
                ValueArray::Int32,
                bins,
                counts,
                ValueArray::Int64,
                squared_mean,
                p50_marker_heights,
                p50_marker_positions,
                p50_desired_marker_positions,
                p90_marker_heights,
                p90_marker_positions,
                p90_desired_marker_positions,
                p99_marker_heights,
                p99_marker_positions,
                p99_desired_marker_positions
            );
        }
        DataType::Int64 => {
            extract_histogram_columns!(
                DatumType::HistogramI64,
                block,
                n_rows,
                start_time,
                ValueArray::Int64,
                bins,
                counts,
                ValueArray::Int64,
                squared_mean,
                p50_marker_heights,
                p50_marker_positions,
                p50_desired_marker_positions,
                p90_marker_heights,
                p90_marker_positions,
                p90_desired_marker_positions,
                p99_marker_heights,
                p99_marker_positions,
                p99_desired_marker_positions
            );
        }
        DataType::Float32 => {
            extract_histogram_columns!(
                DatumType::HistogramF32,
                block,
                n_rows,
                start_time,
                ValueArray::Float32,
                bins,
                counts,
                ValueArray::Float64,
                squared_mean,
                p50_marker_heights,
                p50_marker_positions,
                p50_desired_marker_positions,
                p90_marker_heights,
                p90_marker_positions,
                p90_desired_marker_positions,
                p99_marker_heights,
                p99_marker_positions,
                p99_desired_marker_positions
            );
        }
        DataType::Float64 => {
            extract_histogram_columns!(
                DatumType::HistogramF64,
                block,
                n_rows,
                start_time,
                ValueArray::Float64,
                bins,
                counts,
                ValueArray::Float64,
                squared_mean,
                p50_marker_heights,
                p50_marker_positions,
                p50_desired_marker_positions,
                p90_marker_heights,
                p90_marker_positions,
                p90_desired_marker_positions,
                p99_marker_heights,
                p99_marker_positions,
                p99_desired_marker_positions
            );
        }
        _ => unreachable!(),
    }
}

/// Helper macro to pull out a nullable, copyable datum into an array.
macro_rules! extract_copyable_gauge_datum {
    ($data:ident, $is_null:ident, $values:ident, $datum_type:path) => {
        for (null, value) in $is_null.iter().zip($values) {
            let datum = if *null {
                Datum::Missing(MissingDatum::new($datum_type, None).unwrap())
            } else {
                Datum::from(*value)
            };
            $data.push(datum);
        }
    };
}

/// Extract an array of gauge samples from a block.
///
/// # Panics
///
/// This panics if the datum is not nullable (all datum columns in the oximeter
/// database should be nullable), or if the type is not a supported gauge.
fn extract_gauge_from_block(
    datum_col: &ValueArray,
) -> Result<Vec<Datum>, Error> {
    let ValueArray::Nullable { is_null, values } = datum_col else {
        // The only non-nullable "scalar" we support is a byte array, so try to
        // deserialize that.
        let ValueArray::Array { inner_type: DataType::UInt8, values } =
            datum_col
        else {
            unreachable!()
        };
        assert_eq!(values.len(), 1);
        let ValueArray::UInt8(bytes) = &values[0] else {
            unreachable!();
        };
        return Ok(vec![Datum::from(bytes.as_slice())]);
    };
    let mut data = Vec::with_capacity(values.len());
    match &**values {
        ValueArray::Bool(values) => {
            extract_copyable_gauge_datum!(
                data,
                is_null,
                values,
                DatumType::Bool
            );
        }
        ValueArray::UInt8(values) => {
            extract_copyable_gauge_datum!(data, is_null, values, DatumType::U8);
        }
        ValueArray::UInt16(values) => {
            extract_copyable_gauge_datum!(
                data,
                is_null,
                values,
                DatumType::U16
            );
        }
        ValueArray::UInt32(values) => {
            extract_copyable_gauge_datum!(
                data,
                is_null,
                values,
                DatumType::U32
            );
        }
        ValueArray::UInt64(values) => {
            extract_copyable_gauge_datum!(
                data,
                is_null,
                values,
                DatumType::U64
            );
        }
        ValueArray::Int8(values) => {
            extract_copyable_gauge_datum!(data, is_null, values, DatumType::I8);
        }
        ValueArray::Int16(values) => {
            extract_copyable_gauge_datum!(
                data,
                is_null,
                values,
                DatumType::I16
            );
        }
        ValueArray::Int32(values) => {
            extract_copyable_gauge_datum!(
                data,
                is_null,
                values,
                DatumType::I32
            );
        }
        ValueArray::Int64(values) => {
            extract_copyable_gauge_datum!(
                data,
                is_null,
                values,
                DatumType::I64
            );
        }
        ValueArray::Float32(values) => {
            extract_copyable_gauge_datum!(
                data,
                is_null,
                values,
                DatumType::F32
            );
        }
        ValueArray::Float64(values) => {
            extract_copyable_gauge_datum!(
                data,
                is_null,
                values,
                DatumType::F64
            );
        }
        ValueArray::String(values) => {
            for (null, value) in is_null.iter().zip(values) {
                let datum = if *null {
                    Datum::Missing(
                        MissingDatum::new(DatumType::String, None).unwrap(),
                    )
                } else {
                    Datum::from(value.clone())
                };
                data.push(datum);
            }
        }
        ValueArray::Array { inner_type: DataType::UInt8, values } => {
            for (null, value) in is_null.iter().zip(values) {
                let ValueArray::UInt8(bytes) = value else { unreachable!() };
                assert!(
                    !*null,
                    "Missing byte array samples are not yet supported"
                );
                data.push(Datum::from(bytes.as_slice()))
            }
        }
        _ => unreachable!(),
    }
    Ok(data)
}

/// Helper macro to extract an array of cumulative scalars.
macro_rules! extract_cumulative_scalar {
    (
        $start_times:ident,
        $data:ident,
        $is_null:ident,
        $values:ident,
        $datum_type:path
    ) => {
        for (start_time, (null, value)) in $start_times
            .iter()
            .map(|t| chrono::Utc.from_utc_datetime(&t.naive_utc()))
            .zip($is_null.iter().zip($values))
        {
            let datum = if *null {
                Datum::Missing(
                    MissingDatum::new($datum_type, Some(start_time)).unwrap(),
                )
            } else {
                Datum::from(Cumulative::with_start_time(start_time, *value))
            };
            $data.push(datum);
        }
    };
}

/// Helper to extract a cumulative scalar from a block, using the provided start
/// time column.
///
/// # Panics
///
/// This panics if the arguments don't have the same length, or if the values
/// are not one of the supported cumulative types.
fn extract_cumulative_scalar_from_block(
    datum_col: &ValueArray,
    start_time_col: &ValueArray,
) -> Result<Vec<Datum>, Error> {
    assert_eq!(datum_col.len(), start_time_col.len());
    let ValueArray::DateTime64 { values: start_times, .. } = start_time_col
    else {
        return Err(Error::UnexpectedColumnType);
    };
    let ValueArray::Nullable { is_null, values } = datum_col else {
        return Err(Error::UnexpectedColumnType);
    };
    let mut data = Vec::with_capacity(values.len());
    match &**values {
        ValueArray::UInt64(values) => {
            extract_cumulative_scalar!(
                start_times,
                data,
                is_null,
                values,
                DatumType::CumulativeU64
            );
        }
        ValueArray::Int64(values) => {
            extract_cumulative_scalar!(
                start_times,
                data,
                is_null,
                values,
                DatumType::CumulativeI64
            );
        }
        ValueArray::Float32(values) => {
            extract_cumulative_scalar!(
                start_times,
                data,
                is_null,
                values,
                DatumType::CumulativeF32
            );
        }
        ValueArray::Float64(values) => {
            extract_cumulative_scalar!(
                start_times,
                data,
                is_null,
                values,
                DatumType::CumulativeF64
            );
        }
        _ => unreachable!(),
    }
    Ok(data)
}
