// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Methods for reading / writing oximeter measurements to the database.

// Copyright 2024 Oxide Computer Company

use super::columns;
use crate::model::columns::Quantile;
use crate::native::block::Block;
use crate::native::block::Column;
use crate::native::block::DataType;
use crate::native::block::Precision;
use crate::native::block::ValueArray;
use crate::query::measurement_table_name;
use chrono::TimeZone as _;
use chrono_tz::Tz;
use indexmap::IndexMap;
use oximeter::Datum;
use oximeter::DatumType;
use oximeter::Sample;
use oximeter::histogram::Histogram;
use oximeter::traits::HistogramSupport;
use oximeter::types::MissingDatum;
use strum::IntoEnumIterator as _;

/// Extract the table name and a data block for a sample's measurement.
pub fn extract_measurement_as_block(sample: &Sample) -> (String, Block) {
    // TODO-performance: We're sure computing this key a lot of times. That's
    // probably pretty expensive, since it involves hashing and sorting the
    // fields into an allocated BTreeMap. We should avoid doing that.
    let timeseries_key = crate::timeseries_key(sample);
    let timeseries_name = sample.timeseries_name.to_string();
    extract_measurement_as_block_impl(
        timeseries_name,
        timeseries_key,
        &sample.measurement,
    )
}

/// Internal implmentation, factored out for testing.
pub(crate) fn extract_measurement_as_block_impl(
    timeseries_name: String,
    timeseries_key: u64,
    measurement: &oximeter::Measurement,
) -> (String, Block) {
    // Construct the column arrays for those columns shared by all tables.
    let mut columns =
        IndexMap::from([
            (
                String::from(columns::TIMESERIES_NAME),
                Column::from(ValueArray::from(vec![timeseries_name])),
            ),
            (
                String::from(columns::TIMESERIES_KEY),
                Column::from(ValueArray::from(vec![timeseries_key])),
            ),
            (
                String::from(columns::TIMESTAMP),
                Column::from(ValueArray::DateTime64 {
                    precision: Precision::MAX,
                    tz: Tz::UTC,
                    values: vec![Tz::UTC.from_utc_datetime(
                        &measurement.timestamp().naive_utc(),
                    )],
                }),
            ),
        ]);

    // Insert an array of start times if needed.
    if let Some(start_time) = measurement.start_time() {
        columns.insert(
            String::from(columns::START_TIME),
            Column::from(ValueArray::DateTime64 {
                precision: Precision::MAX,
                tz: Tz::UTC,
                values: vec![
                    Tz::UTC.from_utc_datetime(&start_time.naive_utc()),
                ],
            }),
        );
    }

    // Insert the datum array(s).
    //
    // There may be more than one for the histogram tables.
    insert_datum_columns(measurement.datum(), &mut columns);

    let block =
        Block { name: String::new(), info: Default::default(), columns };
    (measurement_table_name(measurement.datum_type()), block)
}

/// Helper macro to insert the columns for a missing histogram.
///
/// These have a particular structure. For scalar columns, the entries are all
/// zero. For _array_ columns, the entries are all empty arrays.
macro_rules! insert_missing_histogram {
    ($columns:ident, $data_type:path, $zero:literal, $sum:literal) => {
        $columns.insert(
            String::from(columns::BINS),
            Column::from(ValueArray::Array {
                inner_type: $data_type,
                values: vec![ValueArray::empty(&$data_type)],
            }),
        );
        $columns.insert(
            String::from(columns::MIN),
            Column::from(ValueArray::from(vec![$zero])),
        );
        $columns.insert(
            String::from(columns::MAX),
            Column::from(ValueArray::from(vec![$zero])),
        );
        $columns.insert(
            String::from(columns::SUM_OF_SAMPLES),
            Column::from(ValueArray::from(vec![$sum])),
        );
    };
}

/// Insert a possibly nullable datum into the provided columns.
macro_rules! insert_datum_inner {
    ($columns:ident, $missing:literal, $datum:expr) => {
        $columns.insert(
            String::from(columns::DATUM),
            Column::from(ValueArray::Nullable {
                is_null: vec![$missing],
                values: Box::new(ValueArray::from(vec![$datum])),
            }),
        );
    };
}

/// Insert a present datum into the columns.
macro_rules! insert_datum {
    ($columns:ident, $datum:expr) => {
        insert_datum_inner!($columns, false, $datum)
    };
}

/// Insert a missing datum into the columns.
macro_rules! insert_missing_datum {
    ($columns:ident, $datum:expr) => {
        insert_datum_inner!($columns, true, $datum)
    };
}

/// Insert the measurement column(s) for a provided measurement.
fn insert_datum_columns(datum: &Datum, columns: &mut IndexMap<String, Column>) {
    match datum {
        Datum::Bool(x) => {
            insert_datum!(columns, *x);
        }
        Datum::I8(x) => {
            insert_datum!(columns, *x);
        }
        Datum::U8(x) => {
            insert_datum!(columns, *x);
        }
        Datum::I16(x) => {
            insert_datum!(columns, *x);
        }
        Datum::U16(x) => {
            insert_datum!(columns, *x);
        }
        Datum::I32(x) => {
            insert_datum!(columns, *x);
        }
        Datum::U32(x) => {
            insert_datum!(columns, *x);
        }
        Datum::I64(x) => {
            insert_datum!(columns, *x);
        }
        Datum::U64(x) => {
            insert_datum!(columns, *x);
        }
        Datum::F32(x) => {
            insert_datum!(columns, *x);
        }
        Datum::F64(x) => {
            insert_datum!(columns, *x);
        }
        Datum::String(x) => {
            insert_datum!(columns, x.clone());
        }
        Datum::Bytes(x) => {
            let values = ValueArray::Array {
                inner_type: DataType::UInt8,
                values: vec![ValueArray::from(x.to_vec())],
            };
            let data_type = values.data_type();
            let column = Column { values, data_type };
            columns.insert(String::from(columns::DATUM), column);
        }
        Datum::CumulativeI64(x) => {
            insert_datum!(columns, x.value());
        }
        Datum::CumulativeU64(x) => {
            insert_datum!(columns, x.value());
        }
        Datum::CumulativeF32(x) => {
            insert_datum!(columns, x.value());
        }
        Datum::CumulativeF64(x) => {
            insert_datum!(columns, x.value());
        }
        Datum::HistogramI8(hist) => {
            columns.extend(build_histogram_measurement_columns(hist));
        }
        Datum::HistogramU8(hist) => {
            columns.extend(build_histogram_measurement_columns(hist));
        }
        Datum::HistogramI16(hist) => {
            columns.extend(build_histogram_measurement_columns(hist));
        }
        Datum::HistogramU16(hist) => {
            columns.extend(build_histogram_measurement_columns(hist));
        }
        Datum::HistogramI32(hist) => {
            columns.extend(build_histogram_measurement_columns(hist));
        }
        Datum::HistogramU32(hist) => {
            columns.extend(build_histogram_measurement_columns(hist));
        }
        Datum::HistogramI64(hist) => {
            columns.extend(build_histogram_measurement_columns(hist));
        }
        Datum::HistogramU64(hist) => {
            columns.extend(build_histogram_measurement_columns(hist));
        }
        Datum::HistogramF32(hist) => {
            columns.extend(build_histogram_measurement_columns(hist));
        }
        Datum::HistogramF64(hist) => {
            columns.extend(build_histogram_measurement_columns(hist));
        }
        Datum::Missing(missing) => {
            columns.extend(build_missing_measurement_columns(missing));
        }
    }
}

/// Build the measurement columns for a missing datum.
fn build_missing_measurement_columns(
    missing: &MissingDatum,
) -> impl Iterator<Item = (String, Column)> + use<> {
    let mut columns = IndexMap::new();
    match missing.datum_type() {
        DatumType::Bool => {
            insert_missing_datum!(columns, false);
        }
        DatumType::I8 => {
            insert_missing_datum!(columns, 0i8);
        }
        DatumType::U8 => {
            insert_missing_datum!(columns, 0u8);
        }
        DatumType::I16 => {
            insert_missing_datum!(columns, 0i16);
        }
        DatumType::U16 => {
            insert_missing_datum!(columns, 0u16);
        }
        DatumType::I32 => {
            insert_missing_datum!(columns, 0i32);
        }
        DatumType::U32 => {
            insert_missing_datum!(columns, 0u32);
        }
        DatumType::I64 | DatumType::CumulativeI64 => {
            insert_missing_datum!(columns, 0i64);
        }
        DatumType::U64 | DatumType::CumulativeU64 => {
            insert_missing_datum!(columns, 0u64);
        }
        DatumType::F32 | DatumType::CumulativeF32 => {
            insert_missing_datum!(columns, 0f32);
        }
        DatumType::F64 | DatumType::CumulativeF64 => {
            insert_missing_datum!(columns, 0f64);
        }
        DatumType::String => {
            insert_missing_datum!(columns, String::new());
        }
        DatumType::Bytes => {
            unimplemented!("Missing byte samples not supported")
        }
        DatumType::HistogramI8 => {
            insert_missing_histogram!(columns, DataType::Int8, 0i8, 0i64);
        }
        DatumType::HistogramU8 => {
            insert_missing_histogram!(columns, DataType::UInt8, 0u8, 0i64);
        }
        DatumType::HistogramI16 => {
            insert_missing_histogram!(columns, DataType::Int16, 0i16, 0i64);
        }
        DatumType::HistogramU16 => {
            insert_missing_histogram!(columns, DataType::UInt16, 0u16, 0i64);
        }
        DatumType::HistogramI32 => {
            insert_missing_histogram!(columns, DataType::Int32, 0i32, 0i64);
        }
        DatumType::HistogramU32 => {
            insert_missing_histogram!(columns, DataType::UInt32, 0u32, 0i64);
        }
        DatumType::HistogramI64 => {
            insert_missing_histogram!(columns, DataType::Int64, 0i64, 0i64);
        }
        DatumType::HistogramU64 => {
            insert_missing_histogram!(columns, DataType::UInt64, 0u64, 0i64);
        }
        DatumType::HistogramF32 => {
            insert_missing_histogram!(columns, DataType::Float32, 0f32, 0f64);
        }
        DatumType::HistogramF64 => {
            insert_missing_histogram!(columns, DataType::Float64, 0f64, 0f64);
        }
    }

    // Insert empty histogram columns, if applicable.
    //
    // These columns all have the same type, so we don't need to know anything
    // beyond the fact that this is a missing histogram.
    if missing.datum_type().is_histogram() {
        columns.insert(
            String::from(columns::COUNTS),
            Column::from(ValueArray::Array {
                inner_type: DataType::UInt64,
                values: vec![ValueArray::UInt64(vec![])],
            }),
        );
        columns.insert(
            String::from(columns::SQUARED_MEAN),
            Column::from(ValueArray::Float64(vec![0f64])),
        );
        for p in Quantile::iter() {
            columns.insert(
                p.marker_heights().to_string(),
                Column::from(ValueArray::Array {
                    inner_type: DataType::Float64,
                    values: vec![ValueArray::Float64(vec![])],
                }),
            );
            columns.insert(
                p.marker_positions().to_string(),
                Column::from(ValueArray::Array {
                    inner_type: DataType::UInt64,
                    values: vec![ValueArray::UInt64(vec![])],
                }),
            );
            columns.insert(
                p.desired_marker_positions().to_string(),
                Column::from(ValueArray::Array {
                    inner_type: DataType::Float64,
                    values: vec![ValueArray::Float64(vec![])],
                }),
            );
        }
    }

    // Insert the start time, if it exists.
    //
    // This is present for cumulative counters and histograms.
    if let Some(start_time) = missing.start_time() {
        let values = ValueArray::DateTime64 {
            precision: Precision::MAX,
            tz: Tz::UTC,
            values: vec![Tz::UTC.from_utc_datetime(&start_time.naive_utc())],
        };
        columns.insert(String::from(columns::START_TIME), Column::from(values));
    }
    columns.into_iter()
}

/// Helper trait to convert the data type of a histogram's bin into the right
/// type of a `ValueArray`.
trait ConvertBinType: HistogramSupport {
    /// The ClickHouse `DataType` for this histogram type.
    const DATA_TYPE: DataType;

    /// Convert an array of bins to a `ValueArray`.
    fn self_as_value_array(x: Vec<Self>) -> ValueArray;

    /// Convert an array of histogram width types to a `ValueArray`.
    fn width_as_value_array(x: Vec<Self::Width>) -> ValueArray;
}

macro_rules! impl_convert_bin_type {
    ($bin_type:ty, $data_type:path) => {
        impl ConvertBinType for $bin_type {
            const DATA_TYPE: DataType = $data_type;
            fn self_as_value_array(x: Vec<$bin_type>) -> ValueArray {
                ValueArray::from(x)
            }
            fn width_as_value_array(x: Vec<Self::Width>) -> ValueArray {
                ValueArray::from(x)
            }
        }
    };
}

impl_convert_bin_type! {i8, DataType::Int8}
impl_convert_bin_type! {u8, DataType::UInt8}
impl_convert_bin_type! {i16, DataType::Int16}
impl_convert_bin_type! {u16, DataType::UInt16}
impl_convert_bin_type! {i32, DataType::Int32}
impl_convert_bin_type! {u32, DataType::UInt32}
impl_convert_bin_type! {i64, DataType::Int64}
impl_convert_bin_type! {u64, DataType::UInt64}
impl_convert_bin_type! {f32, DataType::Float32}
impl_convert_bin_type! {f64, DataType::Float64}

/// Build the columns for a histogram measurement.
///
/// This all the columns for a histogram. Some of these, such as the bins, sum
/// of samples, and quantiles, depend on the datum type. Others don't, like the
/// counts array, which is always a `u64`.
fn build_histogram_measurement_columns<T>(
    hist: &Histogram<T>,
) -> impl Iterator<Item = (String, Column)> + use<T>
where
    T: ConvertBinType,
{
    let mut columns = IndexMap::with_capacity(15);
    let (bins, counts) = hist.bins_and_counts();
    columns.insert(
        String::from(columns::BINS),
        Column::from(ValueArray::Array {
            inner_type: T::DATA_TYPE,
            values: vec![T::self_as_value_array(bins)],
        }),
    );
    columns.insert(
        String::from(columns::COUNTS),
        Column::from(ValueArray::Array {
            inner_type: DataType::UInt64,
            values: vec![ValueArray::from(counts)],
        }),
    );
    columns.insert(
        String::from(columns::MIN),
        Column::from(T::self_as_value_array(vec![hist.min()])),
    );
    columns.insert(
        String::from(columns::MAX),
        Column::from(T::self_as_value_array(vec![hist.max()])),
    );
    columns.insert(
        String::from(columns::SUM_OF_SAMPLES),
        Column::from(T::width_as_value_array(vec![hist.sum_of_samples()])),
    );
    columns.insert(
        String::from(columns::SQUARED_MEAN),
        Column::from(ValueArray::from(vec![hist.squared_mean()])),
    );
    for (p, quantile) in
        Quantile::iter().zip([hist.p50q(), hist.p90q(), hist.p99q()])
    {
        columns.insert(
            p.marker_heights().to_string(),
            Column::from(ValueArray::Array {
                inner_type: DataType::Float64,
                values: vec![ValueArray::from(
                    quantile.marker_heights().to_vec(),
                )],
            }),
        );
        columns.insert(
            p.marker_positions().to_string(),
            Column::from(ValueArray::Array {
                inner_type: DataType::UInt64,
                values: vec![ValueArray::from(
                    quantile.marker_positions().to_vec(),
                )],
            }),
        );
        columns.insert(
            p.desired_marker_positions().to_string(),
            Column::from(ValueArray::Array {
                inner_type: DataType::Float64,
                values: vec![ValueArray::from(
                    quantile.desired_marker_positions().to_vec(),
                )],
            }),
        );
    }
    columns.into_iter()
}

#[cfg(test)]
mod tests {
    use super::build_histogram_measurement_columns;
    use super::extract_measurement_as_block;
    use super::insert_datum_columns;
    use crate::model::columns;
    use crate::model::columns::Quantile;
    use crate::native::block::DataType;
    use crate::native::block::ValueArray;
    use bytes::Bytes;
    use indexmap::IndexMap;
    use oximeter::Datum;
    use oximeter::Sample;
    use oximeter::histogram::Histogram;
    use oximeter::histogram::Record as _;
    use oximeter::types::Cumulative;

    #[test]
    fn test_insert_datum_columns_scalar() {
        let data = [
            Datum::Bool(true),
            Datum::I8(1),
            Datum::U8(1),
            Datum::I16(1),
            Datum::U16(1),
            Datum::I32(1),
            Datum::U32(1),
            Datum::I64(1),
            Datum::U64(1),
            Datum::F32(1.0),
            Datum::F64(1.0),
            Datum::String("foo".into()),
            Datum::Bytes(Bytes::from(vec![0u8, 1, 2])),
            Datum::CumulativeI64(Cumulative::new(1)),
            Datum::CumulativeU64(Cumulative::new(1)),
            Datum::CumulativeF32(Cumulative::new(1.0)),
            Datum::CumulativeF64(Cumulative::new(1.0)),
        ];
        for datum in data.iter() {
            let mut columns = IndexMap::new();
            insert_datum_columns(datum, &mut columns);
            assert_eq!(
                columns.len(),
                1,
                "Should only insert 1 column for a scalar datum"
            );
            let values = &columns.get(columns::DATUM).unwrap().values;
            let data_type = DataType::column_type_for(datum.datum_type());
            assert_eq!(
                values.data_type(),
                data_type,
                "Inserted incorrect value array type"
            );
        }
    }

    #[test]
    fn test_extract_histogram_measurement_columns() {
        let mut hist = Histogram::<u64>::power_of_two();
        hist.sample(10).unwrap();
        let columns = build_histogram_measurement_columns(&hist)
            .collect::<IndexMap<_, _>>();
        assert_eq!(columns.len(), 15, "Incorrect number of histogram columns");

        let bins = columns
            .get(columns::BINS)
            .expect("Should have inserted a `bins` column");
        let ValueArray::Array { inner_type, values } = &bins.values else {
            panic!("Expected an array column for `bins`, found {bins:#?}");
        };
        assert_eq!(
            inner_type,
            &DataType::UInt64,
            "Incorrect array inner data type"
        );
        assert_eq!(values.len(), 1);
        let bins = &values[0];
        assert_eq!(
            bins.len(),
            hist.n_bins(),
            "Incorrect number of bins in extracted data block",
        );

        let counts = columns
            .get(columns::COUNTS)
            .expect("Should have inserted a `counts` column");
        let ValueArray::Array { inner_type, values } = &counts.values else {
            panic!("Expected an array column for `counts`, found {counts:#?}");
        };
        assert_eq!(
            inner_type,
            &DataType::UInt64,
            "Incorrect array inner data type"
        );
        assert_eq!(values.len(), 1);
        let counts = &values[0];
        assert_eq!(
            counts.len(),
            hist.n_bins(),
            "Incorrect number of bins in extracted data block",
        );

        // Sanity check that we actually stored a value.
        let bin = 10usize.ilog(2) as usize + 1;
        let ValueArray::UInt64(counts) = counts else { unreachable!() };
        assert_eq!(hist.get(bin).unwrap().count, 1);
        assert_eq!(counts[bin], 1);
    }

    #[derive(oximeter::Target)]
    struct SomeTarget {
        x: bool,
    }

    #[derive(oximeter::Metric)]
    struct Gauge {
        datum: i32,
    }

    #[derive(oximeter::Metric)]
    struct Cumul {
        datum: Cumulative<u64>,
    }

    #[derive(oximeter::Metric)]
    struct Hist {
        datum: Histogram<f64>,
    }

    #[test]
    fn test_extract_measurement_as_block_gauge() {
        let sample =
            Sample::new(&SomeTarget { x: false }, &Gauge { datum: 1 }).unwrap();
        let (table_name, block) = extract_measurement_as_block(&sample);
        assert_eq!(
            table_name, "measurements_i32",
            "Incorrect table name for gauge measurement"
        );
        assert_eq!(
            block.n_rows(),
            1,
            "Should have extracted one row for a gauge measurement",
        );
        assert_eq!(
            block.n_columns(),
            4,
            "Should have extracted 4 columns for a gauge measurement"
        );

        let col = block
            .column_values(columns::TIMESERIES_NAME)
            .expect("Should have a `timeseries_name` column");
        let ValueArray::String(names) = col else {
            panic!("Expected a String column for the names, found {col:#?}");
        };
        assert_eq!(names, &[sample.timeseries_name.to_string()]);

        let col = block
            .column_values(columns::TIMESERIES_KEY)
            .expect("Should have a `timeseries_key` column");
        let ValueArray::UInt64(keys) = col else {
            panic!("Expected a UInt64 column for the keys, found {col:#?}");
        };
        assert_eq!(keys, &[crate::timeseries_key(&sample)]);

        let col = block
            .column_values(columns::TIMESTAMP)
            .expect("Should have a `timestamp` column");
        let ValueArray::DateTime64 { values, .. } = col else {
            panic!(
                "Expected a DateTime64 column for the timestamps, found {col:#?}"
            );
        };
        assert_eq!(values, &[sample.measurement.timestamp()]);

        let col = block
            .column_values(columns::DATUM)
            .expect("Should have a `datum` column");
        let ValueArray::Nullable { is_null, values } = col else {
            panic!(
                "Expected a Nullable(Int32) column for datum, found {col:#?}"
            );
        };
        assert!(
            is_null.iter().all(|x| !x),
            "All data points should be non-null"
        );
        let ValueArray::Int32(values) = &**values else {
            panic!("Expected an Int32 column for the datum, found {col:#?}");
        };
        assert_eq!(values, &[1]);
    }

    #[test]
    fn test_extract_measurement_as_block_cumulative() {
        let sample = Sample::new(
            &SomeTarget { x: false },
            &Cumul { datum: Cumulative::new(1) },
        )
        .unwrap();
        let (table_name, block) = extract_measurement_as_block(&sample);
        assert_eq!(
            table_name, "measurements_cumulativeu64",
            "Incorrect table name for cumulative measurement"
        );
        assert_eq!(
            block.n_rows(),
            1,
            "Should have extracted one row for a cumulative measurement",
        );
        assert_eq!(
            block.n_columns(),
            5,
            "Should have extracted 5 columns for a cumulative measurement"
        );

        let col = block
            .column_values(columns::TIMESERIES_NAME)
            .expect("Should have a `timeseries_name` column");
        let ValueArray::String(names) = col else {
            panic!("Expected a String column for the names, found {col:#?}");
        };
        assert_eq!(names, &[sample.timeseries_name.to_string()]);

        let col = block
            .column_values(columns::TIMESERIES_KEY)
            .expect("Should have a `timeseries_key` column");
        let ValueArray::UInt64(keys) = col else {
            panic!("Expected a UInt64 column for the keys, found {col:#?}");
        };
        assert_eq!(keys, &[crate::timeseries_key(&sample)]);

        let col = block
            .column_values(columns::START_TIME)
            .expect("Should have a `start_time` column");
        let ValueArray::DateTime64 { values, .. } = col else {
            panic!(
                "Expected a DateTime64 column for the start_time, found {col:#?}"
            );
        };
        assert_eq!(values, &[sample.measurement.start_time().unwrap()]);

        let col = block
            .column_values(columns::TIMESTAMP)
            .expect("Should have a `timestamp` column");
        let ValueArray::DateTime64 { values, .. } = col else {
            panic!(
                "Expected a DateTime64 column for the timestamps, found {col:#?}"
            );
        };
        assert_eq!(values, &[sample.measurement.timestamp()]);

        let col = block
            .column_values(columns::DATUM)
            .expect("Should have a `datum` column");
        let ValueArray::Nullable { is_null, values } = col else {
            panic!(
                "Expected a Nullable(UInt64) column for the datum, found {col:#?}"
            );
        };
        let ValueArray::UInt64(values) = &**values else {
            panic!(
                "Expected a Nullable(UInt64) column for the datum, found {values:#?}"
            );
        };
        assert_eq!(is_null, &[false]);
        assert_eq!(values, &[1]);
    }

    #[test]
    fn test_extract_measurement_as_block_histogram() {
        let sample = Sample::new(
            &SomeTarget { x: false },
            &Hist { datum: Histogram::<f64>::new(&[0.0, 1.0]).unwrap() },
        )
        .unwrap();
        let (table_name, block) = extract_measurement_as_block(&sample);
        assert_eq!(
            table_name, "measurements_histogramf64",
            "Incorrect table name for histogram measurement"
        );
        assert_eq!(
            block.n_rows(),
            1,
            "Should have extracted one row for a histogram measurement",
        );
        assert_eq!(
            block.n_columns(),
            19, // name, key, start_time, timestamp + 15 histogram columns
            "Should have extracted 20 columns for a histogram measurement"
        );

        let col = block
            .column_values(columns::TIMESERIES_NAME)
            .expect("Should have a `timeseries_name` column");
        let ValueArray::String(names) = col else {
            panic!("Expected a String column for the names, found {col:#?}");
        };
        assert_eq!(names, &[sample.timeseries_name.to_string()]);

        let col = block
            .column_values(columns::TIMESERIES_KEY)
            .expect("Should have a `timeseries_key` column");
        let ValueArray::UInt64(keys) = col else {
            panic!("Expected a UInt64 column for the keys, found {col:#?}");
        };
        assert_eq!(keys, &[crate::timeseries_key(&sample)]);

        let col = block
            .column_values(columns::START_TIME)
            .expect("Should have a `start_time` column");
        let ValueArray::DateTime64 { values, .. } = col else {
            panic!(
                "Expected a DateTime64 column for the start_time, found {col:#?}"
            );
        };
        assert_eq!(values, &[sample.measurement.start_time().unwrap()]);

        let col = block
            .column_values(columns::TIMESTAMP)
            .expect("Should have a `timestamp` column");
        let ValueArray::DateTime64 { values, .. } = col else {
            panic!(
                "Expected a DateTime64 column for the timestamps, found {col:#?}"
            );
        };
        assert_eq!(values, &[sample.measurement.timestamp()]);

        for name in [
            columns::BINS,
            columns::COUNTS,
            columns::MIN,
            columns::MAX,
            columns::SUM_OF_SAMPLES,
            columns::SQUARED_MEAN,
            Quantile::P50.marker_heights(),
            Quantile::P50.marker_positions(),
            Quantile::P50.desired_marker_positions(),
            Quantile::P90.marker_heights(),
            Quantile::P90.marker_positions(),
            Quantile::P90.desired_marker_positions(),
            Quantile::P99.marker_heights(),
            Quantile::P99.marker_positions(),
            Quantile::P99.desired_marker_positions(),
        ] {
            let _ = block.column_values(name).unwrap_or_else(|_| {
                panic!("Should have a column named `{name}`")
            });
        }
    }
}
