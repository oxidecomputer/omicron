// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Types for working with actual blocks and columns of data.

use super::Error;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use std::{
    fmt,
    net::{Ipv4Addr, Ipv6Addr},
};
use uuid::Uuid;

/// A set of rows and columns.
///
/// This is the fundamental unit of data sent between ClickHouse and its
/// clients. It can represent a set of columns selected from a table or the
/// results of a query. It is not necessarily the entire set of data -- most
/// queries result in sending more than one block. But every query (and some of
/// the management or status commands) sends data as one or more block.
#[derive(Clone, Debug, PartialEq)]
pub struct Block {
    /// A name for the block.
    ///
    /// Not sure what this is for, actually.
    pub name: String,
    /// Details about the block.
    pub info: BlockInfo,
    /// The number of columns in the block.
    pub n_columns: u64,
    /// The number of rows in the block.
    pub n_rows: u64,
    /// Mapping from column names to the column data.
    pub columns: IndexMap<String, Column>,
}

impl Block {
    /// Return an iterator over each column's data type.
    pub fn data_types(&self) -> impl Iterator<Item = &'_ DataType> {
        self.columns.values().map(|col| &col.data_type)
    }

    /// Return true if the provided block is empty.
    pub fn is_empty(&self) -> bool {
        self.n_rows == 0
    }

    /// Create an empty block with the provided column names and types
    pub fn empty<'a>(
        types: impl IntoIterator<Item = (&'a str, DataType)>,
    ) -> Result<Self, Error> {
        let mut columns = IndexMap::new();
        let mut n_columns = 0;
        for (name, type_) in types.into_iter() {
            if !type_.is_supported() {
                return Err(Error::UnsupportedDataType(type_.to_string()));
            }
            n_columns += 1;
            columns.insert(name.to_string(), Column::empty(type_));
        }
        Ok(Self {
            name: String::new(),
            info: BlockInfo::default(),
            n_columns,
            n_rows: 0,
            columns,
        })
    }

    /// Concatenate this data block with another.
    ///
    /// An error is returned if the two blocks have different structure.
    pub(crate) fn concat(&mut self, block: Block) -> Result<(), Error> {
        if !self.matches_structure(&block) {
            return Err(Error::MismatchedBlockStructure);
        }
        for (our_col, their_col) in
            self.columns.values_mut().zip(block.columns.into_values())
        {
            our_col.concat(their_col).expect("Checked above");
        }
        Ok(())
    }

    fn matches_structure(&self, block: &Block) -> bool {
        if self.n_columns != block.n_columns {
            return false;
        }
        for (us, them) in self.columns.iter().zip(block.columns.iter()) {
            if us.0 != them.0 {
                return false;
            }
            if us.1.data_type != them.1.data_type {
                return false;
            }
        }
        true
    }
}

/// Details about the block.
///
/// This is only used for a few special kinds of queries. See the fields for
/// details.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BlockInfo {
    /// True if this is an "overflow" block, which is the case if:
    ///
    /// - The data results from a query like `... GROUP BY ... WITH TOTALS`
    /// - The number of rows exceeds the `max_rows_to_group_by` setting.
    /// - The `group_by_overflow_mode` setting is "any".
    ///
    /// In that case, this block contains only the aggregated overflow data
    /// beyond `max_rows_to_group_by`.
    pub is_overflows: bool,
    /// Used to optimize merges for distributed aggregation, when using a
    /// "two-level" aggregation method.
    ///
    /// This is only relevant if the `group_by_two_level_threshold` setting is
    /// non-zero (which it is not by default). But if it is, ClickHouse attempts
    /// to break up large GROUP BY operations into two levels, where it
    /// distributes the data and does groupings in parallel, and then finally
    /// groups all those groups again in a merge step.
    pub bucket_num: i32,
}

impl BlockInfo {
    pub const OVERFLOW_FIELD_NUM: u64 = 1;
    pub const BUCKET_FIELD_NUM: u64 = 2;
}

impl Default for BlockInfo {
    fn default() -> Self {
        Self {
            is_overflows: false,
            // This value is -1 if the block doesn't contain any data resulting
            // from a two-level aggregation. We never expect that method to be
            // used, so we use -1 here.
            bucket_num: -1,
        }
    }
}

/// A single column of data.
///
/// This represents a single column of data fetched in a query or sent in an
/// insert. It includes the type implicitly, in the value array it contains.
#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    /// The data values for this column.
    pub values: ValueArray,
    /// The type of the data in this column.
    pub data_type: DataType,
}

impl Column {
    /// Create an empty column of the provided type.
    pub fn empty(data_type: DataType) -> Self {
        let values = ValueArray::empty(&data_type);
        Self { values, data_type }
    }

    /// Concatenate another column to this one.
    ///
    /// An error is returned if the columns do not match in data types.
    fn concat(&mut self, rhs: Column) -> Result<(), Error> {
        if self.data_type != rhs.data_type {
            return Err(Error::MismatchedBlockStructure);
        }
        self.values.concat(rhs.values);
        Ok(())
    }
}

/// An array of singly-typed data values from the server.
#[derive(Clone, Debug, PartialEq)]
pub enum ValueArray {
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    UInt128(Vec<u128>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Int128(Vec<i128>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    String(Vec<String>),
    Uuid(Vec<Uuid>),
    Ipv4(Vec<Ipv4Addr>),
    Ipv6(Vec<Ipv6Addr>),
    DateTime(Vec<DateTime<Utc>>),
    DateTime64 { precision: Precision, values: Vec<DateTime<Utc>> },
    Nullable { is_null: Vec<bool>, values: Box<ValueArray> },
    Enum8 { variants: IndexMap<i8, String>, values: Vec<i8> },
    Array { inner_type: DataType, values: Vec<ValueArray> },
}

impl ValueArray {
    pub fn len(&self) -> usize {
        match self {
            ValueArray::UInt8(inner) => inner.len(),
            ValueArray::UInt16(inner) => inner.len(),
            ValueArray::UInt32(inner) => inner.len(),
            ValueArray::UInt64(inner) => inner.len(),
            ValueArray::UInt128(inner) => inner.len(),
            ValueArray::Int8(inner) => inner.len(),
            ValueArray::Int16(inner) => inner.len(),
            ValueArray::Int32(inner) => inner.len(),
            ValueArray::Int64(inner) => inner.len(),
            ValueArray::Int128(inner) => inner.len(),
            ValueArray::Float32(inner) => inner.len(),
            ValueArray::Float64(inner) => inner.len(),
            ValueArray::String(inner) => inner.len(),
            ValueArray::Uuid(inner) => inner.len(),
            ValueArray::Ipv4(inner) => inner.len(),
            ValueArray::Ipv6(inner) => inner.len(),
            ValueArray::DateTime(inner) => inner.len(),
            ValueArray::DateTime64 { values, .. } => values.len(),
            ValueArray::Nullable { values, .. } => values.len(),
            ValueArray::Enum8 { values, .. } => values.len(),
            ValueArray::Array { values, .. } => values.len(),
        }
    }

    /// Return an empty value array of the provided type.
    fn empty(data_type: &DataType) -> ValueArray {
        match data_type {
            DataType::UInt8 => ValueArray::UInt8(vec![]),
            DataType::UInt16 => ValueArray::UInt16(vec![]),
            DataType::UInt32 => ValueArray::UInt32(vec![]),
            DataType::UInt64 => ValueArray::UInt64(vec![]),
            DataType::UInt128 => ValueArray::UInt128(vec![]),
            DataType::Int8 => ValueArray::Int8(vec![]),
            DataType::Int16 => ValueArray::Int16(vec![]),
            DataType::Int32 => ValueArray::Int32(vec![]),
            DataType::Int64 => ValueArray::Int64(vec![]),
            DataType::Int128 => ValueArray::Int128(vec![]),
            DataType::Float32 => ValueArray::Float32(vec![]),
            DataType::Float64 => ValueArray::Float64(vec![]),
            DataType::String => ValueArray::String(vec![]),
            DataType::Uuid => ValueArray::Uuid(vec![]),
            DataType::Ipv4 => ValueArray::Ipv4(vec![]),
            DataType::Ipv6 => ValueArray::Ipv6(vec![]),
            DataType::DateTime => ValueArray::DateTime(vec![]),
            DataType::DateTime64(precision) => {
                ValueArray::DateTime64 { precision: *precision, values: vec![] }
            }
            DataType::Enum8(variants) => {
                ValueArray::Enum8 { variants: variants.clone(), values: vec![] }
            }
            DataType::Nullable(inner) => ValueArray::Nullable {
                is_null: vec![],
                values: Box::new(ValueArray::empty(inner)),
            },
            DataType::Array(inner) => {
                let inner_type = (**inner).clone();
                ValueArray::Array { inner_type, values: vec![] }
            }
        }
    }

    /// Concatenate another value array to this.
    ///
    /// # Panics
    ///
    /// This panics if the two value arrays do not have the same types.
    fn concat(&mut self, rhs: ValueArray) {
        match (self, rhs) {
            (ValueArray::UInt8(us), ValueArray::UInt8(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::UInt16(us), ValueArray::UInt16(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::UInt32(us), ValueArray::UInt32(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::UInt64(us), ValueArray::UInt64(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::UInt128(us), ValueArray::UInt128(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::Int8(us), ValueArray::Int8(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::Int16(us), ValueArray::Int16(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::Int32(us), ValueArray::Int32(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::Int64(us), ValueArray::Int64(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::Int128(us), ValueArray::Int128(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::Float32(us), ValueArray::Float32(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::Float64(us), ValueArray::Float64(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::String(us), ValueArray::String(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::Uuid(us), ValueArray::Uuid(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::Ipv4(us), ValueArray::Ipv4(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::Ipv6(us), ValueArray::Ipv6(mut them)) => {
                us.append(&mut them)
            }
            (ValueArray::DateTime(us), ValueArray::DateTime(mut them)) => {
                us.append(&mut them)
            }
            (
                ValueArray::DateTime64 { values: us, .. },
                ValueArray::DateTime64 { values: mut them, .. },
            ) => us.append(&mut them),
            (
                ValueArray::Nullable { is_null, values },
                ValueArray::Nullable {
                    is_null: mut rhs_is_null,
                    values: rhs_values,
                },
            ) => {
                is_null.append(&mut rhs_is_null);
                values.concat(*rhs_values);
            }
            (
                ValueArray::Enum8 { values: us, .. },
                ValueArray::Enum8 { values: mut them, .. },
            ) => us.append(&mut them),
            (
                ValueArray::Array { values: us, .. },
                ValueArray::Array { values: mut them, .. },
            ) => us.append(&mut them),
            (_, _) => unreachable!(),
        }
    }
}

macro_rules! impl_value_array_from_vec {
    ($data_type:ty, $variant:tt) => {
        impl From<Vec<$data_type>> for ValueArray {
            fn from(v: Vec<$data_type>) -> Self {
                Self::$variant(v)
            }
        }
    };
}

impl_value_array_from_vec!(u8, UInt8);
impl_value_array_from_vec!(u16, UInt16);
impl_value_array_from_vec!(u32, UInt32);
impl_value_array_from_vec!(u64, UInt64);
impl_value_array_from_vec!(u128, UInt128);
impl_value_array_from_vec!(i8, Int8);
impl_value_array_from_vec!(i16, Int16);
impl_value_array_from_vec!(i32, Int32);
impl_value_array_from_vec!(i64, Int64);
impl_value_array_from_vec!(i128, Int128);
impl_value_array_from_vec!(f32, Float32);
impl_value_array_from_vec!(f64, Float64);
impl_value_array_from_vec!(String, String);
impl_value_array_from_vec!(Uuid, Uuid);
impl_value_array_from_vec!(Ipv4Addr, Ipv4);
impl_value_array_from_vec!(Ipv6Addr, Ipv6);

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Precision(u8);

impl TryFrom<u8> for Precision {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(())
    }
}

/// Helper macro to generate a function that converts a DateTime64 timestamp to
/// a DateTime.
///
/// ClickHouse encodes DateTime64s as an i64, with a precision or scale
/// specified in the data type itself. That precision is the number of base-10
/// subsecond digits the timestamp specifies, or a scale factor. For example,
/// with a precision of 3, the i64 specifies the number of milliseconds.
///
/// This macro returns a closure that applies the scale factor to an i64, in
/// order to convert it to a number of seconds and nanoseconds. Those are then
/// used to call `DateTime::from_timestamp()`.
macro_rules! precision_conversion_func {
    ($precision:literal) => {{
        |x| {
            const SCALE: i64 = 10i64.pow($precision);
            const FACTOR: i64 = 10i64.pow(Precision::MAX as u32 - $precision);
            let seconds = x.div_euclid(SCALE);
            let nanos = (FACTOR * x.rem_euclid(SCALE)).try_into().unwrap();
            DateTime::from_timestamp(seconds, nanos).unwrap()
        }
    }};
}

impl Precision {
    const MAX: u8 = 9;

    pub fn new(precision: u8) -> Option<Self> {
        if precision <= Self::MAX {
            Some(Self(precision))
        } else {
            None
        }
    }

    /// Return a conversion function that takes an i64 count and converts it to
    /// a DateTime.
    pub(crate) fn as_conv(&self) -> fn(i64) -> DateTime<Utc> {
        // For the easy values, we'll convert to seconds or microseconds, and
        // then use a constructor.
        //
        // For the weird values, say 10ths of a second, we will compute the the
        // next-smallest sane unit, in this case milliseconds, and use the
        // appropriate constructor.
        match self.0 {
            0 => |x| DateTime::from_timestamp(x, 0).unwrap(),
            1 => precision_conversion_func!(1),
            2 => precision_conversion_func!(2),
            3 => |x| DateTime::from_timestamp_millis(x).unwrap(),
            4 => precision_conversion_func!(4),
            5 => precision_conversion_func!(5),
            6 => |x| DateTime::from_timestamp_micros(x).unwrap(),
            7 => precision_conversion_func!(7),
            8 => precision_conversion_func!(8),
            9 => |x| DateTime::from_timestamp_nanos(x),
            10..=u8::MAX => unreachable!(),
        }
    }

    /// Convert the provided datetime into a timestamp in the right precision.
    pub(crate) fn scale(&self, value: DateTime<Utc>) -> i64 {
        match self.0 {
            0 => value.timestamp(),
            1 => value.timestamp_millis() / 100,
            2 => value.timestamp_millis() / 10,
            3 => value.timestamp_millis(),
            4 => value.timestamp_micros() / 100,
            5 => value.timestamp_micros() / 10,
            6 => value.timestamp_micros(),
            7 => value.timestamp_nanos_opt().unwrap() / 100,
            8 => value.timestamp_nanos_opt().unwrap() / 10,
            9 => value.timestamp_nanos_opt().unwrap(),
            10.. => unreachable!(),
        }
    }
}

impl fmt::Display for Precision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A type of a column of data.
#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Float32,
    Float64,
    String,
    Uuid,
    Ipv4,
    Ipv6,
    DateTime,
    DateTime64(Precision),
    Enum8(IndexMap<i8, String>),
    Nullable(Box<DataType>),
    Array(Box<DataType>),
}

impl DataType {
    /// Return true if the data type is supported.
    ///
    /// Some types are not supported by ClickHouse, such as Nullable(Array(T)).
    pub fn is_supported(&self) -> bool {
        match self {
            DataType::Nullable(inner) => match &**inner {
                DataType::Nullable(_) | DataType::Array(_) => false,
                _scalar => true,
            },
            _non_nullable => true,
        }
    }

    /// Return true if this is a nullable type.
    pub(crate) fn is_nullable(&self) -> bool {
        matches!(self, DataType::Nullable(_))
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::UInt8 => write!(f, "UInt8"),
            DataType::UInt16 => write!(f, "UInt16"),
            DataType::UInt32 => write!(f, "UInt32"),
            DataType::UInt64 => write!(f, "UInt64"),
            DataType::UInt128 => write!(f, "UInt128"),
            DataType::Int8 => write!(f, "Int8"),
            DataType::Int16 => write!(f, "Int16"),
            DataType::Int32 => write!(f, "Int32"),
            DataType::Int64 => write!(f, "Int64"),
            DataType::Int128 => write!(f, "Int128"),
            DataType::Float32 => write!(f, "Float32"),
            DataType::Float64 => write!(f, "Float64"),
            DataType::String => write!(f, "String"),
            DataType::Uuid => write!(f, "UUID"),
            DataType::Ipv4 => write!(f, "IPv4"),
            DataType::Ipv6 => write!(f, "IPv6"),
            DataType::DateTime => write!(f, "DateTime"),
            DataType::DateTime64(prec) => write!(f, "DateTime64({prec})"),
            DataType::Enum8(map) => {
                write!(f, "Enum8(")?;
                for (i, (val, name)) in map.iter().enumerate() {
                    write!(f, "'{name}' = {val}")?;
                    if i < map.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
            DataType::Nullable(inner) => write!(f, "Nullable({inner})"),
            DataType::Array(inner) => write!(f, "Array({inner})"),
        }
    }
}

impl std::str::FromStr for DataType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Simple scalar types.
        if s == "UInt8" {
            return Ok(DataType::UInt8);
        } else if s == "UInt16" {
            return Ok(DataType::UInt16);
        } else if s == "UInt32" {
            return Ok(DataType::UInt32);
        } else if s == "UInt64" {
            return Ok(DataType::UInt64);
        } else if s == "UInt128" {
            return Ok(DataType::UInt128);
        } else if s == "Int8" {
            return Ok(DataType::Int8);
        } else if s == "Int16" {
            return Ok(DataType::Int16);
        } else if s == "Int32" {
            return Ok(DataType::Int32);
        } else if s == "Int64" {
            return Ok(DataType::Int64);
        } else if s == "Int128" {
            return Ok(DataType::Int128);
        } else if s == "Float32" {
            return Ok(DataType::Float32);
        } else if s == "Float64" {
            return Ok(DataType::Float64);
        } else if s == "String" {
            return Ok(DataType::String);
        } else if s == "UUID" {
            return Ok(DataType::Uuid);
        } else if s == "IPv4" {
            return Ok(DataType::Ipv4);
        } else if s == "IPv6" {
            return Ok(DataType::Ipv6);
        } else if s == "DateTime" {
            return Ok(DataType::DateTime);
        }

        // Check for DateTime with precision.
        if let Some(suffix) = s.strip_prefix("DateTime64(") {
            let Some(inner) = suffix.strip_suffix(")") else {
                return Err(Error::UnsupportedDataType(s.to_string()));
            };
            return inner
                .parse()
                .map_err(|_| Error::UnsupportedDataType(s.to_string()))
                .map(|p| DataType::DateTime64(Precision(p)));
        }

        // Check for Enum8s.
        //
        // These are written like "Enum8('foo' = 1, 'bar' = 2)"
        if let Some(suffix) = s.strip_prefix("Enum8(") {
            let Some(inner) = suffix.strip_suffix(")") else {
                return Err(Error::UnsupportedDataType(s.to_string()));
            };
            let mut map = IndexMap::new();
            for each in inner.split(',') {
                let Some((name, value)) = each.split_once(" = ") else {
                    return Err(Error::UnsupportedDataType(s.to_string()));
                };
                let Ok(value) = value.parse() else {
                    return Err(Error::UnsupportedDataType(s.to_string()));
                };
                // Trim whitespace from the name and strip any single-quotes.
                let name = name.trim().trim_matches('\'').to_string();
                map.insert(value, name.to_string());
            }
            return Ok(DataType::Enum8(map));
        }

        // Recurse for nullable types.
        if let Some(suffix) = s.strip_prefix("Nullable(") {
            let Some(inner) = suffix.strip_suffix(')') else {
                return Err(Error::UnsupportedDataType(s.to_string()));
            };
            return inner
                .parse()
                .map(|inner| DataType::Nullable(Box::new(inner)));
        }

        // And for arrays.
        if let Some(suffix) = s.strip_prefix("Array(") {
            let Some(inner) = suffix.strip_suffix(')') else {
                return Err(Error::UnsupportedDataType(s.to_string()));
            };
            return inner.parse().map(|inner| DataType::Array(Box::new(inner)));
        }

        // Anything else is unsupported for now.
        Err(Error::UnsupportedDataType(s.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::{DataType, Precision};
    use chrono::{SubsecRound as _, Utc};

    #[test]
    fn test_data_type_to_string() {
        let enum8 =
            [(0i8, "foo".into()), (1, "bar".into())].into_iter().collect();
        for (type_, as_str) in [
            (DataType::UInt8, "UInt8"),
            (DataType::UInt16, "UInt16"),
            (DataType::UInt32, "UInt32"),
            (DataType::UInt64, "UInt64"),
            (DataType::UInt128, "UInt128"),
            (DataType::Int8, "Int8"),
            (DataType::Int16, "Int16"),
            (DataType::Int32, "Int32"),
            (DataType::Int64, "Int64"),
            (DataType::Int128, "Int128"),
            (DataType::Float32, "Float32"),
            (DataType::Float64, "Float64"),
            (DataType::String, "String"),
            (DataType::Uuid, "UUID"),
            (DataType::Ipv4, "IPv4"),
            (DataType::Ipv6, "IPv6"),
            (DataType::DateTime, "DateTime"),
            (DataType::DateTime64(6.try_into().unwrap()), "DateTime64(6)"),
            (DataType::Enum8(enum8), "Enum8('foo' = 0, 'bar' = 1)"),
            (DataType::Nullable(Box::new(DataType::UInt8)), "Nullable(UInt8)"),
            (DataType::Array(Box::new(DataType::UInt8)), "Array(UInt8)"),
        ] {
            assert_eq!(type_.to_string(), as_str);
            assert_eq!(type_, as_str.parse().unwrap());
        }
    }

    #[test]
    fn test_datetime64_conversions() {
        let now = Utc::now();
        for precision in 0..=Precision::MAX {
            let prec = Precision(precision);
            let timestamp = prec.scale(now);
            let conv = prec.as_conv();
            let recovered = conv(timestamp);
            let now_with_precision = now.trunc_subsecs(u16::from(prec.0));
            assert_eq!(
                now_with_precision, recovered,
                "Failed to recover converted timestamp, \
                precision = {prec}, \
                now = {now},\
                timestamp = {timestamp}, \
                recovered = {recovered},
            "
            );
        }
    }
}
