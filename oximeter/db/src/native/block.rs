// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Types for working with actual blocks and columns of data.

use super::packets::server::ColumnDescription;
use super::Error;
use chrono::DateTime;
use chrono::NaiveDate;
use chrono_tz::Tz;
use indexmap::IndexMap;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_while1;
use nom::character::complete::alphanumeric1;
use nom::character::complete::i8 as nom_i8;
use nom::character::complete::u8 as nom_u8;
use nom::combinator::all_consuming;
use nom::combinator::eof;
use nom::combinator::map;
use nom::combinator::map_opt;
use nom::combinator::opt;
use nom::combinator::value;
use nom::multi::separated_list1;
use nom::sequence::delimited;
use nom::sequence::preceded;
use nom::sequence::separated_pair;
use nom::sequence::tuple;
use nom::IResult;
use std::fmt;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::sync::LazyLock;
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

    /// Return true if the provided block is empty, meaning zero columns and
    /// rows.
    ///
    /// NOTE: This is mostly used to indicate the "end of stream" data blocks.
    /// Blocks with zero rows are used to communicate the column names and
    /// types, and are _not_ considered empty.
    pub fn is_empty(&self) -> bool {
        self.n_columns == 0 && self.n_rows == 0
    }

    /// Create an empty block.
    pub fn empty() -> Self {
        Self {
            name: String::new(),
            info: BlockInfo::default(),
            n_columns: 0,
            n_rows: 0,
            columns: IndexMap::new(),
        }
    }

    /// Concatenate this data block with another.
    ///
    /// An error is returned if the two blocks have different structure.
    pub(crate) fn concat(&mut self, block: Block) -> Result<(), Error> {
        if !self.matches_structure(&block) {
            return Err(Error::MismatchedBlockStructure);
        }
        let n_new_rows = block.n_rows;
        for (our_col, their_col) in
            self.columns.values_mut().zip(block.columns.into_values())
        {
            our_col.concat(their_col).expect("Checked above");
        }
        self.n_rows += n_new_rows;
        Ok(())
    }

    /// Return true if this block matches the structure of the other.
    ///
    /// This means it has the same column names and types.
    pub fn matches_structure(&self, block: &Block) -> bool {
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

    /// Return the values of the named column, if it exists.
    pub fn column_values(&self, name: &str) -> Result<&ValueArray, Error> {
        self.columns
            .get(name)
            .map(|col| &col.values)
            .ok_or_else(|| Error::NoSuchColumn(name.to_string()))
    }

    pub(crate) fn matches_table_description(
        &self,
        columns: &[ColumnDescription],
    ) -> bool {
        if self.n_columns != columns.len() as u64 {
            return false;
        }
        for (our_col, their_col) in self.columns.iter().zip(columns.iter()) {
            if our_col.0 != &their_col.name {
                return false;
            }
            if our_col.1.data_type != their_col.data_type {
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

impl From<ValueArray> for Column {
    fn from(values: ValueArray) -> Self {
        let data_type = values.data_type();
        Self { values, data_type }
    }
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

    /// Return true if the column is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the number of elements in the column.
    pub fn len(&self) -> usize {
        self.values.len()
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
    Date(Vec<NaiveDate>),
    DateTime { tz: Tz, values: Vec<DateTime<Tz>> },
    DateTime64 { precision: Precision, tz: Tz, values: Vec<DateTime<Tz>> },
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
            ValueArray::Date(inner) => inner.len(),
            ValueArray::DateTime { values, .. } => values.len(),
            ValueArray::DateTime64 { values, .. } => values.len(),
            ValueArray::Nullable { values, .. } => values.len(),
            ValueArray::Enum8 { values, .. } => values.len(),
            ValueArray::Array { values, .. } => values.len(),
        }
    }

    /// Return an empty value array of the provided type.
    pub fn empty(data_type: &DataType) -> ValueArray {
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
            DataType::Date => ValueArray::Date(vec![]),
            DataType::DateTime(tz) => {
                ValueArray::DateTime { tz: *tz, values: vec![] }
            }
            DataType::DateTime64(precision, tz) => ValueArray::DateTime64 {
                precision: *precision,
                tz: *tz,
                values: vec![],
            },
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
            (
                ValueArray::DateTime { values: us, .. },
                ValueArray::DateTime { values: mut them, .. },
            ) => us.append(&mut them),
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
            (_, _) => panic!("ValueArrays must have the same type"),
        }
    }

    /// Return the data type for this array of values.
    pub fn data_type(&self) -> DataType {
        match self {
            ValueArray::UInt8(_) => DataType::UInt8,
            ValueArray::UInt16(_) => DataType::UInt16,
            ValueArray::UInt32(_) => DataType::UInt32,
            ValueArray::UInt64(_) => DataType::UInt64,
            ValueArray::UInt128(_) => DataType::UInt128,
            ValueArray::Int8(_) => DataType::Int8,
            ValueArray::Int16(_) => DataType::Int16,
            ValueArray::Int32(_) => DataType::Int32,
            ValueArray::Int64(_) => DataType::Int64,
            ValueArray::Int128(_) => DataType::Int128,
            ValueArray::Float32(_) => DataType::Float32,
            ValueArray::Float64(_) => DataType::Float64,
            ValueArray::String(_) => DataType::String,
            ValueArray::Uuid(_) => DataType::Uuid,
            ValueArray::Ipv4(_) => DataType::Ipv4,
            ValueArray::Ipv6(_) => DataType::Ipv6,
            ValueArray::Date(_) => DataType::Date,
            ValueArray::DateTime { tz, .. } => DataType::DateTime(*tz),
            ValueArray::DateTime64 { precision, tz, .. } => {
                DataType::DateTime64(*precision, *tz)
            }
            ValueArray::Nullable { values, .. } => {
                DataType::Nullable(Box::new(values.data_type()))
            }
            ValueArray::Enum8 { variants, .. } => {
                DataType::Enum8(variants.clone())
            }
            ValueArray::Array { inner_type, .. } => {
                DataType::Array(Box::new(inner_type.clone()))
            }
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
    ($tz:expr, $precision:literal) => {{
        |tz, x| {
            const SCALE: i64 = 10i64.pow($precision);
            const FACTOR: i64 = 10i64.pow(Precision::MAX as u32 - $precision);
            let seconds = x.div_euclid(SCALE);
            let nanos = (FACTOR * x.rem_euclid(SCALE)).try_into().unwrap();
            tz.timestamp_opt(seconds, nanos).unwrap()
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
    pub(crate) fn as_conv<T: chrono::TimeZone>(
        &self,
        _: &T,
    ) -> fn(&T, i64) -> DateTime<T> {
        // For the easy values, we'll convert to seconds or microseconds, and
        // then use a constructor.
        //
        // For the weird values, say 10ths of a second, we will compute the the
        // next-smallest sane unit, in this case milliseconds, and use the
        // appropriate constructor.
        match self.0 {
            0 => |tz, x| tz.timestamp_opt(x, 0).unwrap(),
            1 => precision_conversion_func!(tz, 1),
            2 => precision_conversion_func!(tz, 2),
            3 => |tz, x| tz.timestamp_millis_opt(x).unwrap(),
            4 => precision_conversion_func!(tz, 4),
            5 => precision_conversion_func!(tz, 5),
            6 => |tz, x| tz.timestamp_nanos(x * 1000),
            7 => precision_conversion_func!(tz, 7),
            8 => precision_conversion_func!(tz, 8),
            9 => |tz, x| tz.timestamp_nanos(x),
            10..=u8::MAX => unreachable!(),
        }
    }

    /// Convert the provided datetime into a timestamp in the right precision.
    ///
    /// This returns `None` if the timestamp cannot be converted to an `i64`,
    /// which is how ClickHouse stores the values.
    pub(crate) fn scale(
        &self,
        value: DateTime<impl chrono::TimeZone>,
    ) -> Option<i64> {
        match self.0 {
            0 => Some(value.timestamp()),
            1 => Some(value.timestamp_millis() / 100),
            2 => Some(value.timestamp_millis() / 10),
            3 => Some(value.timestamp_millis()),
            4 => Some(value.timestamp_micros() / 100),
            5 => Some(value.timestamp_micros() / 10),
            6 => Some(value.timestamp_micros()),
            7 => value.timestamp_nanos_opt().map(|x| x / 100),
            8 => value.timestamp_nanos_opt().map(|x| x / 10),
            9 => value.timestamp_nanos_opt(),
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
    Date,
    DateTime(Tz),
    DateTime64(Precision, Tz),
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

    /// Parse out a data type from a string.
    ///
    /// This is a `nom`-based function, so that the method can be used in other
    /// contexts. The `DataType::from_str()` implementation is a thin wrapper
    /// around this.
    pub(super) fn nom_parse(s: &str) -> IResult<&str, Self> {
        alt((
            value(DataType::UInt8, tag("UInt8")),
            value(DataType::UInt16, tag("UInt16")),
            value(DataType::UInt32, tag("UInt32")),
            value(DataType::UInt64, tag("UInt64")),
            value(DataType::UInt128, tag("UInt128")),
            value(DataType::Int8, tag("Int8")),
            value(DataType::Int16, tag("Int16")),
            value(DataType::Int32, tag("Int32")),
            value(DataType::Int64, tag("Int64")),
            value(DataType::Int128, tag("Int128")),
            value(DataType::Float32, tag("Float32")),
            value(DataType::Float64, tag("Float64")),
            value(DataType::String, tag("String")),
            value(DataType::Uuid, tag("UUID")),
            value(DataType::Ipv4, tag("IPv4")),
            value(DataType::Ipv6, tag("IPv6")),
            // IMPORTANT: This needs to consume all its input, otherwise we may
            // parse something like `DateTime(UTC)` as `Date`, which is
            // incorrect.
            value(DataType::Date, all_consuming(tag("Date"))),
            // These need to be nested because `alt` supports a max of 21
            // parsers, and we have 22 data types.
            alt((datetime, datetime64, enum8, nullable, array)),
        ))(s)
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
            DataType::Date => write!(f, "Date"),
            DataType::DateTime(tz) => write!(f, "DateTime('{tz}')"),
            DataType::DateTime64(prec, tz) => {
                write!(f, "DateTime64({prec}, '{tz}')")
            }
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

// Parse a quoted timezone, like `'UTC'` or `'America/Los_Angeles'`
//
// Note that the quotes may optionally be escaped, like `\'UTC\'`, which is
// needed to support deserializing table descriptions, where the types for each
// column are serialized as an escaped string.
fn quoted_timezone(s: &str) -> IResult<&str, Tz> {
    map(
        delimited(
            preceded(opt(tag("\\")), tag("'")),
            take_while1(|c: char| {
                c.is_ascii_alphanumeric()
                    || c == '/'
                    || c == '+'
                    || c == '-'
                    || c == '_'
            }),
            preceded(opt(tag("\\")), tag("'")),
        ),
        parse_timezone,
    )(s)
}

// Parse a quoted timezone, delimited by parentheses ().
fn parenthesized_timezone(s: &str) -> IResult<&str, Tz> {
    delimited(tag("("), quoted_timezone, tag(")"))(s)
}

/// Parse a `DateTime` data type from a string, optionally with a timezone in
/// it.
fn datetime(s: &str) -> IResult<&str, DataType> {
    map(
        tuple((tag("DateTime"), opt(parenthesized_timezone), eof)),
        |(_, maybe_tz, _)| {
            DataType::DateTime(maybe_tz.unwrap_or_else(|| *DEFAULT_TIMEZONE))
        },
    )(s)
}

/// Parse a `DateTime64` data type from a string, with a precision and optional
/// timezone in it.
///
/// Matches things like `DateTime64(1)` and `DateTime64(1, 'UTC')`.
fn datetime64(s: &str) -> IResult<&str, DataType> {
    map(
        tuple((
            tag("DateTime64("),
            map_opt(nom_u8, Precision::new),
            opt(preceded(tag(", "), quoted_timezone)),
            tag(")"),
            eof,
        )),
        |(_, precision, maybe_tz, _, _)| {
            DataType::DateTime64(
                precision,
                maybe_tz.unwrap_or_else(|| *DEFAULT_TIMEZONE),
            )
        },
    )(s)
}

static DEFAULT_TIMEZONE: LazyLock<Tz> =
    LazyLock::new(|| match iana_time_zone::get_timezone() {
        Ok(s) => s.parse().unwrap_or_else(|_| Tz::UTC),
        Err(_) => Tz::UTC,
    });

fn parse_timezone(s: &str) -> Tz {
    s.parse().unwrap_or_else(|_| *DEFAULT_TIMEZONE)
}

/// Parse an enum variant name.
fn variant_name(s: &str) -> IResult<&str, &str> {
    delimited(
        preceded(opt(tag("\\")), tag("'")),
        alphanumeric1,
        preceded(opt(tag("\\")), tag("'")),
    )(s)
}

/// Parse a single enum variant, like `'Foo' = 1`.
///
/// Note that the single-quotes may be escaped, which is required for parsing
/// the `ColumnDescription` type from a `TableColumns` server packet.
fn enum_variant(s: &str) -> IResult<&str, (i8, &str)> {
    map(separated_pair(variant_name, tag(" = "), nom_i8), |(name, variant)| {
        (variant, name)
    })(s)
}

/// Parse an `Enum8` data type from a string.
pub(super) fn enum8(s: &str) -> IResult<&str, DataType> {
    map(
        delimited(
            tag("Enum8("),
            separated_list1(tag(", "), enum_variant),
            tag(")"),
        ),
        |variants| {
            let mut map = IndexMap::new();
            for (variant, name) in variants.into_iter() {
                map.insert(variant, name.to_string());
            }
            DataType::Enum8(map)
        },
    )(s)
}

fn nullable(s: &str) -> IResult<&str, DataType> {
    map(delimited(tag("Nullable("), DataType::nom_parse, tag(")")), |inner| {
        DataType::Nullable(Box::new(inner))
    })(s)
}

fn array(s: &str) -> IResult<&str, DataType> {
    map(delimited(tag("Array("), DataType::nom_parse, tag(")")), |inner| {
        DataType::Array(Box::new(inner))
    })(s)
}

impl std::str::FromStr for DataType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::nom_parse(s)
            .map(|(_, parsed)| parsed)
            .map_err(|_| Error::UnsupportedDataType(s.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::enum8;
    use super::Block;
    use super::BlockInfo;
    use super::Column;
    use super::DataType;
    use super::Precision;
    use super::ValueArray;
    use super::DEFAULT_TIMEZONE;
    use crate::native::block::datetime;
    use crate::native::block::datetime64;
    use crate::native::block::enum_variant;
    use crate::native::block::quoted_timezone;
    use chrono::SubsecRound as _;
    use chrono::Utc;
    use chrono_tz::Tz;
    use indexmap::IndexMap;

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
            (DataType::Date, "Date"),
            (DataType::DateTime(Tz::UTC), "DateTime('UTC')"),
            (
                DataType::DateTime64(6.try_into().unwrap(), Tz::UTC),
                "DateTime64(6, 'UTC')",
            ),
            (DataType::Enum8(enum8), "Enum8('foo' = 0, 'bar' = 1)"),
            (DataType::Nullable(Box::new(DataType::UInt8)), "Nullable(UInt8)"),
            (DataType::Array(Box::new(DataType::UInt8)), "Array(UInt8)"),
        ] {
            assert_eq!(type_.to_string(), as_str);
            assert_eq!(type_, as_str.parse().unwrap());
        }
    }

    #[test]
    fn test_parse_invalid_data_type() {
        for each in
            ["xxx", "DateTime64(-1)", "DateTime64(1", "Array(Array(UInt8)"]
        {
            let dt = each.parse::<DataType>();
            assert!(
                dt.is_err(),
                "Should not successfully parse '{}' into a DataType, \
                but it was parsed as: {:?}",
                each,
                dt.unwrap(),
            );
        }
    }

    #[test]
    fn test_datetime64_conversions() {
        let now = Utc::now();
        for precision in 0..=Precision::MAX {
            let prec = Precision(precision);
            let timestamp =
                prec.scale(now).expect("Current time should fit in an i64");
            let conv = prec.as_conv(&Utc);
            let recovered = conv(&Utc, timestamp);
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

    #[test]
    fn datetime64_scale_checks_range() {
        assert_eq!(
            Precision(9).scale(chrono::DateTime::<Utc>::MAX_UTC),
            None,
            "Should fail to scale a timestamp that doesn't fit in \
            the range of an i64"
        );
    }

    #[test]
    fn parse_date_time() {
        for (type_, s) in [
            (DataType::DateTime(*DEFAULT_TIMEZONE), "DateTime"),
            (DataType::DateTime(Tz::UTC), "DateTime('UTC')"),
            (
                DataType::DateTime(Tz::America__Los_Angeles),
                "DateTime('America/Los_Angeles')",
            ),
        ] {
            let dt = datetime(s).unwrap().1;
            assert_eq!(type_, dt, "Failed to parse '{}' into DateTime", s,);
        }

        assert!(datetime("DateTim").is_err());
        assert!(datetime("DateTime()").is_err());
        assert!(datetime("DateTime()").is_err());
        assert!(datetime("DateTime('U)").is_err());
        assert!(datetime("DateTime(0)").is_err());
    }

    #[test]
    fn parse_date_time64() {
        for (type_, s) in [
            (
                DataType::DateTime64(Precision(3), *DEFAULT_TIMEZONE),
                "DateTime64(3)",
            ),
            (
                DataType::DateTime64(Precision(3), Tz::UTC),
                "DateTime64(3, 'UTC')",
            ),
            (
                DataType::DateTime64(Precision(6), Tz::America__Los_Angeles),
                "DateTime64(6, 'America/Los_Angeles')",
            ),
        ] {
            let dt = datetime64(s).unwrap().1;
            assert_eq!(type_, dt, "Failed to parse '{}' into DateTime64", s,);
        }

        assert!(datetime64("DateTime6").is_err());
        assert!(datetime64("DateTime64(").is_err());
        assert!(datetime64("DateTime64()").is_err());
        assert!(datetime64("DateTime64('U)").is_err());
        assert!(datetime64("DateTime64(0, )").is_err());
        assert!(datetime64("DateTime64('a', 'UTC')").is_err());
        assert!(datetime64("DateTime64(1,'UTC')").is_err());
    }

    #[test]
    fn parse_escaped_date_time64() {
        assert_eq!(
            DataType::DateTime64(Precision(1), Tz::UTC),
            datetime64(r#"DateTime64(1, \'UTC\')"#).unwrap().1
        );
    }

    #[test]
    fn concat_blocks() {
        let data = vec![0, 1];
        let values = ValueArray::UInt64(data.clone());
        let mut block = Block {
            name: String::new(),
            info: BlockInfo::default(),
            n_columns: 1,
            n_rows: values.len() as u64,
            columns: IndexMap::from([(
                String::from("a"),
                Column { values: values.clone(), data_type: DataType::UInt64 },
            )]),
        };
        block.concat(block.clone()).unwrap();
        assert_eq!(block.n_columns, 1);
        assert_eq!(block.n_rows, values.len() as u64 * 2);
        assert_eq!(
            block.columns["a"].values,
            ValueArray::UInt64([data.as_slice(), data.as_slice()].concat())
        );
    }

    #[test]
    fn test_parse_enum_variant() {
        assert_eq!(enum_variant("'Foo' = 1'").unwrap().1, (1, "Foo"),);
        assert_eq!(enum_variant("\\'Foo\\' = 1'").unwrap().1, (1, "Foo"),);

        enum_variant("'Foo'").unwrap_err();
        enum_variant("'Foo' = ").unwrap_err();
        enum_variant("'Foo' = x").unwrap_err();
        enum_variant("\"Foo\" = 1").unwrap_err();
    }

    #[test]
    fn test_parse_enum8() {
        let parsed = enum8("Enum8('Foo' = 1, 'Bar' = 2)").unwrap().1;
        let DataType::Enum8(map) = parsed else {
            panic!("Expected DataType::Enum8, found {parsed:#?}");
        };
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&1).unwrap(), "Foo");
        assert_eq!(map.get(&2).unwrap(), "Bar");
    }

    #[test]
    fn test_parse_array_enum8_with_escapes() {
        const INPUT: &str = r#"Array(Enum8(\'Bool\' = 1, \'I64\' = 2))"#;
        let parsed = DataType::nom_parse(INPUT).unwrap().1;
        let DataType::Array(inner) = parsed else {
            panic!("Expected a `DataType::Array(_)`, found {parsed:#?}");
        };
        let DataType::Enum8(map) = &*inner else {
            panic!("Expected a `DataType::Enum8(_)`, found {inner:#?}");
        };
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&1).unwrap(), "Bool");
        assert_eq!(map.get(&2).unwrap(), "I64");
    }

    #[test]
    fn test_parse_array_enum8_with_bad_escapes() {
        DataType::nom_parse(r#"Array(Enum8(\\'Bool\' = 1, \'I64\' = 2))"#)
            .expect_err("Should fail to parse data type with bad escape");
        DataType::nom_parse(r#"Array(Enum8(\t\'Bool\' = 1, \'I64\' = 2))"#)
            .expect_err("Should fail to parse data type with bad escape");
        DataType::nom_parse(r#"Array(Enum8(\"Bool\' = 1, \'I64\' = 2))"#)
            .expect_err("Should fail to parse data type with bad escape");
    }

    #[test]
    fn test_parse_all_known_timezones() {
        for tz in chrono_tz::TZ_VARIANTS.iter() {
            let quoted = format!("'{}'", tz);
            let Ok(out) = quoted_timezone(&quoted) else {
                panic!("Failed to parse quoted timezone: {quoted}");
            };
            assert_eq!(&out.1, tz, "Failed to parse quoted timezone: {quoted}");

            let escape_quoted = format!("\\'{}\\'", tz);
            let Ok(out) = quoted_timezone(&escape_quoted) else {
                panic!(
                    "Failed to parse escaped quoted timezone: {escape_quoted}"
                );
            };
            assert_eq!(
                &out.1, tz,
                "Failed to parse escaped quoted timezone: {escape_quoted}"
            );
        }
    }
}
