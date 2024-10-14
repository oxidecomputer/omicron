// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Encode / decode a column.

use crate::native::{
    block::{Column, DataType, ValueArray},
    io, Error,
};
use bytes::{Buf as _, BufMut as _, BytesMut};
use chrono::{NaiveDate, TimeDelta, TimeZone};
use std::net::{Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

// ClickHouse `Date`s are represented as an unsigned 16-bit number of days from
// the UNIX epoch.
//
// This is deprecated, but we allow it so we can create a const. The fallible
// constructor requires unwrapping.
#[allow(deprecated)]
const EPOCH: NaiveDate = NaiveDate::from_ymd(1970, 1, 1);

// Maximum supported Date in ClickHouse.
//
// See https://clickhouse.com/docs/en/sql-reference/data-types/date
const MAX_DATE: &str = "2149-06-06";

// Maximum supported DateTime in ClickHouse.
//
// See https://clickhouse.com/docs/en/sql-reference/data-types/datetime.
const MAX_DATETIME: &str = "2106-02-07 06:28:15";

// Maximum supported DateTime64 in ClickHouse
//
// See https://clickhouse.com/docs/en/sql-reference/data-types/datetime64.
const MAX_DATETIME64: &str = "2299-12-31 23:59:59.99999999";

/// Helper macro to quickly and unsafely copy POD data from a message from the
/// ClickHouse server into our own column data types.
macro_rules! copyin_pod_values_raw {
    ($data_type:ty, $src:expr, $n_rows:expr) => {{
        // Compute the number of total bytes, and try to split the source
        // buffer at that value. We fail if we have a short read.
        let n_bytes = $n_rows * core::mem::size_of::<$data_type>();
        let Some((data, rest)) = $src.split_at_checked(n_bytes) else {
            return Ok(None);
        };

        // Preallocate a vec with the data, and create a MaybeUninit wrapper
        // around it.
        //
        // This is used to signal to the compiler that the values here are
        // uninitialized at first, though they point to valid and aligned
        // regions of memory.
        let mut values = Vec::<$data_type>::with_capacity($n_rows);
        let remaining = values.spare_capacity_mut();

        // Memcpy the data from the I/O buffer into our array.
        //
        // Safety: `copy_nonoverlapping` has a lot of invariants that it
        // expects.
        //
        // - Lengths: We have checked the length of the source buffer above, and
        // we've allocated the destination just above as well.
        // - Overlap: These cannot overlap, because we've just allocated the new
        // destination above.
        // - Alignment: This one is important. We're copying a buf of u8 into
        // something with possibly larger alignment. So the source buffer may
        // not have alignment of the destination. However, the reverse is always
        // true: the destination buffer is always aligned to a u8 buffer,
        // because all destination types are >= 1 byte. So the ordering of the
        // calls to `cast()` below is critical, we _must_ cast the destination
        // values to `*mut u8`, not the source to `*const T`. Note that because
        // of this, the count argument is in _bytes_, not rows.
        unsafe {
            core::ptr::copy_nonoverlapping(
                data.as_ptr(),
                remaining.as_mut_ptr().cast(),
                n_bytes,
            );
        }

        // Unsafely set the length to the number of values we copied.
        //
        // Safety: This is used for POD types, so any bit pattern should be
        // valid. But even if that were not true, just above we overwrote every
        // element, in the call to `copy_nonoverlapping`.
        unsafe {
            values.set_len($n_rows);
        }

        // Chop the prefix out of the source buffer.
        *$src = rest;
        values
    }};
}

macro_rules! copyin_pod_as_values {
    ($data_type:ty, $src:expr, $n_rows:expr) => {{
        ValueArray::from(copyin_pod_values_raw!($data_type, $src, $n_rows))
    }};
}

/// Decode an array of values in a column.
fn decode_value_array(
    src: &mut &[u8],
    n_rows: usize,
    data_type: &DataType,
) -> Result<Option<ValueArray>, Error> {
    let values = match data_type {
        DataType::UInt8 => copyin_pod_as_values!(u8, src, n_rows),
        DataType::UInt16 => copyin_pod_as_values!(u16, src, n_rows),
        DataType::UInt32 => copyin_pod_as_values!(u32, src, n_rows),
        DataType::UInt64 => copyin_pod_as_values!(u64, src, n_rows),
        DataType::UInt128 => copyin_pod_as_values!(u128, src, n_rows),
        DataType::Int8 => copyin_pod_as_values!(i8, src, n_rows),
        DataType::Int16 => copyin_pod_as_values!(i16, src, n_rows),
        DataType::Int32 => copyin_pod_as_values!(i32, src, n_rows),
        DataType::Int64 => copyin_pod_as_values!(i64, src, n_rows),
        DataType::Int128 => copyin_pod_as_values!(i128, src, n_rows),
        DataType::Float32 => copyin_pod_as_values!(f32, src, n_rows),
        DataType::Float64 => copyin_pod_as_values!(f64, src, n_rows),
        DataType::String => {
            // Strings are each encoded the same way as any other string in the
            // protocol: a varuint with the length, followed by the UTF8 bytes
            // of the string. This one we'll do the slow and safe way, which is
            // really required because we can't know how many bytes each string
            // is without decoding the variable length prefix.
            let mut values = Vec::with_capacity(n_rows);
            for _ in 0..n_rows {
                let Some(s) = io::string::decode(src)? else {
                    return Ok(None);
                };
                values.push(s);
            }
            ValueArray::from(values)
        }
        DataType::Uuid => {
            // Similar to IPv6 addresses, ClickHouse encodes UUIDs directly
            // as 16-octet arrays. We can just decode them as POD.
            copyin_pod_as_values!(Uuid, src, n_rows)
        }
        DataType::Ipv4 => {
            // ClickHouse encodes IPv4 addresses as u32s, but they are
            // unforunately little-endian. That means we can't just
            // transmute those values to IPv4 addresses in Rust, since that
            // representation is effectively a `[u8; 4]`, with _big_ endian
            // octets.
            //
            // It's possible that we can reinterpret the array as a slice of
            // u32s, but it's unlikely it has the correct alignment. Instead,
            // we'll take the slower path of iterating over 4-byte chunks and
            // creating the IP addresses directly.
            let n_bytes = n_rows * std::mem::size_of::<Ipv4Addr>();
            let Some((data, rest)) = src.split_at_checked(n_bytes) else {
                return Ok(None);
            };
            let mut values = Vec::with_capacity(n_rows);
            for chunk in data.chunks_exact(std::mem::size_of::<Ipv4Addr>()) {
                values.push(Ipv4Addr::new(
                    chunk[3], chunk[2], chunk[1], chunk[0],
                ));
            }
            *src = rest;
            ValueArray::from(values)
        }
        DataType::Ipv6 => {
            // IPv6 addresses are different. ClickHouse encodes them
            // directly as [u8; 16], which is much more sane than it's
            // encoding of IPv4 addresses.
            copyin_pod_as_values!(Ipv6Addr, src, n_rows)
        }
        DataType::Date => {
            // Dates are stored as 16-bit unsigned values, giving the number of
            // days since the UNIX epoch.
            let days = copyin_pod_values_raw!(u16, src, n_rows);
            let mut out = Vec::with_capacity(days.len());
            for day in days.into_iter() {
                out.push(EPOCH + TimeDelta::days(i64::from(day)));
            }
            ValueArray::Date(out)
        }
        DataType::DateTime(tz) => {
            // DateTimes are encoded as little-endian u32s, giving a traditional
            // UNIX timestamp with 1 second resolution. Similar to IPv4
            // addresses, we'll iterate in chunks and then convert.
            let n_bytes = n_rows * std::mem::size_of::<u32>();
            let Some((data, rest)) = src.split_at_checked(n_bytes) else {
                return Ok(None);
            };
            let mut values = Vec::with_capacity(n_rows);
            for chunk in data.chunks_exact(std::mem::size_of::<u32>()) {
                // Safety: Because we split this above on `n_bytes`, we know
                // this has exactly `n_rows` chunks of 4 bytes each.
                let timestamp = u32::from_le_bytes(chunk.try_into().unwrap());

                // Safety: This only panics if the timestamp is out of range,
                // which is not possible as this is actually a u32.
                values.push(tz.timestamp_opt(i64::from(timestamp), 0).unwrap());
            }
            *src = rest;
            ValueArray::DateTime { tz: *tz, values }
        }
        DataType::DateTime64(precision, timezone) => {
            // DateTime64s are encoded as little-endian i64s, but their
            // precision is encoded in the argument, not the column itself.
            // We'll iterate over chunks of the provided data again, and convert
            // the values.
            let n_bytes = n_rows * std::mem::size_of::<i64>();
            let Some((data, rest)) = src.split_at_checked(n_bytes) else {
                return Ok(None);
            };
            let mut values = Vec::with_capacity(n_rows);
            // The precision determines how to convert these values. Most things
            // should be 3, 6, or 9, for milliseconds, microseconds, or
            // nanoseconds. But technically any precision in [0, 9] is possible.
            let conv = precision.as_conv(timezone);
            for chunk in data.chunks_exact(std::mem::size_of::<i64>()) {
                // Safety: Because we split this above on `n_bytes`, we know
                // this has exactly `n_rows` chunks of 8 bytes each.
                let timestamp = i64::from_le_bytes(chunk.try_into().unwrap());

                // Safety: This only panics if the timestamp is out of range,
                // which is not possible as this is actually a u32.
                values.push(conv(timezone, timestamp));
            }
            *src = rest;
            ValueArray::DateTime64 {
                precision: *precision,
                tz: *timezone,
                values,
            }
        }
        DataType::Enum8(variants) => {
            // Copy the encoded variant indices themselves, and include the
            // variant mappings from the data type.
            let values = copyin_pod_values_raw!(i8, src, n_rows);
            ValueArray::Enum8 { variants: variants.clone(), values }
        }
        DataType::Nullable(inner) => {
            // Decode the null mask.
            let is_null = copyin_pod_values_raw!(bool, src, n_rows);
            // Then recurse to copyin the raw data itself. This should never
            // recurse more than once, both because we assert that here and
            // because ClickHouse flattens nested Nullable types into one
            // "layer" of Nullable.
            assert!(!inner.is_nullable());
            let Some(values) = decode_value_array(src, n_rows, inner)? else {
                return Ok(None);
            };
            ValueArray::Nullable { is_null, values: Box::new(values) }
        }
        DataType::Array(inner) => {
            // Arrays are encoded as two sequences back to back:
            //
            // - A list of u64s with the "offsets" of each array. These
            // describe the offset to the _next_ element. E.g., the first
            // element gives the offset to the second, etc. That implies that
            // their successive differences equals the size of each array.
            //
            // - The list of actual data, with all arrays flattened.
            let offsets = copyin_pod_values_raw!(u64, src, n_rows);
            let mut values = Vec::with_capacity(n_rows);
            let mut last_offset = 0;
            for offset in offsets.into_iter() {
                let size = usize::try_from(offset - last_offset).unwrap();
                last_offset = offset;
                let Some(arr) = decode_value_array(src, size, inner)? else {
                    return Ok(None);
                };
                values.push(arr);
            }
            ValueArray::Array { inner_type: *inner.clone(), values }
        }
    };
    Ok(Some(values))
}

/// Decode a column with a known type, if possible.
pub fn decode(
    src: &mut &[u8],
    n_rows: usize,
) -> Result<Option<(String, Column)>, Error> {
    let Some(name) = io::string::decode(src)? else {
        return Ok(None);
    };
    let Some(type_str) = io::string::decode(src)? else {
        return Ok(None);
    };
    let data_type: DataType = type_str.parse()?;

    // Columns encode the name and type, and then a "serialization" tag that
    // indicates how the data might be serialized. This should be 0 (false),
    // meaning there is no custom serialization, but it might depend on the
    // type.
    //
    // See https://github.com/ClickHouse/ClickHouse/blob/98a2c1c638c2ff9cea36e68c9ac16b3cf142387b/src/Formats/NativeWriter.cpp#L154-L179
    if !src.has_remaining() {
        return Ok(None);
    }
    if src.get_u8() != 0 {
        return Err(Error::UnsupportedProtocolFeature("custom serialization"));
    }

    // Decode the raw data itself.
    let Some(values) = decode_value_array(src, n_rows, &data_type)? else {
        return Ok(None);
    };
    let col = Column { values, data_type };
    Ok(Some((name, col)))
}

/// Helper macro to quickly and unsafely copy out POD data from our column into
/// a destination buffer meant for the server.
macro_rules! copyout_pod_values {
    ($data_type:ty, $values:expr, $dst:expr) => {{
        let n_bytes = $values.len() * std::mem::size_of::<$data_type>();
        let as_bytes = unsafe {
            std::slice::from_raw_parts($values.as_ptr().cast(), n_bytes)
        };
        $dst.put(as_bytes);
    }};
}

/// Encode a column with the provided name into the buffer.
///
/// # Panics
///
/// This panics if the data type is unsupported. Use `DataType::is_supported()`
/// to check that first.
pub fn encode(
    name: &str,
    column: Column,
    mut dst: &mut BytesMut,
) -> Result<(), Error> {
    assert!(column.data_type.is_supported());
    io::string::encode(name, &mut dst);
    io::string::encode(column.data_type.to_string(), &mut dst);
    // Encode the "custom serialization tag". See `decode` for details.
    dst.put_u8(0);
    encode_value_array(column.values, dst)
}

/// Encode an array of values into a buffer.
fn encode_value_array(
    values: ValueArray,
    mut dst: &mut BytesMut,
) -> Result<(), Error> {
    match values {
        ValueArray::UInt8(values) => dst.put(values.as_slice()),
        ValueArray::UInt16(values) => copyout_pod_values!(u16, values, dst),
        ValueArray::UInt32(values) => copyout_pod_values!(u32, values, dst),
        ValueArray::UInt64(values) => copyout_pod_values!(u64, values, dst),
        ValueArray::UInt128(values) => copyout_pod_values!(u128, values, dst),
        ValueArray::Int8(values) => copyout_pod_values!(i8, values, dst),
        ValueArray::Int16(values) => copyout_pod_values!(i16, values, dst),
        ValueArray::Int32(values) => copyout_pod_values!(i32, values, dst),
        ValueArray::Int64(values) => copyout_pod_values!(i64, values, dst),
        ValueArray::Int128(values) => copyout_pod_values!(i128, values, dst),
        ValueArray::Float32(values) => copyout_pod_values!(f32, values, dst),
        ValueArray::Float64(values) => copyout_pod_values!(f64, values, dst),
        ValueArray::String(values) => {
            // Strings are always encoded in the usual way, with a varuint
            // prefix. We cannot even preallocate accurately, since we don't
            // know the encoded size of all those lengths. We'll do the best we
            // can though.
            dst.reserve(values.len());
            for value in values {
                io::string::encode(value, &mut dst);
            }
        }
        ValueArray::Uuid(values) => copyout_pod_values!(Uuid, values, dst),
        ValueArray::Ipv4(values) => {
            // IPv4 addresses are always little-endian u32s for some reason.
            dst.reserve(values.len() * std::mem::size_of::<u32>());
            for value in values {
                dst.put_u32_le(u32::from(value));
            }
        }
        ValueArray::Ipv6(values) => copyout_pod_values!(Ipv6Addr, values, dst),
        ValueArray::Date(values) => {
            // Dates are represented in ClickHouse as a 16-bit unsigned number
            // of days since the UNIX epoch.
            //
            // Since these can be constructed from any `NaiveDate`, they can
            // have wider values than ClickHouse supports. Check that here
            // during conversion to the `u16` format.
            dst.reserve(values.len() * std::mem::size_of::<u16>());
            for value in values {
                let days = value.signed_duration_since(EPOCH).num_days();
                let days =
                    u16::try_from(days).map_err(|_| Error::OutOfRange {
                        type_name: String::from("Date"),
                        min: EPOCH.to_string(),
                        max: MAX_DATE.to_string(),
                        value: value.to_string(),
                    })?;
                dst.put_u16_le(days);
            }
        }
        ValueArray::DateTime { values, .. } => {
            // DateTimes are always little-endian u32s giving the UNIX
            // timestamp.
            for value in values {
                // DateTime's in ClickHouse must fit in a u32, so validate the
                // range here.
                let val = u32::try_from(value.timestamp()).map_err(|_| {
                    Error::OutOfRange {
                        type_name: String::from("DateTime"),
                        min: EPOCH.and_hms_opt(0, 0, 0).unwrap().to_string(),
                        max: MAX_DATETIME.to_string(),
                        value: value.to_string(),
                    }
                })?;
                dst.put_u32_le(val);
            }
        }
        ValueArray::DateTime64 { precision, values, .. } => {
            // DateTime64s are always encoded as i64s, in whatever
            // resolution is defined by the column type itself.
            dst.reserve(values.len() * std::mem::size_of::<i64>());
            for value in values {
                let Some(timestamp) = precision.scale(value) else {
                    return Err(Error::OutOfRange {
                        type_name: String::from("DateTime64"),
                        min: EPOCH.to_string(),
                        max: MAX_DATETIME64.to_string(),
                        value: value.to_string(),
                    });
                };
                dst.put_i64_le(timestamp);
            }
        }
        ValueArray::Nullable { is_null, values } => {
            copyout_pod_values!(bool, is_null, dst);
            encode_value_array(*values, dst)?;
        }
        ValueArray::Enum8 { values, .. } => {
            copyout_pod_values!(i8, values, dst)
        }
        ValueArray::Array { values: arrays, .. } => {
            // Arrays are encoded as two sequences: a list of offsets to each
            // array, plus the flattened data itself.
            encode_array_offsets(&arrays, dst);
            for array in arrays {
                encode_value_array(array, dst)?;
            }
        }
    }
    Ok(())
}

// Encode the column offsets for an array column into the provided buffer.
//
// ClickHouse encodes array columns with two sequences:
//
// - An array of offsets, where each element points to the start of the _next_
// array. This means the last entry is the length of the entire column.
//
// - The flattened array of all elements themselves.
//
// This method encodes the first of these into the output buffer. It's really
// just inserting the running sum of the lengths of all the provided arrays.
fn encode_array_offsets(arrays: &[ValueArray], dst: &mut BytesMut) {
    let mut current_offset = 0;
    for arr in arrays {
        current_offset += u64::try_from(arr.len()).unwrap();
        dst.put_u64_le(current_offset);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::block::Precision;
    use chrono::SubsecRound as _;
    use chrono::TimeZone;
    use chrono_tz::Tz;

    #[test]
    fn test_decode_uint8_column() {
        let data = b"\
           \x03foo\x05UInt8\
           \x00\
           \x00\x01\x02\
       ";
        let mut src = data.as_slice();
        let (name, col) = decode(&mut src, 3)
            .expect("Should be infallible")
            .expect("Should have read data column");
        assert_eq!(name, "foo");
        let ValueArray::UInt8(values) = &col.values else {
            panic!("Should have decoded UInt8 column, found {col:?}");
        };
        assert_eq!(values, &[0, 1, 2]);
    }

    #[test]
    fn test_decode_uint64_column() {
        let data = b"\
            \x03foo\x06UInt64\
            \x00\
            \x00\x00\x00\x00\x00\x00\x00\x00\
            \x01\x00\x00\x00\x00\x00\x00\x00\
            \x02\x00\x00\x00\x00\x00\x00\x00\
        ";
        let mut src = data.as_slice();
        let (name, col) = decode(&mut src, 3)
            .expect("Should be infallible")
            .expect("Should have read data column");
        assert_eq!(name, "foo");
        let ValueArray::UInt64(values) = &col.values else {
            panic!("Should have decoded UInt64 column, found {col:?}");
        };
        assert_eq!(values, &[0, 1, 2]);
    }

    #[test]
    fn test_decode_ipv4addr_column() {
        // ClickHouse encodes IPv4 address as little-endian u32s.
        //
        // The IPv4 address in Rust expects a [u8; 4], so they bytes are
        // supposed to be big-endian. We need to swap them on deserialization.
        let addrs =
            vec![Ipv4Addr::new(192, 168, 1, 1), Ipv4Addr::new(192, 168, 1, 2)];
        let src = b"\x03foo\x04IPv4\x00";
        let src = [
            &src[..],
            &u32::from(addrs[0]).to_le_bytes()[..],
            &u32::from(addrs[1]).to_le_bytes()[..],
        ]
        .concat();
        let (name, col) = decode(&mut src.as_slice(), addrs.len())
            .expect("Should be infallible")
            .expect("Should have read data column");
        assert_eq!(name, "foo");
        let ValueArray::Ipv4(values) = &col.values else {
            panic!("Should have decoded IPv4 column, found {col:?}");
        };
        assert_eq!(values, &addrs);
    }

    #[test]
    fn test_decode_ipv6addr_column() {
        let addrs: Vec<Ipv6Addr> = vec![
            "fd00:1122:3344:5566:7788:99aa:bbcc:ddee".parse().unwrap(),
            "fd00:1122:3344:5566:7788:99aa:bbcc:ddff".parse().unwrap(),
        ];
        let src = b"\x03foo\x04IPv6\x00";
        let src =
            [&src[..], &addrs[0].octets()[..], &addrs[1].octets()[..]].concat();
        let (name, col) = decode(&mut src.as_slice(), addrs.len())
            .expect("Should be infallible")
            .expect("Should have read data column");
        assert_eq!(name, "foo");
        let ValueArray::Ipv6(values) = &col.values else {
            panic!("Should have decoded IPv6 column, found {col:?}");
        };
        assert_eq!(values, &addrs);
    }

    #[test]
    fn test_decode_uuid_column() {
        let ids = vec![
            uuid::uuid!("cdf3b321-d349-4f97-9f74-761b3b9bca16"),
            uuid::uuid!("cdf3b321-d349-4f97-9f74-761b3b9bca17"),
        ];
        let src = b"\x03foo\x04UUID\x00";
        let src =
            [&src[..], &ids[0].as_bytes()[..], &ids[1].as_bytes()[..]].concat();
        let (name, col) = decode(&mut src.as_slice(), ids.len())
            .expect("Should be infallible")
            .expect("Should have read data column");
        assert_eq!(name, "foo");
        let ValueArray::Uuid(values) = &col.values else {
            panic!("Should have decoded UUID column, found {col:?}");
        };
        assert_eq!(values, &ids);
    }

    #[test]
    fn test_decode_string_column() {
        let data = vec![String::from("foo"), String::from("fooble")];
        let src = b"\x03foo\x06String\x00\x03foo\x06fooble";
        let (name, col) = decode(&mut src.as_slice(), data.len())
            .expect("Should be infallible")
            .expect("Should have read data column");
        assert_eq!(name, "foo");
        let ValueArray::String(values) = &col.values else {
            panic!("Should have decoded String column, found {col:?}");
        };
        assert_eq!(&data, values);
    }

    #[test]
    fn test_encode_decode_column() {
        let now64 = Tz::UTC.timestamp_opt(0, 0).unwrap();
        let now = now64.trunc_subsecs(0);
        let precision = Precision::new(9).unwrap();
        for (typ, values) in [
            (DataType::UInt8, ValueArray::UInt8(vec![0, 1, 2])),
            (DataType::UInt16, ValueArray::UInt16(vec![0, 1, 2])),
            (DataType::UInt32, ValueArray::UInt32(vec![0, 1, 2])),
            (DataType::UInt64, ValueArray::UInt64(vec![0, 1, 2])),
            (DataType::UInt128, ValueArray::UInt128(vec![0, 1, 2])),
            (DataType::Int8, ValueArray::Int8(vec![0, 1, 2])),
            (DataType::Int16, ValueArray::Int16(vec![0, 1, 2])),
            (DataType::Int32, ValueArray::Int32(vec![0, 1, 2])),
            (DataType::Int64, ValueArray::Int64(vec![0, 1, 2])),
            (DataType::Int128, ValueArray::Int128(vec![0, 1, 2])),
            (DataType::Float32, ValueArray::Float32(vec![0.0, 1.0, 2.0])),
            (DataType::Float64, ValueArray::Float64(vec![0.0, 1.0, 2.0])),
            (
                DataType::String,
                ValueArray::String(vec![
                    String::from("foo"),
                    String::from("bar"),
                ]),
            ),
            (
                DataType::Uuid,
                ValueArray::Uuid(vec![Uuid::new_v4(), Uuid::new_v4()]),
            ),
            (DataType::Ipv4, ValueArray::Ipv4(vec![Ipv4Addr::LOCALHOST])),
            (DataType::Ipv6, ValueArray::Ipv6(vec![Ipv6Addr::LOCALHOST])),
            (DataType::Date, ValueArray::Date(vec![now.date_naive()])),
            (
                DataType::DateTime(Tz::UTC),
                ValueArray::DateTime { tz: Tz::UTC, values: vec![now] },
            ),
            (
                DataType::DateTime64(precision, Tz::UTC),
                ValueArray::DateTime64 {
                    precision,
                    tz: Tz::UTC,
                    values: vec![now64],
                },
            ),
            (
                DataType::Nullable(Box::new(DataType::UInt8)),
                ValueArray::Nullable {
                    is_null: vec![true, false],
                    values: Box::new(ValueArray::UInt8(vec![0, 1])),
                },
            ),
            // Array(UInt8)
            (
                DataType::Array(Box::new(DataType::UInt8)),
                ValueArray::Array {
                    inner_type: DataType::UInt8,
                    values: vec![
                        ValueArray::UInt8(vec![0, 1]),
                        ValueArray::UInt8(vec![1, 2]),
                    ],
                },
            ),
            // Array(Nullable(UInt8))
            (
                DataType::Array(Box::new(DataType::Nullable(Box::new(
                    DataType::UInt8,
                )))),
                ValueArray::Array {
                    inner_type: DataType::Nullable(Box::new(DataType::UInt8)),
                    values: vec![ValueArray::Nullable {
                        is_null: vec![true, false],
                        values: Box::new(ValueArray::UInt8(vec![0, 1])),
                    }],
                },
            ),
            // Array(Array(UInt8))
            (
                DataType::Array(Box::new(DataType::Array(Box::new(
                    DataType::UInt8,
                )))),
                ValueArray::Array {
                    inner_type: DataType::Array(Box::new(DataType::UInt8)),
                    values: vec![ValueArray::Array {
                        inner_type: DataType::UInt8,
                        values: vec![ValueArray::UInt8(vec![0, 1])],
                    }],
                },
            ),
        ] {
            let n_rows = values.len();
            let col = Column { values, data_type: typ.clone() };
            let mut buf = BytesMut::new();
            encode("foo", col.clone(), &mut buf).unwrap();
            let (name, decoded) = decode(&mut &buf[..], n_rows)
                .expect("Should have succeeded in decoding full column")
                .unwrap_or_else(|| {
                    panic!("Should have decoded full column of type '{typ}'")
                });
            assert_eq!(name, "foo");
            assert_eq!(
                col,
                decoded,
                "Failed encode/decode round-trip for column with type '{typ:?}'"
            );
        }
    }

    #[test]
    fn fail_to_encode_out_of_range_column() {
        let max = Tz::from_utc_datetime(
            &Tz::UTC,
            &chrono::DateTime::<Tz>::MAX_UTC.naive_utc(),
        );
        let precision = Precision::new(9).unwrap();
        // See https://clickhouse.com/docs/en/sql-reference/data-types/datetime
        // and related pages for the supported ranges of these types.
        for (typ, values) in [
            (DataType::Date, ValueArray::Date(vec![max.date_naive()])),
            (
                DataType::DateTime(Tz::UTC),
                ValueArray::DateTime { tz: Tz::UTC, values: vec![max] },
            ),
            (
                DataType::DateTime64(precision, Tz::UTC),
                ValueArray::DateTime64 {
                    precision,
                    tz: Tz::UTC,
                    values: vec![max],
                },
            ),
        ] {
            let col = Column { values, data_type: typ.clone() };
            let mut buf = BytesMut::new();
            let err = encode("foo", col.clone(), &mut buf)
                .expect_err("Should fail to encode date-like column with out of range value");
            assert!(matches!(err, Error::OutOfRange { .. }));
        }
    }
}
