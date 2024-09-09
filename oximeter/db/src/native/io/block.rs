// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Encoding and decoding data blocks.

use crate::native::block::{Block, BlockInfo};
use crate::native::io;
use crate::native::Error;
use bytes::{Buf as _, BufMut as _, BytesMut};
use indexmap::IndexMap;

/// Encode a data packet to the server.
pub fn encode(block: Block, mut dst: &mut BytesMut) -> Result<(), Error> {
    for type_ in block.data_types() {
        if !type_.is_supported() {
            return Err(Error::UnsupportedDataType(type_.to_string()));
        }
    }
    io::string::encode(&block.name, &mut dst);
    encode_block_info(block.info, dst);
    io::varuint::encode(block.n_columns, &mut dst);
    io::varuint::encode(block.n_rows, &mut dst);
    for (name, col) in block.columns {
        io::column::encode(&name, col, &mut dst);
    }
    Ok(())
}

/// Decode a Data packet from the server, if possible.
pub fn decode(src: &mut &[u8]) -> Result<Option<Block>, Error> {
    let Some(name) = io::string::decode(src)? else {
        return Ok(None);
    };
    let Some(info) = decode_block_info(src)? else {
        return Ok(None);
    };
    let Some(n_columns) = io::varuint::decode(src) else {
        return Ok(None);
    };
    let Some(n_rows) = io::varuint::decode(src) else {
        return Ok(None);
    };
    let mut columns = IndexMap::with_capacity(n_columns as _);
    for _ in 0..n_columns {
        let Some((name, col)) = io::column::decode(src, n_rows as _)? else {
            return Ok(None);
        };
        columns.insert(name, col);
    }
    Ok(Some(Block { name, info, n_columns, n_rows, columns }))
}

/// Decode a `BlockInfo` struct, if possible.
fn decode_block_info(src: &mut &[u8]) -> Result<Option<BlockInfo>, Error> {
    let Some(field_num) = io::varuint::decode(src) else {
        return Ok(None);
    };
    if field_num != BlockInfo::OVERFLOW_FIELD_NUM {
        return Err(Error::UnsupportedProtocolFeature("block info field"));
    };
    if !src.has_remaining() {
        return Ok(None);
    }
    let is_overflows = src.get_u8() == 1;

    let Some(field_num) = io::varuint::decode(src) else {
        return Ok(None);
    };
    if field_num != BlockInfo::BUCKET_FIELD_NUM {
        return Err(Error::UnsupportedProtocolFeature("block info field"));
    };
    // +1 for extra byte at the end
    if src.len() < std::mem::size_of::<i32>() + 1 {
        return Ok(None);
    }
    let bucket_num = src.get_i32_le();
    let _ = src.get_u8();
    Ok(Some(BlockInfo { is_overflows, bucket_num }))
}

/// Encode a block info into the buffer.
///
/// Block info is encoded in quite a different format from all the other parts
/// of a packet. It uses a kind of key-value pair representation, where the
/// "field number" is written first, followed by its value. The field number is
/// a varuint, and then the actual field value is data-dependent. Finally, a
/// zero byte is used to signal the end of the block info.
///
/// See
/// https://github.com/ClickHouse/ClickHouse/blob/98a2c1c638c2ff9cea36e68c9ac16b3cf142387b/src/Core/BlockInfo.cpp#L21 for details on this encoding.
fn encode_block_info(info: BlockInfo, mut dst: &mut BytesMut) {
    io::varuint::encode(BlockInfo::OVERFLOW_FIELD_NUM, &mut dst);
    dst.put_u8(u8::from(info.is_overflows));
    io::varuint::encode(BlockInfo::BUCKET_FIELD_NUM, &mut dst);
    dst.put_i32_le(info.bucket_num);
    dst.put_u8(0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::block::{Column, ValueArray};

    // Expected data block.
    //
    //  - name of the block
    //  - is_overflows field num
    //  - is_overflows vlaue
    //  - bucket_num field num
    //  - bucket_num value
    //  - zero byte
    //  - number of columns
    //  - number of rows
    //  - array of columns, which are:
    //      - name
    //      - type
    //      - custom serialization tag
    //      - data
    const BLOCK: &[u8] = b"\
        \x05block\
        \x01\x00\
        \x02\x00\x00\x00\x00\
        \x00\
        \x01\
        \x03\
        \x03foo\
        \x05UInt8\
        \x00\
        \x00\x01\x02\
    ";

    #[test]
    fn test_decode_full_block() {
        let mut src = BLOCK;
        let block = decode(&mut src)
            .expect("Should succeed in decoding full data block")
            .expect("Should have decoded a full data block");
        assert_eq!(block.name, "block");
        assert_eq!(block.info.is_overflows, false);
        assert_eq!(block.info.bucket_num, 0);
        assert_eq!(block.n_columns, 1);
        assert_eq!(block.n_rows, 3);
        let col =
            block.columns.get("foo").expect("Should have a column named 'foo'");
        let ValueArray::UInt8(values) = &col.values else {
            panic!("Should have decoded a UInt8 column, found {col:?}");
        };
        assert_eq!(values, &[0, 1, 2]);
    }

    // Expected _nullable_ data block.
    //
    //  - name of the block
    //  - is_overflows field num
    //  - is_overflows vlaue
    //  - bucket_num field num
    //  - bucket_num value
    //  - zero byte
    //  - number of columns
    //  - number of rows
    //  - array of columns, which are:
    //      - name
    //      - type
    //      - custom serialization tag
    //      - ARRAY OF NULL MASKS
    //      - data
    const NULLABLE_BLOCK: &[u8] = b"\
        \x05block\
        \x01\x00\
        \x02\x00\x00\x00\x00\
        \x00\
        \x01\
        \x03\
        \x03foo\
        \x0fNullable(UInt8)\
        \x00\
        \x00\x01\x00\
        \x00\x01\x02\
    ";

    #[test]
    fn test_decode_block_with_nulls() {
        let mut src = NULLABLE_BLOCK;
        let block = decode(&mut src)
            .expect("Should succeed in decoding full data block")
            .expect("Should have decoded a nullable data block");
        assert_eq!(block.name, "block");
        assert_eq!(block.info.is_overflows, false);
        assert_eq!(block.info.bucket_num, 0);
        assert_eq!(block.n_columns, 1);
        assert_eq!(block.n_rows, 3);
        let (name, col) =
            block.columns.into_iter().next().expect("Should have one column");
        assert_eq!(name, "foo", "Should have a column named 'foo'");
        let Column { values, data_type } = col;
        let ValueArray::Nullable { is_null, values } = values else {
            panic!("Should have decoded Nullable column, found {values:?}");
        };
        assert_eq!(is_null, &[false, true, false]);
        let ValueArray::UInt8(values) = *values else {
            panic!(
                "Should have decoded a UInt8 column, found type {data_type:?}"
            );
        };
        assert_eq!(values, &[0, 1, 2]);
    }
}
