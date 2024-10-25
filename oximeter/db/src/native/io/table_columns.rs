// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Decoding table column descriptions from the database.
//!
//! As is helpfully noted in the ClickHouse sources at
//! <https://github.com/ClickHouse/ClickHouse/blob/424ecb86e0f04a9422dd01f9216cdbe3c0e314e2/src/Storages/ColumnsDescription.cpp#L132>,
//!
//! > NOTE: Serialization format is insane.
//!
//! The server serializes an array of `ColumnDescription`s to help the client
//! validate data before inserting it. This is a pure-text format that includes
//! column names and types, but also comments, codecs, and TTL values. The
//! format is indeed pretty wonky, so we're using `nom` to help parse it
//! robustly. For now, we only care about the names and data types; the
//! remainder is collected as-is, without interpretation, into a generic
//! `details` field.

use super::string;
use crate::native::block::DataType;
use crate::native::packets::server::ColumnDescription;
use crate::native::Error;
use nom::bytes::streaming::is_not;
use nom::bytes::streaming::tag;
use nom::character::streaming::u64 as nom_u64;
use nom::error::ErrorKind;
use nom::sequence::delimited;
use nom::sequence::separated_pair;
use nom::sequence::terminated;
use nom::Err as NomErr;
use nom::IResult;

/// Decode an array of `ColumnDescription`s from a buffer.
pub fn decode(
    src: &mut &[u8],
) -> Result<Option<Vec<ColumnDescription>>, Error> {
    // See `src/Storages/ColumnDescription.cpp` for details of the encoding
    // here, but briefly:
    //
    // - The packet starts with a "header" describing the table name (usually
    // empty), the version, the number of columns.
    // - Each column is serialized as a string, with its name; type; comment;
    // compression codec; settings; statistics; and TTL
    // - These are all newline delimited, with tabs prefixing each of the items
    // listed above.
    //
    // See https://github.com/ClickHouse/ClickHouse/blob/c82cf25b3e5864bcc153cbe45adb8c6527e1ec6e/src/Server/TCPHandler.cpp#L2433
    // for more details, which is where the encoding of these values starts.
    let Some(_table_name) = string::decode(src)? else {
        return Ok(None);
    };

    // The column description itself is serialized as a ClickHouse
    // varuint-prefixed string. Deserialize this, and then parse out the pieces
    // of that parsed string.
    let Some(text) = string::decode(src)? else {
        return Ok(None);
    };
    column_descriptions(&text)
}

fn column_descriptions(
    text: &str,
) -> Result<Option<Vec<ColumnDescription>>, Error> {
    // Try to match the version "header"
    let text = match tag::<_, _, (_, ErrorKind)>("columns format version: 1\n")(
        text,
    ) {
        Ok((text, _match)) => text,
        Err(NomErr::Incomplete(_)) => return Ok(None),
        Err(_) => return Err(Error::InvalidPacket("TableColumns (header)")),
    };

    // Match the number of columns.
    let (text, n_columns) = match column_count(text) {
        Ok(input) => input,
        Err(NomErr::Incomplete(_)) => return Ok(None),
        Err(_) => {
            return Err(Error::InvalidPacket("TableColumns (column count)"))
        }
    };

    // Extract each column, each of which is on a separate line.
    let mut out = Vec::with_capacity(
        usize::try_from(n_columns).map_err(|_| Error::BlockTooLarge)?,
    );
    for line in text.lines() {
        let col = match column_description(line) {
            Ok(out) => out.1,
            Err(NomErr::Incomplete(_)) => return Ok(None),
            Err(_) => {
                return Err(Error::InvalidPacket("TableColumns (description)"))
            }
        };
        out.push(col);
    }
    assert_eq!(out.len(), n_columns as usize);
    Ok(Some(out))
}

// Parse out the column count from a header line like: "3 columns:\n".
fn column_count(s: &str) -> IResult<&str, u64> {
    terminated(nom_u64, tag(" columns:\n"))(s)
}

// Parse a backtick-quoted column name like: "`foo.bar`".
fn backtick_quoted_column_name(s: &str) -> IResult<&str, &str> {
    delimited(tag("`"), is_not("`"), tag("`"))(s)
}

// Parse a full column description from one line.
//
// Note that this must _not_ end in a newline, so one should use something like
// `str::lines()` and pass the result to this method.
fn column_description(s: &str) -> IResult<&str, ColumnDescription> {
    let (s, (name, data_type)) = separated_pair(
        backtick_quoted_column_name,
        tag(" "),
        DataType::nom_parse,
    )(s)?;

    // At this point, we take any remaining output as the details, which may be
    // empty.
    Ok((
        "",
        ColumnDescription {
            name: name.to_string(),
            data_type,
            details: s.to_string(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::backtick_quoted_column_name;
    use super::column_count;
    use super::column_description;
    use super::column_descriptions;
    use super::NomErr;
    use crate::native::block::DataType;

    #[test]
    fn test_backtick_quoted_column_name() {
        assert_eq!(backtick_quoted_column_name("`foo`").unwrap().1, "foo");
        assert_eq!(
            backtick_quoted_column_name("`foo.bar`").unwrap().1,
            "foo.bar"
        );
        assert!(matches!(
            backtick_quoted_column_name("`foo").unwrap_err(),
            NomErr::Incomplete(_)
        ));
    }

    #[test]
    fn test_column_count() {
        assert_eq!(column_count("4 columns:\n").unwrap().1, 4,);
    }

    #[test]
    fn test_column_description_only_required_parts() {
        let desc = column_description("`timeseries_name` String").unwrap().1;
        assert_eq!(desc.name, "timeseries_name");
        assert_eq!(desc.data_type, DataType::String);
        assert!(desc.details.is_empty());
    }

    #[test]
    fn test_column_descriptions() {
        static INPUT: &str = r#"columns format version: 1
2 columns:
`timeseries_name` String
`fields.name` Array(String)
"#;
        let columns = column_descriptions(&mut &*INPUT)
            .expect("failed to decode column descriptions")
            .expect("expected Some(_) column description");
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "timeseries_name");
        assert_eq!(columns[0].data_type, DataType::String);
        assert_eq!(columns[1].name, "fields.name");
        assert_eq!(
            columns[1].data_type,
            DataType::Array(Box::new(DataType::String))
        );
    }

    #[test]
    fn test_column_description_with_default() {
        static INPUT: &str = r#"`timeseries_name` String\tDEFAULT "foo""#;
        let column = column_description(&mut &*INPUT)
            .expect("failed to decode column description")
            .1;
        assert_eq!(column.name, "timeseries_name");
        assert_eq!(column.data_type, DataType::String);
        assert_eq!(column.details, "\\tDEFAULT \"foo\"");
    }
}
