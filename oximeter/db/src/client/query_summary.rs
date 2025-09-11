// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing summaries of queries against the timeseries database.

// Copyright 2024 Oxide Computer Company

use crate::Error;
use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

/// A count of bytes / rows accessed during a query.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct IoCount {
    /// The number of bytes accessed.
    pub bytes: u64,
    /// The number of rows accessed.
    pub rows: u64,
}

impl std::fmt::Display for IoCount {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} rows ({} bytes)", self.rows, self.bytes)
    }
}

/// Summary of the I/O resources used by a query.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(try_from = "serde_json::Value")]
pub struct IoSummary {
    /// The bytes and rows read by the query.
    pub read: IoCount,
    /// The bytes and rows written by the query.
    pub written: IoCount,
}

impl TryFrom<serde_json::Value> for IoSummary {
    type Error = Error;

    fn try_from(j: serde_json::Value) -> Result<Self, Self::Error> {
        use serde_json::Map;
        use serde_json::Value;
        use std::str::FromStr;

        let Value::Object(map) = j else {
            return Err(Error::Database(String::from(
                "Expected a JSON object for a metadata summary",
            )));
        };

        fn unpack_summary_value<T>(
            map: &Map<String, Value>,
            key: &str,
        ) -> Result<T, Error>
        where
            T: FromStr,
            <T as FromStr>::Err: std::error::Error,
        {
            let value = map.get(key).ok_or_else(|| {
                Error::MissingHeaderKey { key: key.to_string() }
            })?;
            let Value::String(v) = value else {
                return Err(Error::BadMetadata {
                    key: key.to_string(),
                    msg: String::from("Expected a string value"),
                });
            };
            v.parse::<T>().map_err(|e| Error::BadMetadata {
                key: key.to_string(),
                msg: e.to_string(),
            })
        }
        let rows_read: u64 = unpack_summary_value(&map, "read_rows")?;
        let bytes_read: u64 = unpack_summary_value(&map, "read_bytes")?;
        let rows_written: u64 = unpack_summary_value(&map, "written_rows")?;
        let bytes_written: u64 = unpack_summary_value(&map, "written_bytes")?;
        Ok(Self {
            read: IoCount { bytes: bytes_read, rows: rows_read },
            written: IoCount { bytes: bytes_written, rows: rows_written },
        })
    }
}

/// Basic metadata about the resource usage of a single SQL query.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct QuerySummary {
    /// The database-assigned query ID.
    pub id: Uuid,
    /// The raw SQL query.
    pub query: String,
    /// The total duration of the query (network plus execution).
    pub elapsed: Duration,
    /// Summary of the data read and written.
    pub io_summary: IoSummary,
}

// TODO-remove: https://github.com/oxidecomputer/omicron/issues/7094
#[allow(dead_code)]
impl QuerySummary {
    /// Construct a SQL query summary from the headers received from the DB.
    pub(crate) fn from_headers(
        elapsed: Duration,
        headers: &HeaderMap,
    ) -> Result<Self, Error> {
        fn get_header<'a>(
            map: &'a HeaderMap,
            key: &'a str,
        ) -> Result<&'a str, Error> {
            let hdr = map.get(key).ok_or_else(|| Error::MissingHeaderKey {
                key: key.to_string(),
            })?;
            std::str::from_utf8(hdr.as_bytes())
                .map_err(|err| Error::Database(err.to_string()))
        }
        let summary =
            serde_json::from_str(get_header(headers, "X-ClickHouse-Summary")?)
                .map_err(|err| Error::Database(err.to_string()))?;
        let id = get_header(headers, "X-ClickHouse-Query-Id")?
            .parse()
            .map_err(|err: uuid::Error| Error::Database(err.to_string()))?;
        Ok(Self { id, query: String::from(""), elapsed, io_summary: summary })
    }
}
