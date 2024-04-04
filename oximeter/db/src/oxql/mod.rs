// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The Oximeter Query Language, OxQL.

// Copyright 2024 Oxide Computer Company

use peg::error::ParseError as PegError;
use peg::str::LineCol;

pub mod ast;
pub mod point;
pub mod query;
pub mod table;

pub use self::query::Query;
pub use self::table::Table;
pub use self::table::Timeseries;
pub use anyhow::Error;

// Format a PEG parsing error into a nice anyhow error.
fn fmt_parse_error(source: &str, err: PegError<LineCol>) -> Error {
    use std::fmt::Write;
    let mut out =
        format!("Error at {}:{}", err.location.line, err.location.column);
    const CONTEXT: usize = 24;
    let start = err.location.offset.saturating_sub(CONTEXT);
    let end = err.location.offset.saturating_add(CONTEXT).min(source.len());
    if let Some(context) = source.get(start..end) {
        let prefix_len = out.len() + 2;
        writeln!(out, ": .. {context} ..").unwrap();
        let left_pad = err.location.offset - start + 3 + prefix_len;
        let right_pad = end - err.location.offset + 3 + prefix_len;
        writeln!(out, "{:<left_pad$}^{:>right_pad$}", ' ', ' ').unwrap();
    }
    writeln!(out, "Expected: {}", err).unwrap();
    anyhow::anyhow!(out)
}
