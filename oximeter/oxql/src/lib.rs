// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Target-independent parser for the Oximeter Query Language.

mod ast;
mod grammar;

use std::collections::BTreeSet;

use peg::error::ParseError as PegError;
use peg::str::LineCol;
use serde::Serialize;

pub const MAX_QUERY_LEN: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParseResult {
    pub diagnostics: Vec<Diagnostic>,
    pub referenced_timeseries: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Diagnostic {
    pub code: &'static str,
    pub phase: &'static str,
    pub severity: &'static str,
    pub message: String,
    pub line: Option<usize>,
    pub column: Option<usize>,
}

pub fn parse(source: &str) -> ParseResult {
    let source = source.trim();
    if source.len() > MAX_QUERY_LEN {
        return ParseResult {
            diagnostics: vec![Diagnostic {
                code: "query-too-long",
                phase: "parse",
                severity: "error",
                message: format!(
                    "Queries must be <= {MAX_QUERY_LEN} characters"
                ),
                line: None,
                column: None,
            }],
            referenced_timeseries: Vec::new(),
        };
    }

    match grammar::query_parser::query(source) {
        Ok(query) => ParseResult {
            diagnostics: Vec::new(),
            referenced_timeseries: query
                .all_timeseries_names()
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        },
        Err(error) => ParseResult {
            diagnostics: vec![parse_diagnostic(source, error)],
            referenced_timeseries: Vec::new(),
        },
    }
}

fn parse_diagnostic(source: &str, error: PegError<LineCol>) -> Diagnostic {
    Diagnostic {
        code: "invalid-syntax",
        phase: "parse",
        severity: "error",
        message: format_parse_error(source, &error),
        line: Some(error.location.line),
        column: Some(error.location.column),
    }
}

fn format_parse_error(source: &str, error: &PegError<LineCol>) -> String {
    use std::fmt::Write;

    let mut out =
        format!("Error at {}:{}", error.location.line, error.location.column);
    const CONTEXT: usize = 24;
    let start = error.location.offset.saturating_sub(CONTEXT);
    let end = error.location.offset.saturating_add(CONTEXT).min(source.len());
    if let Some(context) = source.get(start..end) {
        let prefix_len = out.len() + 2;
        writeln!(out, ": .. {context} ..").unwrap();
        let left_pad = error.location.offset - start + 3 + prefix_len;
        let right_pad = end - error.location.offset + 3 + prefix_len;
        writeln!(out, "{:<left_pad$}^{:>right_pad$}", ' ', ' ').unwrap();
    }
    writeln!(out, "Expected: {error}").unwrap();
    out
}
