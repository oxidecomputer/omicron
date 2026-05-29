// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared helpers for rendering `serde_json::Value`s as bulleted lists in
//! `Display` impls.

use std::fmt;

/// Recursively format a JSON value as a bulleted list entry, nesting any
/// object or array children as indented sub-bullets.
pub(crate) fn fmt_json_value(
    f: &mut fmt::Formatter<'_>,
    key: &str,
    value: &serde_json::Value,
    indent: usize,
) -> fmt::Result {
    match value {
        serde_json::Value::Object(map) => {
            writeln!(f, "{:indent$}* {key}:", "")?;
            for (k, v) in map {
                fmt_json_value(f, k, v, indent + 2)?;
            }
            Ok(())
        }
        serde_json::Value::Array(arr) => {
            writeln!(f, "{:indent$}* {key}:", "")?;
            let indent = indent + 2;
            for (i, v) in arr.iter().enumerate() {
                fmt_json_array_item(f, i + 1, v, indent)?;
            }
            Ok(())
        }
        serde_json::Value::String(s) => {
            writeln!(f, "{:indent$}* {key}: {s}", "")
        }
        serde_json::Value::Null => {
            writeln!(f, "{:indent$}* {key}: <none>", "")
        }
        serde_json::Value::Bool(b) => {
            writeln!(f, "{:indent$}* {key}: {b}", "")
        }
        serde_json::Value::Number(n) => {
            writeln!(f, "{:indent$}* {key}: {n}", "")
        }
    }
}

/// Format a single element of a JSON array as a numbered list item,
/// e.g. `1. value` for scalars or `1.` followed by indented children for
/// objects and nested arrays.
pub(crate) fn fmt_json_array_item(
    f: &mut fmt::Formatter<'_>,
    n: usize,
    value: &serde_json::Value,
    indent: usize,
) -> fmt::Result {
    match value {
        serde_json::Value::Object(map) => {
            writeln!(f, "{:indent$}{n}.", "")?;
            for (k, v) in map {
                fmt_json_value(f, k, v, indent + 2)?;
            }
            Ok(())
        }
        serde_json::Value::Array(arr) => {
            writeln!(f, "{:indent$}{n}.", "")?;
            let indent = indent + 2;
            for (i, v) in arr.iter().enumerate() {
                fmt_json_array_item(f, i + 1, v, indent)?;
            }
            Ok(())
        }
        serde_json::Value::String(s) => {
            writeln!(f, "{:indent$}{n}. {s}", "")
        }
        serde_json::Value::Null => {
            writeln!(f, "{:indent$}{n}. <none>", "")
        }
        serde_json::Value::Bool(b) => {
            writeln!(f, "{:indent$}{n}. {b}", "")
        }
        serde_json::Value::Number(num) => {
            writeln!(f, "{:indent$}{n}. {num}", "")
        }
    }
}
