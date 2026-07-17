// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared helpers for rendering `serde_json::Value`s as bulleted lists in
//! `Display` impls.

use std::fmt;

/// Recursively format a JSON value as a bulleted list entry, nesting any
/// object or array children as indented sub-bullets.
pub struct Json<'json> {
    json: &'json serde_json::Value,
    indent: usize,
}

impl<'json> Json<'json> {
    pub fn new(json: &'json serde_json::Value) -> Self {
        Self { json, indent: 0 }
    }

    pub fn with_indent(mut self, indent: usize) -> Self {
        self.indent = indent;
        self
    }
}

impl fmt::Display for Json<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let indent = self.indent;
        match self.json {
            serde_json::Value::Object(map) => {
                for (k, v) in map {
                    fmt_json_value(f, k, v, indent)?;
                }
            }
            serde_json::Value::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    fmt_json_array_item(f, i + 1, v, indent)?;
                }
            }
            serde_json::Value::String(s) => {
                writeln!(f, "{:indent$}{s}", "")?;
            }
            serde_json::Value::Null => {
                writeln!(f, "{:indent$}<none>", "")?;
            }
            serde_json::Value::Bool(val) => {
                writeln!(f, "{:indent$}{val}", "")?;
            }
            serde_json::Value::Number(val) => {
                writeln!(f, "{:indent$}{val}", "")?;
            }
        }
        Ok(())
    }
}

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

/// Displays a (potentially multi-line) `comment` string with configurable
/// indentation and leading `// ` delimiters.
pub struct Comment<'c> {
    comment: &'c str,
    indent: usize,
    leading_newline: bool,
}

impl<'c> Comment<'c> {
    pub fn new(comment: &'c str, indent: usize) -> Self {
        Self { comment, indent, leading_newline: false }
    }

    pub fn indent(self, indent: usize) -> Self {
        Self { indent, ..self }
    }

    pub fn with_leading_newline(self) -> Self {
        Self { leading_newline: true, ..self }
    }
}

impl<'c> From<&'c String> for Comment<'c> {
    fn from(comment: &'c String) -> Self {
        Self::new(comment.as_str(), 0)
    }
}

impl<'c> From<&'c Option<String>> for Comment<'c> {
    fn from(comment: &'c Option<String>) -> Self {
        Self::from(comment.as_deref())
    }
}

impl<'c> From<Option<&'c String>> for Comment<'c> {
    fn from(comment: Option<&'c String>) -> Self {
        Self::from(comment.map(String::as_str))
    }
}

impl<'c> From<Option<&'c str>> for Comment<'c> {
    fn from(comment: Option<&'c str>) -> Self {
        Self::new(comment.unwrap_or(""), 0)
    }
}

impl std::fmt::Display for Comment<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self { comment, indent, leading_newline } = self;
        if comment.is_empty() {
            return Ok(());
        }
        if leading_newline {
            writeln!(f, "")?;
        }
        for line in comment.lines() {
            writeln!(f, "{:indent$}// {line}", "",)?;
        }
        Ok(())
    }
}
