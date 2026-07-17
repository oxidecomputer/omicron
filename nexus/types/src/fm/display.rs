// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared helpers for rendering `serde_json::Value`s as bulleted lists in
//! `Display` impls, and for optionally styling FM displayer output with
//! ANSI terminal colors.

use owo_colors::{Style, Styled};
use std::fmt;

/// The palette of terminal styles used by the FM displayers.
///
/// When terminal colors are not enabled, all styles are no-ops, and styling a
/// value does nothing. When colors are enabled, each method on `Styles` may be
/// used to decorate a particular value in the style for the semantic meaning of
/// that value, such as [`Self::warning`] for warning lines or
/// [`Self::annotation`] for annotations.
///
/// ## Styling conventions:
///
/// Follow these if you want to make Eliza happy. Chosen completely based on my
/// personal preference. :)
///
/// * Punctuation that is not part of the text itself stays *outside* the
///   styled span: the `:` after a field label or section title is not
///   bolded, and neither are parenthesized counts like `(2 total)` in
///   section titles.
/// * The [missing style](Self::missing) is only for placeholders appearing
///   on the value side of a key-value pair (`foo: <none>`). A standalone
///   line whose body says a section is empty ("no open cases") is a
///   heading, not a missing value.
#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct Styles {
    colored: bool,
}

impl Styles {
    pub(crate) fn new(colored: bool) -> Self {
        Self { colored }
    }

    fn style(self, f: impl FnOnce(Style) -> Style) -> Style {
        if self.colored { f(Style::new()) } else { Style::new() }
    }

    /// Headings: field labels (e.g. "sitrep ID:"), section titles, item
    /// headers (e.g. "case <uuid>"), and the keys of key-value pairs.
    pub(crate) fn heading(self) -> Style {
        self.style(|s| s.bold())
    }

    /// `// comments`.
    pub(crate) fn comment(self) -> Style {
        self.style(|s| s.dimmed())
    }

    /// The `/!\ WARNING:` marker (and similar) prefixing warning lines.
    pub(crate) fn warning(self) -> Style {
        self.style(|s| s.red().bold())
    }

    /// The message text of a warning line.
    pub(crate) fn warning_text(self) -> Style {
        self.style(|s| s)
    }

    /// Annotations pointing at a value of note (e.g. "<-- this sitrep").
    pub(crate) fn annotation(self) -> Style {
        self.style(|s| s.cyan())
    }

    /// Placeholders for missing values ("<none>", "<UNKNOWN>") appearing on
    /// the value side of a key-value pair. Not for standalone notes that a
    /// section is empty; those are [headings](Self::heading).
    pub(crate) fn missing(self) -> Style {
        self.style(|s| s.dimmed())
    }

    /// Returns a displayer for a field label ending in `:`, styling
    /// everything before the colon as a [heading](Self::heading) and
    /// left-aligning the whole label (colon included, per `{text:<width$}`)
    /// to `width` visible columns. The colon and padding stay unstyled, per
    /// the conventions above.
    pub(crate) fn label<'s>(self, text: &'s str, width: usize) -> Label<'s> {
        Label { text, style: self.heading(), width }
    }

    /// Returns `value` unstyled if present, or `placeholder` in the
    /// [missing-value style](Self::missing) if not.
    pub(crate) fn or_missing<'s>(
        self,
        value: Option<&'s str>,
        placeholder: &'s str,
    ) -> Styled<&'s str> {
        match value {
            Some(value) => Style::new().style(value),
            None => self.missing().style(placeholder),
        }
    }

    /// Returns `text` in the [annotation style](Self::annotation) if `cond`
    /// holds, or the empty string (with no escape sequences) if it doesn't.
    pub(crate) fn annotation_if(self, cond: bool, text: &str) -> Styled<&str> {
        if cond {
            self.annotation().style(text)
        } else {
            Style::new().style("")
        }
    }
}

/// Displays a field label such as `diagnosis engine:`, as returned by
/// [`Styles::label`]: the label text bold, the trailing `:` and alignment
/// padding unstyled.
pub(crate) struct Label<'s> {
    text: &'s str,
    style: Style,
    width: usize,
}

impl fmt::Display for Label<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let &Self { text, style, width } = self;
        match text.strip_suffix(':') {
            Some(label) => write!(f, "{}:", style.style(label))?,
            None => write!(f, "{}", style.style(text))?,
        }
        // Pad outside the styled span, so that alignment whitespace does
        // not carry the style.
        let pad = width.saturating_sub(text.len());
        write!(f, "{:pad$}", "")
    }
}

/// Recursively format a JSON value as a bulleted list entry, nesting any
/// object or array children as indented sub-bullets.
pub struct Json<'json> {
    json: &'json serde_json::Value,
    indent: usize,
    colored: bool,
}

impl<'json> Json<'json> {
    pub fn new(json: &'json serde_json::Value) -> Self {
        Self { json, indent: 0, colored: false }
    }

    pub fn with_indent(mut self, indent: usize) -> Self {
        self.indent = indent;
        self
    }

    /// If `colored` is true, style the output with ANSI terminal colors.
    pub fn colored(mut self, colored: bool) -> Self {
        self.colored = colored;
        self
    }
}

impl fmt::Display for Json<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let indent = self.indent;
        let styles = Styles::new(self.colored);
        match self.json {
            serde_json::Value::Object(map) => {
                for (k, v) in map {
                    fmt_json_value(f, k, v, indent, styles)?;
                }
            }
            serde_json::Value::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    fmt_json_array_item(f, i + 1, v, indent, styles)?;
                }
            }
            serde_json::Value::String(s) => {
                writeln!(f, "{:indent$}{s}", "")?;
            }
            serde_json::Value::Null => {
                writeln!(
                    f,
                    "{:indent$}{}",
                    "",
                    styles.missing().style("<none>")
                )?;
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
    styles: Styles,
) -> fmt::Result {
    // Style the key like other headings; the `:` stays unstyled, which is
    // indistinguishable in practice and avoids formatting the key twice.
    let key = styles.heading().style(key);
    match value {
        serde_json::Value::Object(map) => {
            writeln!(f, "{:indent$}* {key}:", "")?;
            for (k, v) in map {
                fmt_json_value(f, k, v, indent + 2, styles)?;
            }
            Ok(())
        }
        serde_json::Value::Array(arr) => {
            writeln!(f, "{:indent$}* {key}:", "")?;
            let indent = indent + 2;
            for (i, v) in arr.iter().enumerate() {
                fmt_json_array_item(f, i + 1, v, indent, styles)?;
            }
            Ok(())
        }
        serde_json::Value::String(s) => {
            writeln!(f, "{:indent$}* {key}: {s}", "")
        }
        serde_json::Value::Null => {
            writeln!(
                f,
                "{:indent$}* {key}: {}",
                "",
                styles.missing().style("<none>")
            )
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
    styles: Styles,
) -> fmt::Result {
    match value {
        serde_json::Value::Object(map) => {
            writeln!(f, "{:indent$}{n}.", "")?;
            for (k, v) in map {
                fmt_json_value(f, k, v, indent + 2, styles)?;
            }
            Ok(())
        }
        serde_json::Value::Array(arr) => {
            writeln!(f, "{:indent$}{n}.", "")?;
            let indent = indent + 2;
            for (i, v) in arr.iter().enumerate() {
                fmt_json_array_item(f, i + 1, v, indent, styles)?;
            }
            Ok(())
        }
        serde_json::Value::String(s) => {
            writeln!(f, "{:indent$}{n}. {s}", "")
        }
        serde_json::Value::Null => {
            writeln!(
                f,
                "{:indent$}{n}. {}",
                "",
                styles.missing().style("<none>")
            )
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
    colored: bool,
}

impl<'c> Comment<'c> {
    pub fn new(comment: &'c str, indent: usize) -> Self {
        Self { comment, indent, leading_newline: false, colored: false }
    }

    pub fn indent(self, indent: usize) -> Self {
        Self { indent, ..self }
    }

    pub fn with_leading_newline(self) -> Self {
        Self { leading_newline: true, ..self }
    }

    /// If `colored` is true, dim the comment text.
    pub fn colored(self, colored: bool) -> Self {
        Self { colored, ..self }
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
        let &Self { comment, indent, leading_newline, colored } = self;
        if comment.is_empty() {
            return Ok(());
        }
        if leading_newline {
            writeln!(f, "")?;
        }
        let style = Styles::new(colored).comment();
        for line in comment.lines() {
            writeln!(
                f,
                "{:indent$}{}",
                "",
                style.style(format_args!("// {line}"))
            )?;
        }
        Ok(())
    }
}

/// Test helpers for asserting that colored displayer output differs from
/// uncolored output only in ANSI escape sequences.
#[cfg(test)]
pub(crate) mod test_utils {
    use std::fmt;

    /// Renders the `fmt::Display` output of the value constructed by
    /// `mk_displayer` with colors off and on, prints the colored output to
    /// stderr for manual inspection, and asserts that:
    ///
    /// * the plain (non-colored) `Display` output contains no ANSI escapes
    /// * the colored output, with SGR sequences stripped, is identical
    ///   to the plain one. Enabling colors should add styling but should not
    ///   change the underlying text.
    ///
    /// It also asserts that the colored output contains at least one escape
    /// (but deliberately not any *particular* escape, since updating assertions
    /// if we change how things are formatted is annoying). This only guards
    /// against the `colored` flag silently not being passed through.
    ///
    /// Taking a constructor closure rather than two pre-rendered strings
    /// guarantees the two formatted outputs come from the same displayer,
    /// differing only in the `colored` flag.
    ///
    /// Returns the plain output, for golden-file comparison.
    #[track_caller]
    pub(crate) fn check_colored_display<D: fmt::Display>(
        mk_displayer: impl Fn(bool) -> D,
    ) -> String {
        let plain = mk_displayer(false).to_string();
        let colored = mk_displayer(true).to_string();
        eprintln!("{colored}");
        assert!(
            !plain.contains('\x1b'),
            "uncolored output must not contain ANSI escapes:\n{}",
            ShowEscapes(&plain)
        );
        assert!(
            colored.contains('\x1b'),
            "colored output should contain at least one ANSI escape; \
             is the `colored` flag being passed through?\n{}",
            ShowEscapes(&colored)
        );
        assert_eq!(
            strip_sgr(&colored),
            plain,
            "colored output must differ from uncolored output only in \
             ANSI escape sequences"
        );
        plain
    }

    /// Displays a string with control characters *other than newlines*
    /// escaped, so that ANSI escape sequences in assertion failure messages
    /// are visible as text rather than interpreted by the terminal, while
    /// multi-line output doesn't get dumped out on one giant line.
    struct ShowEscapes<'a>(&'a str);

    impl fmt::Display for ShowEscapes<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            for c in self.0.chars() {
                if c == '\n' || !c.is_control() {
                    write!(f, "{c}")?;
                } else {
                    write!(f, "{}", c.escape_debug())?;
                }
            }
            Ok(())
        }
    }

    /// Strip ANSI SGR sequences (`ESC [ ... m`), the only kind of escape
    /// the `Display` implementations here emit.
    fn strip_sgr(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\x1b' {
                for c in chars.by_ref() {
                    if c == 'm' {
                        break;
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    }
}
