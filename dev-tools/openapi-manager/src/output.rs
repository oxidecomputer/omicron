// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{fmt, fmt::Write, io};

use camino::Utf8Path;
use clap::{Args, ColorChoice};
use indent_write::fmt::IndentWriter;
use owo_colors::{OwoColorize, Style};
use similar::{ChangeTag, DiffableStr, TextDiff};

use crate::spec::{ApiSpec, ApiSpecFile, DocumentSummary};

#[derive(Debug, Args)]
#[clap(next_help_heading = "Global options")]
pub struct OutputOpts {
    /// Color output
    #[clap(long, value_enum, global = true, default_value_t)]
    pub(crate) color: ColorChoice,
}

impl OutputOpts {
    /// Returns true if color should be used for the stream.
    pub(crate) fn use_color(&self, stream: supports_color::Stream) -> bool {
        match self.color {
            ColorChoice::Auto => supports_color::on_cached(stream).is_some(),
            ColorChoice::Always => true,
            ColorChoice::Never => false,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Styles {
    pub(crate) bold: Style,
    pub(crate) header: Style,
    pub(crate) success_header: Style,
    pub(crate) failure: Style,
    pub(crate) failure_header: Style,
    pub(crate) warning_header: Style,
    pub(crate) unchanged_header: Style,
    pub(crate) filename: Style,
    pub(crate) diff_before: Style,
    pub(crate) diff_after: Style,
}

impl Styles {
    pub(crate) fn colorize(&mut self) {
        self.bold = Style::new().bold();
        self.header = Style::new().purple();
        self.success_header = Style::new().green().bold();
        self.failure = Style::new().red();
        self.failure_header = Style::new().red().bold();
        self.unchanged_header = Style::new().blue().bold();
        self.warning_header = Style::new().yellow().bold();
        self.filename = Style::new().cyan();
        self.diff_before = Style::new().red();
        self.diff_after = Style::new().green();
    }
}

// This is copied from similar's UnifiedDiff::to_writer, except with colorized
// output.
pub(crate) fn write_diff<'diff, 'old, 'new, 'bufs>(
    diff: &'diff TextDiff<'old, 'new, 'bufs, [u8]>,
    full_path: &Utf8Path,
    styles: &Styles,
    out: &mut dyn io::Write,
) -> io::Result<()>
where
    'diff: 'old + 'new + 'bufs,
{
    // The "a/" (/ courtesy full_path) and "b/" make it feel more like git diff.
    writeln!(
        out,
        "{}",
        format!("--- a{}", full_path).style(styles.diff_before)
    )?;
    writeln!(
        out,
        "{}",
        format!("+++ b/generated/{}", full_path.file_name().unwrap())
            .style(styles.diff_after)
    )?;

    let udiff = diff.unified_diff();
    for hunk in udiff.iter_hunks() {
        for (idx, change) in hunk.iter_changes().enumerate() {
            if idx == 0 {
                writeln!(out, "{}", hunk.header())?;
            }
            let style = match change.tag() {
                ChangeTag::Delete => styles.diff_before,
                ChangeTag::Insert => styles.diff_after,
                ChangeTag::Equal => Style::new(),
            };

            write!(out, "{}", change.tag().style(style))?;
            write!(out, "{}", change.value().to_string_lossy().style(style))?;
            if !diff.newline_terminated() {
                writeln!(out)?;
            }
            if diff.newline_terminated() && change.missing_newline() {
                writeln!(
                    out,
                    "{}",
                    MissingNewlineHint(hunk.missing_newline_hint())
                )?;
            }
        }
    }

    Ok(())
}

pub(crate) fn display_api_spec(spec: &ApiSpec, styles: &Styles) -> String {
    format!(
        "{} ({} v{})",
        spec.filename.style(styles.filename),
        spec.title,
        spec.version,
    )
}

pub(crate) fn display_api_spec_file(
    spec: &ApiSpec,
    spec_file: ApiSpecFile<'_>,
    styles: &Styles,
) -> String {
    match spec_file {
        ApiSpecFile::Openapi => {
            format!("OpenAPI document {}", spec.filename.style(styles.filename))
        }
        ApiSpecFile::Extra(path) => {
            format!("Extra file {}", path.style(styles.filename))
        }
    }
}

pub(crate) fn display_summary(
    summary: &DocumentSummary,
    styles: &Styles,
) -> String {
    let mut summary_str = format!(
        "{} {}",
        summary.path_count.style(styles.bold),
        plural::paths(summary.path_count),
    );

    if let Some(schema_count) = summary.schema_count {
        summary_str.push_str(&format!(
            ", {} {}",
            schema_count.style(styles.bold),
            plural::schemas(schema_count),
        ));
    } else {
        summary_str.push_str(&format!(
            ", {} for schemas",
            "data missing".style(styles.failure)
        ));
    }

    summary_str
}

pub(crate) fn display_error(
    error: &anyhow::Error,
    failure_style: Style,
) -> impl fmt::Display + '_ {
    struct DisplayError<'a> {
        error: &'a anyhow::Error,
        failure_style: Style,
    }

    impl fmt::Display for DisplayError<'_> {
        fn fmt(&self, mut f: &mut fmt::Formatter<'_>) -> fmt::Result {
            writeln!(f, "{}", self.error.style(self.failure_style))?;

            let mut source = self.error.source();
            while let Some(curr) = source {
                write!(f, "-> ")?;
                writeln!(
                    IndentWriter::new_skip_initial("   ", &mut f),
                    "{}",
                    curr.style(self.failure_style),
                )?;
                source = curr.source();
            }

            Ok(())
        }
    }

    DisplayError { error, failure_style }
}

struct MissingNewlineHint(bool);

impl fmt::Display for MissingNewlineHint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 {
            write!(f, "\n\\ No newline at end of file")?;
        }
        Ok(())
    }
}

/// Output headers.
pub(crate) mod headers {
    // Same width as Cargo's output.
    pub(crate) const HEADER_WIDTH: usize = 12;

    pub(crate) static SEPARATOR: &str = "-------";

    pub(crate) static CHECKING: &str = "Checking";
    pub(crate) static GENERATING: &str = "Generating";

    pub(crate) static FRESH: &str = "Fresh";

    // Stale encompasses:
    // - Stale: the file on disk is different from what we generated.
    // - Missing: the file on disk does not exist.
    pub(crate) static STALE: &str = "Stale";
    pub(crate) static NEW: &str = "-> New";
    pub(crate) static MODIFIED: &str = "-> Modified";

    pub(crate) static UPDATED: &str = "Updated";
    pub(crate) static UNCHANGED: &str = "Unchanged";

    pub(crate) static SUCCESS: &str = "Success";
    pub(crate) static FAILURE: &str = "Failure";

    fn count_section_width(count_width: usize) -> usize {
        // Status strings are of the form:
        //
        //    Generated [ 1/12] api.json: 1 path, 1 schema
        //             ^^^^^^^^^
        //
        // So the width of the count section is:
        // (count_width * 2) for current and total counts
        // + 3 for '[/]'
        // + 2 for spaces on either side.
        count_width * 2 + 3 + 2
    }

    pub(crate) fn count_section_indent(count_width: usize) -> String {
        " ".repeat(count_section_width(count_width))
    }

    pub(crate) fn continued_indent(count_width: usize) -> String {
        // HEADER_WIDTH for the status string + count_section_width
        " ".repeat(HEADER_WIDTH + count_section_width(count_width))
    }
}

pub(crate) mod plural {
    pub(crate) fn files(count: usize) -> &'static str {
        if count == 1 {
            "file"
        } else {
            "files"
        }
    }

    pub(crate) fn documents(count: usize) -> &'static str {
        if count == 1 {
            "document"
        } else {
            "documents"
        }
    }

    pub(crate) fn paths(count: usize) -> &'static str {
        if count == 1 {
            "path"
        } else {
            "paths"
        }
    }

    pub(crate) fn schemas(count: usize) -> &'static str {
        if count == 1 {
            "schema"
        } else {
            "schemas"
        }
    }
}
