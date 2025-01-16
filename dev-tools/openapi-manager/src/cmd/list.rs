// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::io::Write;

use indent_write::io::IndentWriter;
use owo_colors::OwoColorize;

use super::output::display_api_spec_new;
use crate::{
    apis::ManagedApis,
    cmd::output::{display_error, OutputOpts, Styles},
};

pub(crate) fn list_impl(
    verbose: bool,
    output: &OutputOpts,
) -> anyhow::Result<()> {
    let mut styles = Styles::default();
    if output.use_color(supports_color::Stream::Stdout) {
        styles.colorize();
    }
    let mut out = std::io::stdout();

    let all_apis = ManagedApis::all()?;
    let total = all_apis.len();
    let count_width = total.to_string().len();

    if verbose {
        // A string for verbose indentation. +1 for the closing ), and +2 for
        // further indentation.
        let initial_indent = " ".repeat(count_width + 1 + 2);
        // This plus 4 more for continued indentation.
        let continued_indent = " ".repeat(count_width + 1 + 2 + 4);

        for (ix, api) in all_apis.iter_apis().enumerate() {
            let count = ix + 1;

            writeln!(
                &mut out,
                "{count:count_width$}) {}",
                api.ident().style(styles.bold),
            )?;

            writeln!(
                &mut out,
                "{initial_indent} {}: {} ({})",
                "title".style(styles.header),
                api.title(),
                if api.is_versioned() { "versioned" } else { "unversioned" },
            )?;

            write!(
                &mut out,
                "{initial_indent} {}: ",
                "description".style(styles.header)
            )?;
            writeln!(
                IndentWriter::new_skip_initial(&continued_indent, &mut out),
                "{}",
                api.description(),
            )?;

            writeln!(
                &mut out,
                "{initial_indent} {}: {}",
                "boundary".style(styles.header),
                api.boundary(),
            )?;

            writeln!(
                &mut out,
                "{initial_indent} {}:",
                "spec details".style(styles.header),
            )?;

            for v in api.iter_versions() {
                match api.generate_openapi_doc(v) {
                    Ok(openapi) => {
                        let num_paths = openapi.paths.paths.len();
                        let num_schemas = openapi.components.map_or_else(
                            || "(data missing)".to_owned(),
                            |c| c.schemas.len().to_string(),
                        );
                        writeln!(
                            &mut out,
                            "{continued_indent} {}: {} paths, {} schemas",
                            format!("v{}", v).style(styles.header),
                            num_paths.style(styles.bold),
                            num_schemas.style(styles.bold),
                        )?;
                    }
                    Err(error) => {
                        write!(
                            &mut out,
                            "{initial_indent} {}: ",
                            "error".style(styles.failure),
                        )?;
                        let display = display_error(&error, styles.failure);
                        write!(
                            IndentWriter::new_skip_initial(
                                &continued_indent,
                                std::io::stderr(),
                            ),
                            "{}",
                            display,
                        )?;
                    }
                };

                if ix + 1 < total {
                    writeln!(&mut out)?;
                }
            }
        }
    } else {
        for (ix, spec) in all_apis.iter_apis().enumerate() {
            let count = ix + 1;

            writeln!(
                &mut out,
                "{count:count_width$}) {}",
                display_api_spec_new(spec, &styles),
            )?;
        }

        writeln!(
            &mut out,
            "note: run with {} for more information",
            "-v".style(styles.bold),
        )?;
    }

    Ok(())
}
