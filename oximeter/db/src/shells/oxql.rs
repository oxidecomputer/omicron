// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! OxQL shell implementation.

// Copyright 2024 Oxide Computer

use super::{list_timeseries, prepare_columns};
use crate::{make_client, oxql::Table, Client, OxqlResult};
use clap::Args;
use crossterm::style::Stylize;
use reedline::DefaultPrompt;
use reedline::DefaultPromptSegment;
use reedline::Reedline;
use reedline::Signal;
use slog::Logger;
use std::net::IpAddr;

/// Options for the OxQL shell.
#[derive(Clone, Debug, Default, Args)]
pub struct ShellOptions {
    /// Print summaries of each SQL query run against the database.
    #[clap(long = "summaries")]
    pub print_summaries: bool,
    /// Print the total elapsed query duration.
    #[clap(long = "elapsed")]
    pub print_elapsed: bool,
}

/// Run/execute the OxQL shell.
pub async fn shell(
    address: IpAddr,
    port: u16,
    log: Logger,
    opts: ShellOptions,
) -> anyhow::Result<()> {
    // Create the client.
    let client = make_client(address, port, &log).await?;

    // A workaround to ensure the client has all available timeseries when the
    // shell starts.
    let dummy = "foo:bar".parse().unwrap();
    let _ = client.schema_for_timeseries(&dummy).await;

    // Create the line-editor.
    let mut ed = Reedline::create();
    let prompt = DefaultPrompt::new(
        DefaultPromptSegment::Basic("0x".to_string()),
        DefaultPromptSegment::Empty,
    );
    println!("Oximeter Query Language shell");
    println!();
    print_basic_commands();
    loop {
        let sig = ed.read_line(&prompt);
        match sig {
            Ok(Signal::Success(buf)) => {
                let cmd = buf.as_str().trim();
                match cmd {
                    "" => continue,
                    "\\?" | "\\h" | "help" => print_basic_commands(),
                    "\\q" | "quit" | "exit" => return Ok(()),
                    "\\l" | "\\d" => list_timeseries(&client).await?,
                    _ => {
                        if let Some(timeseries_name) = cmd.strip_prefix("\\d") {
                            if timeseries_name.is_empty() {
                                list_timeseries(&client).await?;
                            } else {
                                describe_timeseries(
                                    &client,
                                    timeseries_name
                                        .trim()
                                        .trim_end_matches(';'),
                                )
                                .await?;
                            }
                        } else if let Some(stmt) = cmd.strip_prefix("\\ql") {
                            let stmt = stmt.trim();
                            if stmt.is_empty() {
                                print_general_oxql_help();
                            } else {
                                print_oxql_operation_help(stmt);
                            }
                        } else {
                            match client
                                .oxql_query(cmd.trim().trim_end_matches(';'))
                                .await
                            {
                                Ok(result) => {
                                    print_query_summary(
                                        &result,
                                        opts.print_elapsed,
                                        opts.print_summaries,
                                    );
                                    print_tables(&result.tables);
                                }
                                Err(e) => {
                                    eprintln!("{}", "Error".underlined().red());
                                    eprintln!("{e}");
                                }
                            }
                        }
                    }
                }
            }
            Ok(Signal::CtrlD) => return Ok(()),
            Ok(Signal::CtrlC) => continue,
            err => eprintln!("err: {err:?}"),
        }
    }
}

/// Describe a single timeseries.
async fn describe_timeseries(
    client: &Client,
    timeseries: &str,
) -> anyhow::Result<()> {
    match timeseries.parse() {
        Err(_) => eprintln!(
            "Invalid timeseries name '{timeseries}, \
            use \\l to list available timeseries by name
        "
        ),
        Ok(name) => {
            if let Some(schema) = client.schema_for_timeseries(&name).await? {
                let (cols, types) = prepare_columns(&schema);
                let mut builder = tabled::builder::Builder::default();
                builder.push_record(cols); // first record is the header
                builder.push_record(types);
                println!(
                    "{}",
                    builder.build().with(tabled::settings::Style::psql())
                );
            } else {
                eprintln!("No such timeseries: {timeseries}");
            }
        }
    }
    Ok(())
}

/// Print help for a specific OxQL operation.
fn print_oxql_operation_help(op: &str) {
    match op {
        "get" => {
            const HELP: &str = r#"get <timeseries_name>");

Get instances of a timeseries by name"#;
            println!("{HELP}");
        }
        "filter" => {
            const HELP: &str = r#"filter <expr>");

Filter timeseries based on their attributes.
<expr> can be a logical combination of filtering
\"atoms\", such as `field_foo > 0`. Expressions
may use any of the usual comparison operators, and
can be nested and combined with && or ||.

Expressions must refer to the name of a field
for a timeseries at this time, and must compare
against literals. For example, `some_field > 0`
is supported, but `some_field > other_field` is not."#;
            println!("{HELP}");
        }
        "group_by" => {
            const HELP: &str = r#"group_by [<field name>, ... ]
group_by [<field name>, ... ], <reducer>

Group timeseries by the named fields, optionally
specifying a reducer to use when aggregating the
timeseries within each group. If no reducer is
specified, `mean` is used, averaging the values
within each group.

Current supported reducers:
 - mean
 - sum"#;
            println!("{HELP}");
        }
        "join" => {
            const HELP: &str = r#"join

Combine 2 or more tables by peforming a natural
inner join, matching up those with fields of the
same value. Currently, joining does not take into
account the timestamps, and does not align the outputs
directly."#;
            println!("{HELP}");
        }
        _ => eprintln!("unrecognized OxQL operation: '{op}'"),
    }
}

/// Print help for the basic OxQL commands.
fn print_basic_commands() {
    println!("Basic commands:");
    println!("  \\?, \\h, help       - Print this help");
    println!("  \\q, quit, exit, ^D - Exit the shell");
    println!("  \\l                 - List timeseries");
    println!("  \\d <timeseries>    - Describe a timeseries");
    println!("  \\ql [<operation>]  - Get OxQL help about an operation");
    println!();
    println!("Or try entering an OxQL `get` query");
}

/// Print high-level information about OxQL.
fn print_general_oxql_help() {
    const HELP: &str = r#"Oximeter Query Language

The Oximeter Query Language (OxQL) implements queries as
as sequence of operations. Each of these takes zero or more
timeseries as inputs, and produces zero or more timeseries
as outputs. Operations are chained together with the pipe
operator, "|".

All queries start with a `get` operation, which selects a
timeseries from the database, by name. For example:

`get physical_data_link:bytes_received`

The supported timeseries operations are:

- get: Select a timeseries by name
- filter: Filter timeseries by field or sample values
- group_by: Group timeseries by fields, applying a reducer.
- join: Join two or more timeseries together

Run `\ql <operation>` to get specific help about that operation.
    "#;
    println!("{HELP}");
}

fn print_query_summary(
    result: &OxqlResult,
    print_elapsed: bool,
    print_summaries: bool,
) {
    if !print_elapsed && !print_summaries {
        return;
    }
    println!("{}", "Query summary".underlined().bold());
    println!(" {}: {}", "ID".bold(), result.query_id);
    if print_elapsed {
        println!(" {}: {:?}\n", "Total duration".bold(), result.total_duration);
    }
    if print_summaries {
        println!(" {}:", "SQL queries".bold());
        for summary in result.query_summaries.iter() {
            println!("  {}: {}", "ID".bold(), summary.id);
            println!("  {}: {:?}", "Duration".bold(), summary.elapsed);
            println!("  {}: {}", "Read".bold(), summary.io_summary.read);
            println!();
        }
    }
}

fn print_tables(tables: &[Table]) {
    for table in tables.iter() {
        println!();
        println!("{}", table.name().underlined().bold());
        for timeseries in table.iter() {
            if timeseries.points.is_empty() {
                continue;
            }
            println!();
            for (name, value) in timeseries.fields.iter() {
                println!(" {}: {}", name.as_str().bold(), value);
            }
            for point in timeseries.points.iter_points() {
                println!("   {point}");
            }
        }
    }
}
