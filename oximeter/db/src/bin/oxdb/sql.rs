// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! SQL shell subcommand for `oxdb`.

// Copyright 2024 Oxide Computer Company

use crate::make_client;
use clap::Args;
use dropshot::EmptyScanParams;
use dropshot::WhichPage;
use oximeter_db::sql::function_allow_list;
use oximeter_db::Client;
use oximeter_db::QueryMetadata;
use oximeter_db::QueryResult;
use oximeter_db::Table;
use reedline::DefaultPrompt;
use reedline::DefaultPromptSegment;
use reedline::Reedline;
use reedline::Signal;
use slog::Logger;
use std::net::IpAddr;

fn print_basic_commands() {
    println!("Basic commands:");
    println!("  \\?, \\h, help      - Print this help");
    println!("  \\q, quit, exit, ^D - Exit the shell");
    println!("  \\l                 - List tables");
    println!("  \\d <table>         - Describe a table");
    println!(
        "  \\f <function>      - List or describe ClickHouse SQL functions"
    );
    println!();
    println!("Or try entering a SQL `SELECT` statement");
}

async fn list_virtual_tables(client: &Client) -> anyhow::Result<()> {
    let mut page = WhichPage::First(EmptyScanParams {});
    let limit = 100.try_into().unwrap();
    loop {
        let results = client.timeseries_schema_list(&page, limit).await?;
        for schema in results.items.iter() {
            println!("{}", schema.timeseries_name);
        }
        if results.next_page.is_some() {
            if let Some(last) = results.items.last() {
                page = WhichPage::Next(last.timeseries_name.clone());
            } else {
                return Ok(());
            }
        } else {
            return Ok(());
        }
    }
}

async fn describe_virtual_table(
    client: &Client,
    table: &str,
) -> anyhow::Result<()> {
    match table.parse() {
        Err(_) => println!("Invalid timeseries name: {table}"),
        Ok(name) => {
            if let Some(schema) = client.schema_for_timeseries(&name).await? {
                let mut cols =
                    Vec::with_capacity(schema.field_schema.len() + 2);
                let mut types = cols.clone();
                for field in schema.field_schema.iter() {
                    cols.push(field.name.clone());
                    types.push(field.field_type.to_string());
                }
                cols.push("timestamp".into());
                types.push("DateTime64".into());

                if schema.datum_type.is_histogram() {
                    cols.push("start_time".into());
                    types.push("DateTime64".into());

                    cols.push("bins".into());
                    types.push(format!(
                        "Array[{}]",
                        schema
                            .datum_type
                            .to_string()
                            .strip_prefix("Histogram")
                            .unwrap()
                            .to_lowercase(),
                    ));

                    cols.push("counts".into());
                    types.push("Array[u64]".into());
                } else if schema.datum_type.is_cumulative() {
                    cols.push("start_time".into());
                    types.push("DateTime64".into());
                    cols.push("datum".into());
                    types.push(schema.datum_type.to_string());
                } else {
                    cols.push("datum".into());
                    types.push(schema.datum_type.to_string());
                }

                let mut builder = tabled::builder::Builder::default();
                builder.push_record(cols); // first record is the header
                builder.push_record(types);
                println!(
                    "{}",
                    builder.build().with(tabled::settings::Style::psql())
                );
            } else {
                println!("No such timeseries: {table}");
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Args)]
pub struct ShellOptions {
    /// Print query metadata.
    #[clap(long = "metadata")]
    print_metadata: bool,
    /// Print the original SQL query.
    #[clap(long = "original")]
    print_original_query: bool,
    /// Print the rewritten SQL query that is actually run on the DB.
    #[clap(long = "rewritten")]
    print_rewritten_query: bool,
    /// Print the transformed query, but do not run it.
    #[clap(long)]
    transform: Option<String>,
}

impl Default for ShellOptions {
    fn default() -> Self {
        Self {
            print_metadata: true,
            print_original_query: false,
            print_rewritten_query: false,
            transform: None,
        }
    }
}

fn list_supported_functions() {
    println!("Subset of ClickHouse SQL functions currently supported");
    println!(
        "See https://clickhouse.com/docs/en/sql-reference/functions for more"
    );
    println!();
    for func in function_allow_list().iter() {
        println!(" {func}");
    }
}

fn show_supported_function(name: &str) {
    if let Some(func) = function_allow_list().iter().find(|f| f.name == name) {
        println!("{}", func.name);
        println!("  {}", func.usage);
        println!("  {}", func.description);
    } else {
        println!("No supported function '{name}'");
    }
}

fn print_sql_query(query: &str) {
    println!(
        "{}",
        sqlformat::format(
            &query,
            &sqlformat::QueryParams::None,
            sqlformat::FormatOptions { uppercase: true, ..Default::default() }
        )
    );
    println!();
}

fn print_query_metadata(table: &Table, metadata: &QueryMetadata) {
    println!("Metadata");
    println!(" Query ID:    {}", metadata.id);
    println!(" Result rows: {}", table.rows.len());
    println!(" Time:        {:?}", metadata.elapsed);
    println!(" Read:        {}\n", metadata.summary.read);
}

pub async fn sql_shell(
    address: IpAddr,
    port: u16,
    log: Logger,
    opts: ShellOptions,
) -> anyhow::Result<()> {
    let client = make_client(address, port, &log).await?;

    // A workaround to ensure the client has all available timeseries when the
    // shell starts.
    let dummy = "foo:bar".parse().unwrap();
    let _ = client.schema_for_timeseries(&dummy).await;

    // Possibly just transform the query, but do not execute it.
    if let Some(query) = &opts.transform {
        let transformed = client.transform_query(query).await?;
        println!(
            "{}",
            sqlformat::format(
                &transformed,
                &sqlformat::QueryParams::None,
                sqlformat::FormatOptions {
                    uppercase: true,
                    ..Default::default()
                }
            )
        );
        return Ok(());
    }

    let mut ed = Reedline::create();
    let prompt = DefaultPrompt::new(
        DefaultPromptSegment::Basic("0x".to_string()),
        DefaultPromptSegment::Empty,
    );
    println!("Oximeter SQL shell");
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
                    "\\l" | "\\d" => list_virtual_tables(&client).await?,
                    _ => {
                        if let Some(table_name) = cmd.strip_prefix("\\d") {
                            if table_name.is_empty() {
                                list_virtual_tables(&client).await?;
                            } else {
                                describe_virtual_table(
                                    &client,
                                    table_name.trim().trim_end_matches(';'),
                                )
                                .await?;
                            }
                        } else if let Some(func_name) = cmd.strip_prefix("\\f")
                        {
                            if func_name.is_empty() {
                                list_supported_functions();
                            } else {
                                show_supported_function(
                                    func_name.trim().trim_end_matches(';'),
                                );
                            }
                        } else {
                            match client.query(&buf).await {
                                Err(e) => println!("Query failed: {e:#?}"),
                                Ok(QueryResult {
                                    original_query,
                                    rewritten_query,
                                    metadata,
                                    table,
                                }) => {
                                    println!();
                                    let mut builder =
                                        tabled::builder::Builder::default();
                                    builder.push_record(&table.column_names); // first record is the header
                                    for row in table.rows.iter() {
                                        builder.push_record(
                                            row.iter().map(ToString::to_string),
                                        );
                                    }
                                    if opts.print_original_query {
                                        print_sql_query(&original_query);
                                    }
                                    if opts.print_rewritten_query {
                                        print_sql_query(&rewritten_query);
                                    }
                                    println!(
                                        "{}\n",
                                        builder.build().with(
                                            tabled::settings::Style::psql()
                                        )
                                    );
                                    if opts.print_metadata {
                                        print_query_metadata(&table, &metadata);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(Signal::CtrlD) => return Ok(()),
            Ok(Signal::CtrlC) => continue,
            err => println!("err: {err:?}"),
        }
    }
}
