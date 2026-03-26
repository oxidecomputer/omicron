// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A prototype SQL shell directly to ClickHouse for testing the native client.

// Copyright 2024 Oxide Computer Company

use crate::native::{self, QueryResult, block::ValueArray};
use anyhow::Context as _;
use crossterm::style::Stylize;
use display_error_chain::DisplayErrorChain;
use reedline::{DefaultPrompt, DefaultPromptSegment, Reedline, Signal};
use std::net::{IpAddr, SocketAddr};
use tabled::{builder::Builder, settings::Style};

/// Run the native SQL shell.
pub async fn shell(addr: IpAddr, port: u16) -> anyhow::Result<()> {
    let addr = SocketAddr::new(addr, port);
    let mut conn = native::Connection::new(addr)
        .await
        .context("Trying to connect to ClickHouse server")?;

    println!("Connected to ClickHouse server:");
    let server_info = conn.server_info();
    println!(" Name:     {}", server_info.name);
    println!(" Host:     {}", server_info.display_name);
    println!(
        " Version:  {}.{}.{} (rev {})",
        server_info.version_major,
        server_info.version_minor,
        server_info.version_patch,
        server_info.revision,
    );
    println!();
    println!("{}", "WARNING! This is a very early prototype!".red());
    println!();
    println!(" It can be used to run basic select queries, but be");
    println!(" aware that a minimal feature-set is implemented and");
    println!(" some queries may panic the shell.");
    println!();
    println!(" Any query which modifies the database or any of its");
    println!(" tables may appear to work, but there are absolutely");
    println!(" no guarantees about their correctness or how their");
    println!(" results are handled.");
    println!();
    println!("{}", "USE WITH CAUTION".red());

    // Create the line-editor.
    let mut ed = Reedline::create();
    let prompt = DefaultPrompt::new(
        DefaultPromptSegment::Basic("clickhouse ".to_string()),
        DefaultPromptSegment::Empty,
    );
    loop {
        let sig = ed.read_line(&prompt);
        match sig {
            Ok(Signal::Success(buf)) => {
                let query = buf.as_str().trim();
                if query.is_empty() {
                    continue;
                }
                match conn.query(uuid::Uuid::new_v4(), query).await {
                    Ok(result) => print_query_result(result),
                    Err(e) => {
                        eprintln!(
                            "{}\n{}",
                            "Error!".underlined().red(),
                            DisplayErrorChain::new(&e),
                        );
                        conn = native::Connection::new(addr)
                            .await
                            .context("Trying to rebuild connection")?;
                    }
                }
            }
            Ok(Signal::CtrlD) => return Ok(()),
            Ok(Signal::CtrlC) => continue,
            err => eprintln!("err: {err:?}"),
        }
    }
}

fn print_query_result(result: QueryResult) {
    let Some(block) = result.data.as_ref() else {
        return;
    };
    if block.is_empty() {
        return;
    }
    let mut builder = Builder::with_capacity(block.n_rows(), block.n_columns());
    let mut columns: Vec<_> = block
        .columns
        .values()
        .map(|col| values_to_string(&col.values))
        .collect();
    builder.push_record(block.columns.keys());
    for _row in 0..block.n_rows() {
        let row = columns.iter_mut().map(|col_iter| col_iter.next().unwrap());
        builder.push_record(row);
    }
    println!("{}", builder.build().with(Style::psql()));
    println!();
    println!("{}: {}", "Query ID".bold(), result.id);
    println!("{}:  {:?}", "Elapsed".bold(), result.progress.query_time);
}

fn values_to_string<'a>(
    values: &'a ValueArray,
) -> Box<dyn Iterator<Item = String> + 'a> {
    match values {
        ValueArray::Bool(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::UInt8(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::UInt16(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::UInt32(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::UInt64(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::UInt128(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::Int8(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::Int16(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::Int32(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::Int64(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::Int128(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::Float32(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::Float64(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::String(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::Uuid(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::Ipv4(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::Ipv6(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::Date(vals) => {
            Box::new(vals.iter().map(ToString::to_string))
        }
        ValueArray::DateTime { values, .. } => {
            Box::new(values.iter().map(ToString::to_string))
        }
        ValueArray::DateTime64 { values, .. } => {
            Box::new(values.iter().map(ToString::to_string))
        }
        ValueArray::Nullable { is_null, values } => {
            let inner = values_to_string(values);
            let it = is_null.iter().zip(inner).map(|(is_null, value)| {
                if *is_null { String::from("NULL") } else { value.to_string() }
            });
            Box::new(it)
        }
        ValueArray::Enum8 { variants, values } => {
            Box::new(values.iter().map(|i| variants.get(i).unwrap().clone()))
        }
        ValueArray::Array { values, .. } => {
            Box::new(values.iter().map(|arr| {
                format!(
                    "[{}]",
                    values_to_string(arr).collect::<Vec<_>>().join(",")
                )
            }))
        }
    }
}
