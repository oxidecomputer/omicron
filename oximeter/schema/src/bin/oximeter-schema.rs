// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

//! CLI tool to understand timeseries schema

use anyhow::Context as _;
use clap::Parser;
use clap::Subcommand;
use oximeter_schema::ir::TimeseriesDefinition;
use std::num::NonZeroU8;
use std::path::PathBuf;

#[derive(Debug, Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
    /// The path to the schema definition TOML file.
    path: PathBuf,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Print the intermediate representation parsed from the schema file
    Ir,

    /// Print the derived timeseries schema.
    Schema {
        /// Show the schema for a specified timeseries by name.
        ///
        /// If not provided, all timeseries are printed.
        #[arg(short, long)]
        timeseries: Option<String>,

        /// Show the schema for a specified version.
        ///
        /// If not provided, all versions are shown.
        #[arg(short, long)]
        version: Option<NonZeroU8>,
    },

    /// Print the Rust code that would be emitted in the macro format.
    Emit,
}

fn main() -> anyhow::Result<()> {
    let args = Args::try_parse()?;
    let contents = std::fs::read_to_string(&args.path).with_context(|| {
        format!("failed to read from {}", args.path.display())
    })?;
    match args.cmd {
        Cmd::Ir => {
            let def: TimeseriesDefinition = toml::from_str(&contents)?;
            println!("{def:#?}");
        }
        Cmd::Schema { timeseries, version } => {
            let schema = oximeter_schema::ir::load_schema(&contents)?;
            match (timeseries, version) {
                (None, None) => {
                    for each in schema.into_iter() {
                        println!("{each:#?}");
                    }
                }
                (None, Some(version)) => {
                    for each in
                        schema.into_iter().filter(|s| s.version == version)
                    {
                        println!("{each:#?}");
                    }
                }
                (Some(name), None) => {
                    for each in
                        schema.into_iter().filter(|s| s.timeseries_name == name)
                    {
                        println!("{each:#?}");
                    }
                }
                (Some(name), Some(version)) => {
                    for each in schema.into_iter().filter(|s| {
                        s.timeseries_name == name && s.version == version
                    }) {
                        println!("{each:#?}");
                    }
                }
            }
        }
        Cmd::Emit => {
            let code = oximeter_schema::codegen::use_timeseries(&contents)?;
            let formatted =
                prettyplease::unparse(&syn::parse_file(&format!("{code}"))?);
            println!("{formatted}");
        }
    }
    Ok(())
}
