// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test-migrates one or more old-format services ledger files to new-format
//! Omicron zones ledgers

use anyhow::Context;
use camino::Utf8PathBuf;
use clap::Parser;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_sled_agent::services::ZonesConfig;
use omicron_sled_agent::services_migration::AllServiceRequests;

#[tokio::main]
async fn main() {
    if let Err(message) = do_run().await {
        fatal(CmdError::Failure(format!("{:#}", message)));
    }
}

// XXX-dap make this two subcommands:
// - `check` does what this does now
// - `show` takes one argument and pretty-prints the converted form

#[derive(Debug, Parser)]
struct Args {
    #[clap(action)]
    files: Vec<Utf8PathBuf>,
}

async fn do_run() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    for file_path in &args.files {
        let contents = tokio::fs::read_to_string(file_path)
            .await
            .with_context(|| format!("read {:?}", &file_path))?;
        let parsed: AllServiceRequests = serde_json::from_str(&contents)
            .with_context(|| format!("parse {:?}", &file_path))?;
        let converted = ZonesConfig::try_from(parsed)
            .with_context(|| format!("migrate contents of {:?}", &file_path))?;
        // XXX-dap what sanity-checks on the result?
        println!(
            "{}: converted okay (zones: {})",
            file_path,
            converted.zones.len()
        );
    }

    println!("all files converted okay (files: {})", args.files.len());
    Ok(())
}
