// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test-migrates one or more old-format services ledger files to new-format
//! Omicron zones ledgers

use anyhow::Context;
use camino::Utf8PathBuf;
use clap::Args;
use clap::Parser;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_sled_agent::services::ZonesConfig;
use omicron_sled_agent::services_migration::AllZoneRequests;

#[tokio::main]
async fn main() {
    if let Err(message) = do_run().await {
        fatal(CmdError::Failure(format!("{:#}", message)));
    }
}

#[derive(Debug, Parser)]
#[clap(about = "Test conversion of old-format services ledgers to new-format \
    zones ledgers")]
enum Converter {
    /// checks whether one or more ledger file(s) can be converted successfully
    Check(CheckArgs),

    /// for a given ledger file, prints the converted form
    Show(ShowArgs),
}

#[derive(Debug, Args)]
struct CheckArgs {
    #[clap(action)]
    files: Vec<Utf8PathBuf>,
}

#[derive(Debug, Args)]
struct ShowArgs {
    #[clap(action)]
    file: Utf8PathBuf,
}

async fn do_run() -> Result<(), anyhow::Error> {
    let args = Converter::parse();

    let (files, do_show) = match args {
        Converter::Check(CheckArgs { files }) => (files, false),
        Converter::Show(ShowArgs { file }) => (vec![file], true),
    };

    for file_path in &files {
        let contents = tokio::fs::read_to_string(file_path)
            .await
            .with_context(|| format!("read {:?}", &file_path))?;
        let parsed: AllZoneRequests = serde_json::from_str(&contents)
            .with_context(|| format!("parse {:?}", &file_path))?;
        let converted = ZonesConfig::try_from(parsed)
            .with_context(|| format!("convert contents of {:?}", &file_path))?;
        if do_show {
            println!(
                "{:#}",
                serde_json::to_string_pretty(&converted).with_context(
                    || format!("print contents of {:?}", &file_path)
                )?
            );
        }
        eprintln!(
            "{}: processed okay (zones: {})",
            file_path,
            converted.zones.len()
        );
    }

    eprintln!("all files processed okay (files: {})", files.len());
    Ok(())
}
