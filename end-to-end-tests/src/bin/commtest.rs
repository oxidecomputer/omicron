// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use clap::{CommandFactory, FromArgMatches};
use commtest::{Cli, run_cli};
use end_to_end_tests::helpers::cli::oxide_cli_style;

fn main() -> Result<()> {
    let matches = Cli::command().styles(oxide_cli_style()).get_matches();
    let cli = Cli::from_arg_matches(&matches)?;
    oxide_tokio_rt::run(async { run_cli(&cli).await })
}
