// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Developer tool for easily running bits of Omicron

use clap::Parser;
use omicron_common::cmd::{fatal, CmdError};
use omicron_dev::OmicronDevApp;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let app = OmicronDevApp::parse();
    let result = app.exec().await;
    if let Err(error) = result {
        fatal(CmdError::Failure(error));
    }
    Ok(())
}
