// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::error::Error;

use clap::Parser;
use helios_protostar::HostExecutor;
use installinator::InstallinatorApp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let app = InstallinatorApp::parse();
    let log = InstallinatorApp::setup_log("/tmp/installinator.log")?;
    let executor = HostExecutor::new(log.clone()).as_executor();
    app.exec(&log, &executor).await?;
    Ok(())
}
