// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::error::Error;

use clap::Parser;
use installinator::InstallinatorApp;

fn main() -> Result<(), Box<dyn Error>> {
    let app = InstallinatorApp::parse();
    let log = InstallinatorApp::setup_log("/tmp/installinator.log")?;
    oxide_tokio_rt::run(app.exec(&log))?;
    Ok(())
}
