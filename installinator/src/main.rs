// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::error::Error;

use clap::Parser;
use installinator::InstallinatorApp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Work around https://github.com/oxidecomputer/omicron/issues/3507 by
    // allocating a balloon (that we don't use) to push us past the dangerous
    // address space.
    const BALLOON_SIZE: usize = 2 << 30; // 2 GiB
    let balloon = vec![1; BALLOON_SIZE];
    let app = InstallinatorApp::parse();
    let log = InstallinatorApp::setup_log("/tmp/installinator.log")?;
    app.exec(&log).await?;

    // Dumb way to ensure `balloon` isn't compiled out
    println!(
        "omicron#3507 workaround (ignore this): {}",
        std::hint::black_box(&balloon[BALLOON_SIZE - 1])
    );
    Ok(())
}
