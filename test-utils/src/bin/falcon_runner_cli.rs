// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use libfalcon::{cli::run, error::Error, unit::gb, Runner};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut d = Runner::new("launchpad_mcduck_runner");

    d.node("launchpad_mcduck_test_vm", "helios-2.0", 2, gb(2));
    run(&mut d).await?;
    Ok(())
}
