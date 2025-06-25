// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

#[cfg(target_os = "illumos")]
mod illumos {
    pub use libfalcon::{Runner, cli::run, error::Error, unit::gb};
}

#[cfg(target_os = "illumos")]
fn main() -> Result<(), illumos::Error> {
    use illumos::*;
    oxide_tokio_rt::run(async {
        let mut d = Runner::new("launchpad_mcduck_runner");

        d.node("launchpad_mcduck_test_vm", "helios-2.0", 2, gb(2));
        run(&mut d).await?;
        Ok(())
    })
}

#[cfg(not(target_os = "illumos"))]
#[tokio::main]
async fn main() {}
