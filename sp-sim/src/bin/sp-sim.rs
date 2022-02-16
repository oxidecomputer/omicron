// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use anyhow::Result;
use sp_sim::{Config, Sidecar};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config {
        ip: "127.0.0.1".parse().unwrap(),
        port: 23456,
    };

    let _sidecar = Sidecar::spawn(&config).await?;

    // gross; real use case is as a lib, where we wait for incoming requests to
    // poke at `sidecar` to change its state for tests. for now just wait to be
    // killed.
    tokio::time::sleep(Duration::MAX).await;
    Ok(())
}
