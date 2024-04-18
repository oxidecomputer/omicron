// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Result};
use clap::Parser;

#[derive(Parser)]
pub struct Args {}

pub fn run_cmd(_args: Args) -> Result<()> {
    bail!("Virtual hardware only available on illumos")
}
