// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Result};
use internal_dns::dropshot_server::api;
use std::fs::File;
use std::io;

fn usage(args: &[String]) -> String {
    format!("{} [output path]", args[0])
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let mut out = match args.len() {
        1 => Box::new(io::stdout()) as Box<dyn io::Write>,
        2 => Box::new(File::create(args[1].clone())?) as Box<dyn io::Write>,
        _ => bail!(usage(&args)),
    };

    let api = api();
    let openapi = api.openapi("Internal DNS", "v0.1.0");
    openapi.write(&mut out)?;
    Ok(())
}
