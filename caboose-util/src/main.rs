// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use anyhow::{bail, Context, Result};
use hubtools::{Caboose, RawHubrisArchive};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    match args.next().context("subcommand required")?.as_str() {
        "read-board" => {
            let caboose = read_caboose(args.next())?;
            println!("{}", std::str::from_utf8(caboose.board()?)?);
            Ok(())
        }
        "read-version" => {
            let caboose = read_caboose(args.next())?;
            println!("{}", std::str::from_utf8(caboose.version()?)?);
            Ok(())
        }
        unknown => bail!("unknown command {}", unknown),
    }
}

fn read_caboose(path: Option<String>) -> Result<Caboose> {
    let archive = RawHubrisArchive::load(
        &path.context("path to hubris archive required")?,
    )?;
    Ok(archive.read_caboose()?)
}
