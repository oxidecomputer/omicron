// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use anyhow::{anyhow, bail, Context, Result};
use hubtools::RawHubrisArchive;
use tlvc_text::{Piece, Tag};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    match args.next().context("subcommand required")?.as_str() {
        "read-version" => {
            let archive = RawHubrisArchive::load(
                &args.next().context("path to hubris archive required")?,
            )?;
            let caboose = archive.read_caboose()?;
            let reader = tlvc::TlvcReader::begin(caboose.as_slice())
                .map_err(|e| anyhow!("tlvc error: {e:?}"))?;
            let t = tlvc_text::dump(reader);
            for piece in t {
                if let Piece::Chunk(tag, data) = piece {
                    if tag == Tag::new(*b"VERS") {
                        for datum in data {
                            if let Piece::String(version) = datum {
                                println!("{}", version);
                                return Ok(());
                            }
                        }
                    }
                }
            }
            bail!("no VERS string found");
        }
        unknown => bail!("unknown command {}", unknown),
    }
}
