// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// Dump the sled database backing a DNS server
///
/// This is a low-level debugging tool, not something we'd expect people to use
/// regularly.

use anyhow::{bail, Context, Result};
use clap::Parser;

#[derive(Debug, Parser)]
#[clap(name = "dns-db-dump", about = "Dump DNS server's sled database")]
struct Arg {
    storage_path: String,
}

fn main() -> Result<()> {
    let arg = Arg::parse();

    let db = sled::Config::default()
        .path(&arg.storage_path)
        .create_new(false)
        .open()
        .with_context(|| format!("open {:?}", &arg.storage_path))?;

    if !db.was_recovered() {
        bail!(
            "no database found at {:?} (sorry, this command likely just \
            created an empty database there)",
            &arg.storage_path
        );
    }

    for tree_name_bytes in db.tree_names() {
        let tree_name = String::from_utf8_lossy(&tree_name_bytes);
        println!("\ntree: {:?}", tree_name);
        match db.open_tree(tree_name_bytes) {
            Ok(tree) => print_tree(&tree),
            Err(error) => {
                eprintln!("error: {:#}", error);
            }
        };
    }

    Ok(())
}

fn print_tree(tree: &sled::Tree) {
    for k in tree.iter() {
        match k {
            Ok((key_bytes, value_bytes)) => {
                println!("key: {}", String::from_utf8_lossy(&key_bytes));
                println!("     {}", String::from_utf8_lossy(&value_bytes));
            }
            Err(error) => {
                eprintln!("error: {:#}", error);
            }
        }
    }
}
