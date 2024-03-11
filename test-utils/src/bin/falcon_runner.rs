// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanisms to launch and control propolis VMs via falcon

use libfalcon::{unit::gb, Runner};
use std::env;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args: Vec<_> = env::args().collect();
    eprintln!("args = {:?}", args);
    let name = "launchpad_mcduck";

    let runner_name = format!("{}_runner", name);
    let node_name = format!("{}_test_vm", name);
    let mut d = Runner::new(&runner_name);
    let vm = d.node(&node_name, "helios-2.0", 2, gb(8));

    eprintln!("Launching test vm {node_name}");
    d.launch().await?;
    eprintln!("Launched test vm {node_name}");
    let out = d.exec(vm, "uname -a").await?;
    eprintln!("Hello: {out}");
    Ok(())
}
