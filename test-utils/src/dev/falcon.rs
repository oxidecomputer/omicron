// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanisms to launch and control propolis VMs via falcon

use libfalcon::{error::Error, unit::gb, Runner};

/// Configuration for running a test in a propolis VM via Falcon
/// TODO: Create a builder?
pub struct FalconTestConfig {
    name: &'static str,
}

// A single test launched in a Falcon VM
pub struct FalconTest {
    config: FalconTestConfig,
}

impl FalconTest {
    pub async fn run(&mut self) -> anyhow::Result<()> {
        let runner_name = format!("{}_runner", self.config.name);
        let node_name = format!("{}_test_vm", self.config.name);
        let mut d = Runner::new(&runner_name);
        let vm = d.node(&node_name, "helios-2.0", 1, gb(8));

        println!("Launching test vm {node_name}");
        d.launch().await?;
        println!("Launched test vm {node_name}");
        let out = d.exec(vm, "uname -a").await?;
        println!("Hello: {out}");

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    //    #[cfg(falcon)]
    async fn launch() {
        eprintln!("hello falcon runner");
        let config = FalconTestConfig { name: "launchpad_mcduck" };
        let mut test = FalconTest { config };
        test.run().await.unwrap();
    }
}
