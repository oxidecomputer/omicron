// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanisms to launch and control propolis VMs via falcon

use anyhow::anyhow;
use camino_tempfile::tempdir;
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

    let falcon_dir = tempdir()?;
    eprintln!("Setting falcon directory to {}", falcon_dir.path());
    d.falcon_dir = falcon_dir.path().into();

    let cargo_bay = tempdir()?;
    eprintln!("Set cargo-bay to {} on host machine", cargo_bay.path());
    let source_test_path = &args[1];
    let test_name = &args[3];
    let test_file_name = camino::Utf8Path::new(source_test_path)
        .file_name()
        .expect("Failed to get test file name");
    let test_path = cargo_bay.path().to_path_buf().join(test_file_name);

    eprintln!("Copying {source_test_path} to {test_path}");
    std::fs::copy(camino::Utf8Path::new(source_test_path), &test_path)?;

    d.mount(cargo_bay.path(), "/opt/cargo-bay", vm)?;

    eprintln!("Launching test vm {node_name}");
    d.launch().await?;
    eprintln!("Launched test vm {node_name}");

    eprintln!("Running test: {test_file_name}::{test_name}");
    let run_test = format!(
        "cd /opt/cargo-bay && chmod +x {test_file_name}; \
        res=$?; \
        if [[ $res -eq 0 ]]; then \
          ./{test_file_name} --color never --exact {test_name} --nocapture; \
          res=$?; \
        fi; \
        echo $res"
    );
    let out = d.exec(vm, &run_test).await?;

    // The last line of our output contains the exit code
    let exit_code_index = out.rfind('\n').unwrap();
    let exit_code: u8 = (&out[exit_code_index + 1..]).parse().unwrap_or(255);

    eprintln!("{}", &out[..=exit_code_index]);

    if exit_code == 0u8 {
        // We destroy here so that our tempdir doesn't get dropped first and
        // give us an error.
        let _ = d.destroy();
        Ok(())
    } else {
        // Leave the VM running
        d.persistent = true;

        // Don't remove the falcon directory
        eprintln!(
            "Test failed: VM remains running, with falcon dir: {}",
            falcon_dir.path()
        );
        std::mem::forget(falcon_dir);

        Err(anyhow!("Test failed: exit code = {exit_code}"))
    }
}
