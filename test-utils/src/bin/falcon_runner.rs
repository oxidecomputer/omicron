// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanisms to launch and control propolis VMs via falcon

#[cfg(target_os = "illumos")]
mod illumos {
    pub use anyhow::anyhow;
    pub use camino_tempfile::tempdir;
    pub use libfalcon::{Runner, unit::gb};
    pub use omicron_test_utils::dev::test_setup_log;
    pub use slog::info;
    pub use std::env;
}

#[cfg(target_os = "illumos")]
fn main() -> Result<(), anyhow::Error> {
    use illumos::*;
    oxide_tokio_rt::run(async {
        let args: Vec<_> = env::args().collect();
        let logctx = test_setup_log("falcon_test_runner");
        let log = logctx.log;
        info!(log, "args = {:?}", args);
        let name = "launchpad_mcduck";

        let runner_name = format!("{}_runner", name);
        let node_name = format!("{}_test_vm", name);
        let mut d = Runner::new(&runner_name);
        let vm = d.node(&node_name, "helios-2.0", 2, gb(8));

        let falcon_dir = tempdir()?;
        info!(log, "Setting falcon directory to {}", falcon_dir.path());
        d.falcon_dir = falcon_dir.path().into();

        let cargo_bay = tempdir()?;
        info!(log, "Set cargo-bay to {} on host machine", cargo_bay.path());
        let source_test_path = &args[1];
        let test_name = &args[3];
        let test_file_name = camino::Utf8Path::new(source_test_path)
            .file_name()
            .expect("Failed to get test file name");
        let test_path = cargo_bay.path().to_path_buf().join(test_file_name);

        info!(log, "Copying {source_test_path} to {test_path}");
        std::fs::copy(camino::Utf8Path::new(source_test_path), &test_path)?;

        d.mount(cargo_bay.path(), "/opt/cargo-bay", vm)?;

        info!(log, "Launching test vm {node_name}");
        d.launch().await?;
        info!(log, "Launched test vm {node_name}");

        info!(log, "Running test: {test_file_name}::{test_name}");
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
        let exit_code_index =
            out.rfind('\n').expect("No newline found in output");

        // Ensure there is enough data left for an exit code
        let exit_code: u8 = if exit_code_index + 1 < out.len() {
            (&out[exit_code_index + 1..]).parse().expect("Invalid exit code")
        } else {
            panic!("No exit code available");
        };

        info!(log, "{}", &out[..=exit_code_index]);

        if exit_code == 0u8 {
            // We destroy here so that our tempdir doesn't get dropped first and
            // give us an error.
            //
            // `drop` calls `d.destroy()` and also makes sure it doesn't run twice.
            drop(d);
            Ok(())
        } else {
            // Leave the VM running
            d.persistent = true;

            // Don't remove the falcon directory
            info!(
                log,
                "Test failed: VM remains running, with falcon dir: {}",
                falcon_dir.path()
            );
            std::mem::forget(falcon_dir);

            Err(anyhow!("Test failed: exit code = {exit_code}"))
        }
    })
}

#[cfg(not(target_os = "illumos"))]
fn main() {}
