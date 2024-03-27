// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example test to run in a VM.

mod test {
    use super::*;
    use crate::dev::test_setup_log;
    use anyhow::anyhow;
    use gethostname::gethostname;
    use libfalcon::unit::gb;

    #[ignore]
    #[tokio::test]
    async fn run_launch_in_vm() {
        let args: Vec<_> = std::env::args().collect();
        let logctx = test_setup_log("launch");
        let log = logctx.log.clone();
        let runner_name = "launchpad_mcduck";
        let node_name = "launchpad_mcduck_test_vm";
        let mut test = super::FalconTest::new(runner_name, &log).unwrap();
        eprintln!("args = {:?}", args);
        // Create a falcon node (VM)
        let vm = test.runner().node(node_name, "helios-2.0", 1, gb(2));

        // Create a place(cargo-bay) to store data to be mounted into the VM
        let cargo_bay = tempdir().unwrap();
        info!(log, "Set cargo-bay to {} on host machine", cargo_bay.path());
        let test_name = "launch";
        let source_test_path = &args[0];
        let test_file_name = camino::Utf8Path::new(source_test_path)
            .file_name()
            .expect("Failed to get test file name");
        let test_path = cargo_bay.path().to_path_buf().join(test_file_name);

        // Copy the test binary into the cargo-bay
        info!(log, "Copying {source_test_path} to {test_path}");
        std::fs::copy(camino::Utf8Path::new(source_test_path), &test_path)
            .expect("failed to copy source test binary into cargo-bay");

        // Mount the cargo-bay into the VM
        test.runner()
            .mount(cargo_bay.path(), "/opt/cargo-bay", vm)
            .expect("failed to mount cargo-bay in VM");

        // Launch the VM
        info!(log, "Launching test vm {node_name}");
        test.runner().launch().await.expect("failed to launch vm");
        info!(log, "Launched test vm {node_name}");

        // Run the test

        let test_filepath_in_vm: Utf8PathBuf =
            ["opt", "cargo-bay", test_file_name].iter().collect();
        match test.run_test(vm, &test_filepath_in_vm, test_name).await {
            Ok(succ) => eprintln!("{}", succ.output()),
            Err(e) => eprintln!("{:?}", e),
        }

        logctx.cleanup_successful();
    }

    #[ignore]
    #[tokio::test]
    async fn launch() -> Result<(), anyhow::Error> {
        if gethostname() == "launchpad_mcduck_test_vm" {
            eprintln!("hello falcon runner: I am inside the VM");
            Ok(())
        } else {
            Err(anyhow!("Test failed: Not running in a VM"))
        }
    }
}

use camino::{Utf8Path, Utf8PathBuf};
use camino_tempfile::{tempdir, Utf8TempDir};
use derive_more::From;
use libfalcon::{NodeRef, Runner};
use slog::Logger;

#[derive(Debug, Clone)]
pub struct FalconTestSuccess {
    output: String,
    exit_code_index: usize,
}

impl FalconTestSuccess {
    pub fn output(&self) -> &str {
        &self.output[..=self.exit_code_index]
    }
}

#[derive(Debug, From)]
pub enum FalconTestError {
    TestCompleted { error_code: u8 },
    IncompleteOutput(String),
    Falcon(libfalcon::error::Error),
}

/// Allows tests running on a host to manage running test code inside one or
/// more falcon VMs
pub struct FalconTest {
    log: Logger,
    runner: Runner,
    falcon_dir: Utf8TempDir,
}

impl FalconTest {
    pub fn new(name: &str, log: &Logger) -> Result<FalconTest, anyhow::Error> {
        let mut runner = Runner::new(name);
        let falcon_dir = tempdir()?;
        info!(log, "Setting falcon directory to {}", falcon_dir.path());
        runner.falcon_dir = falcon_dir.path().into();
        Ok(FalconTest { log: log.clone(), runner, falcon_dir })
    }

    pub fn runner(&mut self) -> &mut Runner {
        &mut self.runner
    }

    pub async fn run_test(
        &mut self,
        vm: NodeRef,
        test_filepath_in_vm: &Utf8Path,
        test_name: &str,
    ) -> Result<FalconTestSuccess, FalconTestError> {
        let test_file_name = test_filepath_in_vm
            .file_name()
            .ok_or(format!("No filename in path: {test_filepath_in_vm}"))?;
        info!(self.log, "Running test: {test_file_name}::{test_name}");
        let run_test = format!(
            "cd /opt/cargo-bay && chmod +x {test_file_name}; \
          res=$?; \
          if [[ $res -eq 0 ]]; then \
            ./{test_file_name} --color never --exact {test_name} --nocapture; \
            res=$?; \
          fi; \
          echo $res"
        );
        let out = self.runner.exec(vm, &run_test).await?;

        // Leave the VM running in  case of failure. We reset this on success.
        self.runner.persistent = true;

        // The last line of our output contains the exit code
        let exit_code_index =
            out.rfind('\n').ok_or("No newline found in output".to_string())?;

        // Ensure there is enough data left for an exit code
        let exit_code: u8 = if exit_code_index + 1 < out.len() {
            (&out[exit_code_index + 1..])
                .parse()
                .map_err(|_| "Invalid exit code".to_string())?
        } else {
            return Err("No exit code available".to_string().into());
        };

        info!(self.log, "{}", &out[..=exit_code_index]);

        if exit_code == 0u8 {
            // Test succeeded. Don't forcibly keep VMs running.
            self.runner.persistent = false;
            Ok(FalconTestSuccess { output: out, exit_code_index })
        } else {
            // Don't remove the falcon directory
            info!(
                self.log,
                "Test failed: VM remains running, with falcon dir: {}",
                self.falcon_dir.path()
            );

            Err(FalconTestError::TestCompleted { error_code: exit_code })
        }
    }
}
