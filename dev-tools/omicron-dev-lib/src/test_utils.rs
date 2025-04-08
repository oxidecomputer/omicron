// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test utilities for omicron-dev.

use std::{path::Path, time::Duration};

use omicron_test_utils::dev::process_running;

/// timeout used for various things that should be pretty quick
const TIMEOUT: Duration = Duration::from_secs(30);

/// Waits for the subprocess to exit and returns status information
///
/// This assumes the caller has arranged for the processes to terminate.  This
/// function verifies that both the omicron-dev and CockroachDB processes are
/// gone and that the temporary directory has been cleaned up.
pub fn verify_graceful_exit(
    mut subproc: subprocess::Popen,
    cmd_pid: u32,
    db_pid: u32,
    temp_dir: &Path,
) -> subprocess::ExitStatus {
    let wait_result = subproc
        .wait_timeout(TIMEOUT)
        .expect("failed to wait for process to exit")
        .unwrap_or_else(|| {
            panic!("timed out waiting {:?} for process to exit", &TIMEOUT)
        });

    assert!(!process_running(cmd_pid));
    assert!(!process_running(db_pid));
    assert_eq!(
        libc::ENOENT,
        std::fs::metadata(temp_dir)
            .expect_err("temporary directory still exists")
            .raw_os_error()
            .unwrap()
    );

    wait_result
}
