// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities used by the command-line tools

use std::env::current_exe;
use std::process::exit;

/// represents a fatal error in a command-line program
#[derive(Debug)]
pub enum CmdError {
    /// incorrect command-line arguments
    Usage(String),
    /// all other errors
    Failure(anyhow::Error),
}

/// Exits the current process on a fatal error.
pub fn fatal(cmd_error: CmdError) -> ! {
    let arg0_result = current_exe().ok();
    let arg0 = arg0_result
        .as_deref()
        .and_then(|pathbuf| pathbuf.file_stem())
        .and_then(|file_name| file_name.to_str())
        .unwrap_or("command");
    let (exit_code, message) = match cmd_error {
        CmdError::Usage(m) => (2, m),
        CmdError::Failure(e) => (1, format!("{e:?}")),
    };
    eprintln!("{}: {}", arg0, message);
    exit(exit_code);
}
