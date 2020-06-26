/*!
 * Facilities used by the command-line tools
 */

use std::env::current_exe;
use std::process::exit;

/** represents a fatal error in a command-line program */
pub enum CmdError {
    /** incorrect command-line arguments */
    Usage(String),
    /** all other errors */
    Failure(String),
}

/**
 * Exits the current process on a fatal error.
 */
pub fn fatal(cmd_error: CmdError) -> ! {
    let arg0_result = current_exe().ok();
    let arg0 = arg0_result
        .as_deref()
        .and_then(|pathbuf| pathbuf.file_name())
        .and_then(|file_name| file_name.to_str())
        .unwrap_or("command");
    let (exit_code, message) = match cmd_error {
        CmdError::Usage(m) => (2, m),
        CmdError::Failure(m) => (1, m),
    };
    eprintln!("{}: {}", arg0, message);
    exit(exit_code);
}
