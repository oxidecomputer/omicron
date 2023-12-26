use camino::Utf8PathBuf;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::process::Command;

#[derive(thiserror::Error, Debug)]
pub enum CoreAdmError {
    #[error("Error obtaining or modifying coreadm configuration. core_dir: {core_dir:?}")]
    Execution { core_dir: Utf8PathBuf },

    #[error("Invalid invocation of coreadm: {0:?} {1:?}")]
    InvalidCommand(Vec<String>, OsString),

    #[error("coreadm process was terminated by a signal.")]
    TerminatedBySignal,

    #[error("coreadm invocation exited with unexpected return code {0}")]
    UnexpectedExitCode(i32),

    #[error("Failed to execute dumpadm process: {0}")]
    Exec(std::io::Error),
}

const COREADM: &str = "/usr/bin/coreadm";

pub fn coreadm(core_dir: &Utf8PathBuf) -> Result<(), CoreAdmError> {
    let mut cmd = Command::new(COREADM);
    cmd.env_clear();

    // disable per-process core patterns
    cmd.arg("-d").arg("process");
    cmd.arg("-d").arg("proc-setid");

    // use the global core pattern
    cmd.arg("-e").arg("global");
    cmd.arg("-e").arg("global-setid");

    // set the global pattern to place all cores into core_dir,
    // with filenames of "core.[zone-name].[exe-filename].[pid].[time]"
    cmd.arg("-g").arg(core_dir.join("core.%z.%f.%p.%t"));

    // also collect DWARF data from the exe and its library deps
    cmd.arg("-G").arg("default+debug");

    let out = cmd.output().map_err(CoreAdmError::Exec)?;

    match out.status.code() {
        Some(0) => Ok(()),
        Some(1) => Err(CoreAdmError::Execution { core_dir: core_dir.clone() }),
        Some(2) => {
            // unwrap: every arg we've provided in this function is UTF-8
            let mut args =
                vec![cmd.get_program().to_str().unwrap().to_string()];
            cmd.get_args()
                .for_each(|arg| args.push(arg.to_str().unwrap().to_string()));
            let stderr = OsString::from_vec(out.stderr);
            Err(CoreAdmError::InvalidCommand(args, stderr))
        }
        Some(n) => Err(CoreAdmError::UnexpectedExitCode(n)),
        None => Err(CoreAdmError::TerminatedBySignal),
    }
}
