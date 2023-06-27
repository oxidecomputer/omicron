use byteorder::{LittleEndian, ReadBytesExt};
use camino::Utf8PathBuf;
use std::ffi::OsString;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::os::unix::ffi::OsStringExt;
use std::process::Command;

#[derive(thiserror::Error, Debug)]
pub enum DumpHdrError {
    #[error("I/O error while attempting to open raw disk: {0}")]
    OpenRaw(std::io::Error),

    #[error("I/O error while seeking to dumphdr offset: {0}")]
    Seek(std::io::Error),

    #[error("I/O error while reading magic bytes: {0}")]
    ReadMagic(std::io::Error),

    #[error("I/O error while reading version bytes: {0}")]
    ReadVersion(std::io::Error),

    #[error("I/O error while reading flag bytes: {0}")]
    ReadFlags(std::io::Error),

    #[error("Invalid magic number {0} (expected 0xdefec8ed)")]
    InvalidMagic(u32),

    #[error("Invalid dumphdr version {0} (expected 10)")]
    InvalidVersion(u32),
}

pub fn dump_flag_is_valid(
    dump_slice: &Utf8PathBuf,
) -> Result<bool, DumpHdrError> {
    // values from /usr/src/uts/common/sys/dumphdr.h:
    const DUMP_OFFSET: u64 = 65536; // pad at start/end of dev

    const DUMP_MAGIC: u32 = 0xdefec8ed; // weird hex but ok
    const DUMP_VERSION: u32 = 10; // version of this dumphdr

    const DF_VALID: u32 = 0x00000001; // Dump is valid (savecore clears)

    let mut f = File::open(dump_slice).map_err(DumpHdrError::OpenRaw)?;
    f.seek(SeekFrom::Start(DUMP_OFFSET)).map_err(DumpHdrError::Seek)?;

    // read the first few fields of dumphdr.
    // typedef struct dumphdr {
    //     uint32_t dump_magic;
    //     uint32_t dump_version;
    //     uint32_t dump_flags;
    //     /* [...] */
    // }

    let magic =
        f.read_u32::<LittleEndian>().map_err(DumpHdrError::ReadMagic)?;
    if magic != DUMP_MAGIC {
        return Err(DumpHdrError::InvalidMagic(magic));
    }

    let version =
        f.read_u32::<LittleEndian>().map_err(DumpHdrError::ReadVersion)?;
    if version != DUMP_VERSION {
        return Err(DumpHdrError::InvalidVersion(version));
    }

    let flags =
        f.read_u32::<LittleEndian>().map_err(DumpHdrError::ReadFlags)?;
    Ok((flags & DF_VALID) != 0)
}

const DUMPADM: &str = "/usr/sbin/dumpadm";
const SAVECORE: &str = "/usr/bin/savecore";

#[derive(thiserror::Error, Debug)]
pub enum DumpAdmError {
    #[error("Error obtaining or modifying dump configuration. dump_slice: {dump_slice}, savecore_dir: {savecore_dir:?}")]
    Execution { dump_slice: Utf8PathBuf, savecore_dir: Option<Utf8PathBuf> },

    #[error("Invalid invocation of dumpadm: {0:?} {1:?}")]
    InvalidCommand(Vec<String>, std::ffi::OsString),

    #[error("dumpadm process was terminated by a signal.")]
    TerminatedBySignal,

    #[error("dumpadm invocation exited with unexpected return code {0}")]
    UnexpectedExitCode(i32),

    #[error(
        "Failed to create placeholder savecore directory at /tmp/crash: {0}"
    )]
    Mkdir(std::io::Error),

    #[error("savecore failed: {0:?}")]
    SavecoreFailure(std::ffi::OsString),

    #[error("Failed to execute dumpadm process: {0}")]
    ExecDumpadm(std::io::Error),

    #[error("Failed to execute savecore process: {0}")]
    ExecSavecore(std::io::Error),
}

pub fn dumpadm(
    dump_slice: &Utf8PathBuf,
    savecore_dir: Option<&Utf8PathBuf>,
) -> Result<Option<OsString>, DumpAdmError> {
    let mut cmd = Command::new(DUMPADM);
    cmd.env_clear();

    // Include memory from the current process if there is one for the panic
    // context, in addition to kernel memory:
    cmd.arg("-c").arg("curproc");

    // Use the given block device path for dump storage:
    cmd.arg("-d").arg(dump_slice);

    // Compress crash dumps:
    cmd.arg("-z").arg("on");

    if let Some(savecore_dir) = savecore_dir {
        // Run savecore(8) to place the existing contents of dump_slice (if
        // any) into savecore_dir, and clear the presence flag.
        cmd.arg("-s").arg(savecore_dir);
    } else {
        // Do not run savecore(8) automatically...
        cmd.arg("-n");

        // ...but do create and use a tmpfs path (rather than the default
        // location under /var/crash, which is in the ramdisk pool), because
        // dumpadm refuses to do what we ask otherwise.
        let tmp_crash = "/tmp/crash";
        std::fs::create_dir_all(tmp_crash).map_err(DumpAdmError::Mkdir)?;

        cmd.arg("-s").arg(tmp_crash);
    }

    let out = cmd.output().map_err(DumpAdmError::ExecDumpadm)?;

    match out.status.code() {
        Some(0) => {
            if savecore_dir.is_some() {
                if let Ok(true) = dump_flag_is_valid(dump_slice) {
                    return savecore();
                }
            }
            Ok(None)
        }
        Some(1) => Err(DumpAdmError::Execution {
            dump_slice: dump_slice.clone(),
            savecore_dir: savecore_dir.cloned(),
        }),
        Some(2) => {
            // unwrap: every arg we've provided in this function is UTF-8
            let mut args =
                vec![cmd.get_program().to_str().unwrap().to_string()];
            cmd.get_args()
                .for_each(|arg| args.push(arg.to_str().unwrap().to_string()));
            let stderr = OsString::from_vec(out.stderr);
            Err(DumpAdmError::InvalidCommand(args, stderr))
        }
        Some(n) => Err(DumpAdmError::UnexpectedExitCode(n)),
        None => Err(DumpAdmError::TerminatedBySignal),
    }
}

// invokes savecore(8) according to the system-wide config set by dumpadm.
// savecore(8) creates a file in the savecore directory called `vmdump.<n>`,
// where `<n>` is the number in the neighboring plaintext file called `bounds`.
// if savecore(8) successfully copies the data from the dump slice to the
// vmdump file, it clears the "valid" flag in the dump slice's header and
// increments the number in `bounds` by 1.
// in the event that savecore(8) terminates before it finishes copying the
// dump, the incomplete dump will remain in the target directory, but the next
// invocation will overwrite it.
fn savecore() -> Result<Option<OsString>, DumpAdmError> {
    let mut cmd = Command::new(SAVECORE);
    cmd.env_clear();
    cmd.arg("-v");
    let out = cmd.output().map_err(DumpAdmError::ExecSavecore)?;
    if out.status.success() {
        Ok(Some(OsString::from_vec(out.stdout)))
    } else {
        Err(DumpAdmError::SavecoreFailure(OsString::from_vec(out.stderr)))
    }
}
