use crate::{execute, ExecutionError};
use camino::Utf8PathBuf;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::process::Command;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

pub const DUMPADM: &str = "/usr/sbin/dumpadm";
pub const SAVECORE: &str = "/usr/bin/savecore";

// values from /usr/src/uts/common/sys/dumphdr.h:
pub const DUMP_OFFSET: u64 = 65536; // pad at start/end of dev

pub const DUMP_MAGIC: u32 = 0xdefec8ed; // weird hex but ok
pub const DUMP_VERSION: u32 = 10; // version of this dumphdr

pub const DF_VALID: u32 = 0x00000001; // Dump is valid (savecore clears)

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

/// Returns Ok(true) if the given block device contains a dump that needs to
/// be savecore'd, Ok(false) if the given block device contains a dump that's
/// already been savecore'd, Err(DumpHdrError::InvalidMagic) if there's never
/// been a core written there at all, Err(DumpHdrError::InvalidVersion) if the
/// dumphdr isn't the one we know how to handle (10), or other variants of
/// DumpHdrError if there are I/O failures while reading the block device.
pub async fn dump_flag_is_valid(
    dump_slice: &Utf8PathBuf,
) -> Result<bool, DumpHdrError> {
    let mut f = File::open(dump_slice).await.map_err(DumpHdrError::OpenRaw)?;
    f.seek(SeekFrom::Start(DUMP_OFFSET)).await.map_err(DumpHdrError::Seek)?;

    // read the first few fields of dumphdr.
    // typedef struct dumphdr {
    //     uint32_t dump_magic;
    //     uint32_t dump_version;
    //     uint32_t dump_flags;
    //     /* [...] */
    // }

    let magic = f.read_u32().await.map_err(DumpHdrError::ReadMagic)?;
    if magic != DUMP_MAGIC.to_be() {
        return Err(DumpHdrError::InvalidMagic(magic));
    }

    let version = f.read_u32().await.map_err(DumpHdrError::ReadVersion)?;
    if version != DUMP_VERSION.to_be() {
        return Err(DumpHdrError::InvalidVersion(version));
    }

    let flags = f.read_u32().await.map_err(DumpHdrError::ReadFlags)?;
    Ok((flags & DF_VALID.to_be()) != 0)
}

pub enum DumpContentType {
    Kernel,
    All,
    CurProc,
}

impl AsRef<str> for DumpContentType {
    fn as_ref(&self) -> &str {
        match self {
            DumpContentType::Kernel => "kernel",
            DumpContentType::All => "all",
            DumpContentType::CurProc => "curproc",
        }
    }
}

/// Invokes `dumpadm(8)` to configure the kernel to dump core into the given
/// `dump_slice` block device in the event of a panic.
pub struct DumpAdm {
    cmd: Command,
    content_type: Option<DumpContentType>,
    dump_slice: Utf8PathBuf,
    savecore_dir: Utf8PathBuf,
}

impl DumpAdm {
    pub fn new(dump_slice: Utf8PathBuf, savecore_dir: Utf8PathBuf) -> Self {
        let mut cmd = Command::new(DUMPADM);
        cmd.env_clear();

        Self { cmd, content_type: None, dump_slice, savecore_dir }
    }

    pub fn content_type(&mut self, ctype: DumpContentType) {
        self.content_type = Some(ctype);
    }

    pub fn compress(&mut self, on: bool) {
        let arg = if on { "on" } else { "off" };
        self.cmd.arg("-z").arg(arg);
    }

    pub fn no_boot_time_savecore(&mut self) {
        self.cmd.arg("-n");
    }

    pub fn execute(mut self) -> Result<(), ExecutionError> {
        if let Some(ctype) = self.content_type {
            self.cmd.arg("-c").arg(ctype.as_ref());
        }
        self.cmd.arg("-d").arg(self.dump_slice);
        self.cmd.arg("-s").arg(self.savecore_dir);

        execute(&mut self.cmd)?;
        Ok(())
    }
}

pub struct SaveCore;

impl SaveCore {
    /// Invokes savecore(8) according to the system-wide config set by dumpadm.
    /// savecore(8) creates a file in the savecore directory called `vmdump.<n>`,
    /// where `<n>` is the number in the neighboring plaintext file called `bounds`,
    /// or 0 if the file doesn't exist.
    /// If savecore(8) successfully copies the data from the dump slice to the
    /// vmdump file, it clears the "valid" flag in the dump slice's header and
    /// increments the number in `bounds` by 1.
    /// In the event that savecore(8) terminates before it finishes copying the
    /// dump, the incomplete dump will remain in the target directory, but the next
    /// invocation will overwrite it, because `bounds` wasn't created/incremented.
    pub fn execute(&self) -> Result<Option<OsString>, ExecutionError> {
        let mut cmd = Command::new(SAVECORE);
        cmd.env_clear();
        cmd.arg("-v");
        let out = execute(&mut cmd)?;
        if out.stdout.is_empty() || out.stdout == vec![b'\n'] {
            Ok(None)
        } else {
            Ok(Some(OsString::from_vec(out.stdout)))
        }
    }
}
