// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers that extract system facilities for operating on zones, ZFS, etc. so
//! that these implementations can be swapped out in tests

use async_trait::async_trait;
use camino::Utf8PathBuf;
use illumos_utils::ExecutionError;
use illumos_utils::coreadm::{CoreAdm, CoreFileOption};
use illumos_utils::dumpadm::{DumpAdm, DumpContentType};
use illumos_utils::zone::ZONE_PREFIX;
use illumos_utils::zpool::ZpoolName;
use sled_storage::config::MountConfig;
use slog::error;
use std::ffi::OsString;
use zone::{Zone, ZoneError};

// Names of ZFS dataset properties.  These are stable and documented in zfs(1M).

pub(super) const ZFS_PROP_USED: &str = "used";
pub(super) const ZFS_PROP_AVAILABLE: &str = "available";

/// Helper for invoking coreadm(8) and dumpadm(8), abstracted out for tests
#[async_trait]
pub(super) trait CoreDumpAdmInvoker {
    fn coreadm(&self, core_dir: &Utf8PathBuf) -> Result<(), ExecutionError>;
    async fn dumpadm(
        &self,
        dump_slice: &Utf8PathBuf,
        savecore_dir: Option<&Utf8PathBuf>,
    ) -> Result<Option<OsString>, ExecutionError>;
}

/// Helper for interacting with ZFS filesystems, abstracted out for tests
pub(super) trait ZfsInvoker {
    fn zfs_get_prop(
        &self,
        mountpoint_or_name: &str,
        property: &str,
    ) -> Result<String, ZfsGetError>;

    fn zfs_get_integer(
        &self,
        mountpoint_or_name: &str,
        property: &str,
    ) -> Result<u64, ZfsGetError> {
        self.zfs_get_prop(mountpoint_or_name, property)?
            .parse()
            .map_err(Into::into)
    }

    fn below_thresh(
        &self,
        mountpoint: &Utf8PathBuf,
        percent: u64,
    ) -> Result<(bool, u64), ZfsGetError> {
        let used = self.zfs_get_integer(mountpoint.as_str(), ZFS_PROP_USED)?;
        let available =
            self.zfs_get_integer(mountpoint.as_str(), ZFS_PROP_AVAILABLE)?;
        let capacity = used + available;
        let below = (used * 100) / capacity < percent;
        Ok((below, used))
    }

    fn mountpoint(
        &self,
        mount_config: &MountConfig,
        zpool: &ZpoolName,
        mountpoint: &'static str,
    ) -> Utf8PathBuf;
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ZfsGetError {
    #[error("Error executing 'zfs get' command: {0}")]
    IoError(#[from] std::io::Error),
    #[error(
        "Output of 'zfs get' was not only not an integer string, it wasn't \
         even UTF-8: {0}"
    )]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Error parsing output of 'zfs get' command as integer: {0}")]
    Parse(#[from] std::num::ParseIntError),
}

/// Helper for listing currently-running zones on the system
#[async_trait]
pub(super) trait ZoneInvoker {
    async fn get_zones(&self) -> Result<Vec<Zone>, ZoneError>;
}

// Concrete implementations backed by the real system

pub(super) struct RealCoreDumpAdm {}
pub(super) struct RealZfs {}
pub(super) struct RealZone {}

#[async_trait]
impl CoreDumpAdmInvoker for RealCoreDumpAdm {
    fn coreadm(&self, core_dir: &Utf8PathBuf) -> Result<(), ExecutionError> {
        let mut cmd = CoreAdm::new();

        // disable per-process core patterns
        cmd.disable(CoreFileOption::Process);
        cmd.disable(CoreFileOption::ProcSetid);

        // use the global core pattern
        cmd.enable(CoreFileOption::Global);
        cmd.enable(CoreFileOption::GlobalSetid);

        // set the global pattern to place all cores into core_dir,
        // with filenames of "core.[zone-name].[exe-filename].[pid].[time]"
        cmd.global_pattern(core_dir.join("core.%z.%f.%p.%t"));

        // also collect DWARF data from the exe and its library deps
        cmd.global_contents("default+debug");

        cmd.execute()
    }

    // Invokes `dumpadm(8)` to configure the kernel to dump core into the given
    // `dump_slice` block device in the event of a panic. If a core is already
    // present in that block device, and a `savecore_dir` is provided, this
    // function also invokes `savecore(8)` to save it into that directory.
    // On success, returns Ok(Some(stdout)) if `savecore(8)` was invoked, or
    // Ok(None) if it wasn't.
    async fn dumpadm(
        &self,
        dump_slice: &Utf8PathBuf,
        savecore_dir: Option<&Utf8PathBuf>,
    ) -> Result<Option<OsString>, ExecutionError> {
        let savecore_dir_cloned = if let Some(dir) = savecore_dir.cloned() {
            dir
        } else {
            // if we don't have a savecore destination yet, still create and use
            // a tmpfs path (rather than the default location under /var/crash,
            // which is in the ramdisk pool), because dumpadm refuses to do what
            // we ask otherwise.
            let tmp_crash = "/tmp/crash";
            tokio::fs::create_dir_all(tmp_crash).await.map_err(|err| {
                ExecutionError::ExecutionStart {
                    command: format!("mkdir {tmp_crash:?}"),
                    err,
                }
            })?;
            Utf8PathBuf::from(tmp_crash)
        };

        // Use the given block device path for dump storage:
        let mut cmd = DumpAdm::new(dump_slice.to_owned(), savecore_dir_cloned);

        // Include memory from the current process if there is one for the panic
        // context, in addition to kernel memory:
        cmd.content_type(DumpContentType::CurProc);

        // Compress crash dumps:
        cmd.compress(true);

        // Do not run savecore(8) automatically on boot (irrelevant anyhow, as
        // the config file being mutated by dumpadm won't survive reboots on
        // gimlets).  The sled-agent will invoke it manually instead.
        cmd.no_boot_time_savecore();

        cmd.execute()?;

        // do we have a destination for the saved dump
        if savecore_dir.is_some() {
            // and does the dump slice have one to save off
            if let Ok(true) =
                illumos_utils::dumpadm::dump_flag_is_valid(dump_slice).await
            {
                return illumos_utils::dumpadm::SaveCore.execute();
            }
        }
        Ok(None)
    }
}

impl ZfsInvoker for RealZfs {
    fn zfs_get_prop(
        &self,
        mountpoint_or_name: &str,
        property: &str,
    ) -> Result<String, ZfsGetError> {
        let mut cmd = std::process::Command::new(illumos_utils::zfs::ZFS);
        cmd.arg("get").arg("-Hpo").arg("value");
        cmd.arg(property);
        cmd.arg(mountpoint_or_name);
        let output = cmd.output()?;
        Ok(String::from_utf8(output.stdout)?.trim().to_string())
    }

    fn mountpoint(
        &self,
        mount_config: &MountConfig,
        zpool: &ZpoolName,
        mountpoint: &'static str,
    ) -> Utf8PathBuf {
        zpool.dataset_mountpoint(&mount_config.root, mountpoint)
    }
}

#[async_trait]
impl ZoneInvoker for RealZone {
    async fn get_zones(&self) -> Result<Vec<Zone>, ZoneError> {
        Ok(zone::Adm::list()
            .await?
            .into_iter()
            .filter(|z| z.global() || z.name().starts_with(ZONE_PREFIX))
            .collect::<Vec<_>>())
    }
}
