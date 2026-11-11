// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes a handful of configuration options that can be
//! used to tweak behavior under test.

use camino::Utf8PathBuf;

/// Options to alter the mount path of datasets.
///
/// By default, datasets within a pool are mounted under "/pool/ext/..." and
/// "/pool/int/...". For more context, see:
/// [illumos_utils::zpool::ZpoolName::dataset_mountpoint].
///
/// However, under test, it can be desirable to have a root filesystem
/// which is isolated from other tests, and which doesn't need to exist under
/// the root filesystem. [MountConfig] provides options to tweak which path is
/// used to set up and access these datasets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountConfig {
    /// The root path under which datasets are located.
    pub root: Utf8PathBuf,

    /// The path where synthetic disks are stored,
    /// if their paths are not absolute.
    pub synthetic_disk_root: Utf8PathBuf,
}

impl Default for MountConfig {
    fn default() -> Self {
        Self {
            root: Utf8PathBuf::from(
                illumos_utils::zpool::ZPOOL_MOUNTPOINT_ROOT,
            ),
            synthetic_disk_root: Utf8PathBuf::from("/var/tmp"),
        }
    }
}
