// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for poking at ZFS.

use crate::{PFEXEC, execute_async};
use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use camino::{Utf8Path, Utf8PathBuf};
use camino_tempfile::Utf8TempDir;
use itertools::Itertools;
use omicron_common::api::external::ByteCount;
use omicron_common::disk::CompressionAlgorithm;
use omicron_common::disk::DiskIdentity;
use omicron_common::disk::SharedDatasetConfig;
use omicron_uuid_kinds::DatasetUuid;
use std::collections::BTreeMap;
use std::fmt;

use tokio::process::Command;

// These locations in the ramdisk must only be used by the switch zone.
//
// We need the switch zone online before we can create the U.2 drives and
// encrypt the zpools during rack initialization. Without the switch zone we
// cannot get the rack initialization request from wicketd in RSS which allows
// us to  initialize the trust quorum and derive the encryption keys needed for
// the U.2 disks.
pub const ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT: &str = "/zone";
pub const ZONE_ZFS_RAMDISK_DATASET: &str = "rpool/zone";

pub const ZFS: &str = "/usr/sbin/zfs";

/// This path is intentionally on a `tmpfs` to prevent copy-on-write behavior
/// and to ensure it goes away on power off.
///
/// We want minimize the time the key files are in memory, and so we rederive
/// the keys and recreate the files on demand when creating and mounting
/// encrypted filesystems. We then zero them and unlink them.
pub const KEYPATH_ROOT: &str = "/var/run/oxide/";

/// Error returned by [`Zfs::list_datasets`].
#[derive(thiserror::Error, Debug)]
#[error("Could not list datasets within zpool {name}: {err}")]
pub struct ListDatasetsError {
    name: String,
    #[source]
    err: crate::ExecutionError,
}

#[derive(thiserror::Error, Debug)]
pub enum DestroyDatasetErrorVariant {
    #[error("Dataset not found")]
    NotFound,
    #[error(transparent)]
    Other(crate::ExecutionError),
}

/// Error returned by [`Zfs::destroy_dataset`].
#[derive(thiserror::Error, Debug)]
#[error("Could not destroy dataset {name}")]
pub struct DestroyDatasetError {
    pub name: String,
    #[source]
    pub err: DestroyDatasetErrorVariant,
}

/// When the sled agent creates a mountpoint directory for a dataset,
/// it needs that directory to be empty.
///
/// If the directory is not empty, we create a new directory in the
/// same filesystem, and move old data there.
pub const MOUNTPOINT_TRANSFER_PREFIX: &str = "old-under-mountpoint-";

// Errors related to initializing a mountpoint.
//
// Note that this is passed back as part of a larger "EnsureDatasetErrorRaw",
// which includes the path of the mountpoint -- there's no need for individual
// error variants to include the path.
#[derive(thiserror::Error, Debug)]
enum MountpointError {
    #[error("Something is already mounted on the mountpoint")]
    AlreadyMounted,

    #[error("Invalid mountpoint (cannot split parent/child paths)")]
    BadMountpoint,

    #[error("Cannot query for existence of mountpoint")]
    CheckExists(#[source] std::io::Error),

    #[error("Cannot check if mountpoint is already mounted")]
    CheckMounted(#[source] crate::ExecutionError),

    #[error("Cannot parse the parent mountpoint of the directory: {0}")]
    CheckMountedParse(String),

    #[error("Cannot 'create_dir_all' the mountpoint directory")]
    CreateMountpointDirectory(#[source] std::io::Error),

    #[error(
        "Failed to create 'transfer' directory to hold old mountpoint contents"
    )]
    CreateTransferDirectory(#[source] std::io::Error),

    #[error(
        "Mountpoint directory not empty. Is someone concurrently adding files here?"
    )]
    DirectoryNotEmpty,

    #[error("Failed to make mountpoint immutable")]
    MakeImmutable(#[source] crate::ExecutionError),

    #[error("Failed to make mountpoint mutable")]
    MakeMutable(#[source] crate::ExecutionError),

    #[error("Cannot parse immutable attribute")]
    ParseImmutable(#[source] crate::ExecutionError),

    #[error("Failed to read directory")]
    Readdir(#[source] std::io::Error),

    #[error("Failed to read directory entry")]
    ReaddirEntry(#[source] std::io::Error),

    #[error("Failed to rename entry away from mountpoint ({src} -> {dst})")]
    Rename {
        src: Utf8PathBuf,
        dst: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },
}

#[derive(thiserror::Error, Debug)]
enum EnsureDatasetErrorRaw {
    #[error("ZFS execution error: {0}")]
    Execution(#[from] crate::ExecutionError),

    #[error("Unexpected output from ZFS commands: {0}")]
    Output(String),

    #[error("Dataset does not exist")]
    DoesNotExist,

    #[error("Failed to mount filesystem")]
    MountFsFailed(#[source] crate::ExecutionError),

    #[error("Failed to mount overlay filesystem")]
    MountOverlayFsFailed(#[source] crate::ExecutionError),

    #[error("Failed to initialize mountpoint at {mountpoint}")]
    MountpointCreation {
        mountpoint: Utf8PathBuf,
        #[source]
        err: MountpointError,
    },
}

/// Error returned by [`Zfs::ensure_dataset`].
#[derive(thiserror::Error, Debug)]
#[error("Failed to ensure filesystem '{name}': {err}")]
pub struct EnsureDatasetError {
    name: String,
    #[source]
    err: EnsureDatasetErrorRaw,
}

/// Error returned by [`Zfs::set_oxide_value`]
#[derive(thiserror::Error, Debug)]
#[error("Failed to set values '{values}' on filesystem {filesystem}: {err}")]
pub struct SetValueError {
    filesystem: String,
    values: String,
    err: crate::ExecutionError,
}

#[derive(thiserror::Error, Debug)]
enum GetValueErrorRaw {
    #[error(transparent)]
    Execution(#[from] crate::ExecutionError),

    #[error("Invalid property value 'all'")]
    InvalidValueAll,

    #[error("No value found with that name")]
    MissingValue,
}

/// Error returned by [`Zfs::get_oxide_value`] or [`Zfs::get_value`].
#[derive(thiserror::Error, Debug)]
#[error("Failed to get value '{name}' from filesystem {filesystem}")]
pub struct GetValueError {
    filesystem: String,
    name: String,
    #[source]
    err: GetValueErrorRaw,
}

#[derive(Debug, thiserror::Error)]
#[error("Failed to list snapshots: {0}")]
pub struct ListSnapshotsError(#[from] crate::ExecutionError);

#[derive(Debug, thiserror::Error)]
#[error(
    "Failed to create snapshot '{snap_name}' from filesystem '{filesystem}': {err}"
)]
pub struct CreateSnapshotError {
    filesystem: String,
    snap_name: String,
    err: crate::ExecutionError,
}

#[derive(Debug, thiserror::Error)]
#[error("Failed to delete snapshot '{filesystem}@{snap_name}': {err}")]
pub struct DestroySnapshotError {
    filesystem: String,
    snap_name: String,
    err: crate::ExecutionError,
}

#[derive(thiserror::Error, Debug)]
pub enum EnsureDatasetVolumeErrorInner {
    #[error(transparent)]
    Execution(#[from] crate::ExecutionError),

    #[error(transparent)]
    GetValue(#[from] GetValueError),

    #[error("value {value_name} parse error: {value} not a number!")]
    ValueParseError { value_name: String, value: String },

    #[error("expected {value_name} to be {expected}, but saw {actual}")]
    ValueMismatch { value_name: String, expected: u64, actual: u64 },
}

/// Error returned by [`Zfs::ensure_dataset_volume`].
#[derive(thiserror::Error, Debug)]
#[error("Failed to ensure volume '{name}': {err}")]
pub struct EnsureDatasetVolumeError {
    name: String,
    #[source]
    err: EnsureDatasetVolumeErrorInner,
}

impl EnsureDatasetVolumeError {
    pub fn execution(name: String, err: crate::ExecutionError) -> Self {
        EnsureDatasetVolumeError {
            name,
            err: EnsureDatasetVolumeErrorInner::Execution(err),
        }
    }

    pub fn get_value(name: String, err: GetValueError) -> Self {
        EnsureDatasetVolumeError {
            name,
            err: EnsureDatasetVolumeErrorInner::GetValue(err),
        }
    }

    pub fn value_parse(
        name: String,
        value_name: String,
        value: String,
    ) -> Self {
        EnsureDatasetVolumeError {
            name,
            err: EnsureDatasetVolumeErrorInner::ValueParseError {
                value_name,
                value,
            },
        }
    }

    pub fn value_mismatch(
        name: String,
        value_name: String,
        expected: u64,
        actual: u64,
    ) -> Self {
        EnsureDatasetVolumeError {
            name,
            err: EnsureDatasetVolumeErrorInner::ValueMismatch {
                value_name,
                expected,
                actual,
            },
        }
    }
}

/// Wraps commands for interacting with ZFS.
pub struct Zfs {}

/// Describes a mountpoint for a ZFS filesystem.
#[derive(Debug, Clone)]
pub struct Mountpoint(pub Utf8PathBuf);

impl fmt::Display for Mountpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// This is the path for an encryption key used by ZFS
#[derive(Debug, Clone)]
pub struct Keypath(pub Utf8PathBuf);

impl fmt::Display for Keypath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Keypath {
    /// Constructs a Keypath for the specified disk within the supplied root
    /// directory.
    ///
    /// By supplying "root", tests can override the location where these paths
    /// are stored to non-global locations.
    pub fn new<P: AsRef<Utf8Path>>(id: &DiskIdentity, root: &P) -> Keypath {
        let keypath_root = Utf8PathBuf::from(KEYPATH_ROOT);
        let mut keypath = keypath_root.as_path();
        let keypath_directory = loop {
            match keypath.strip_prefix("/") {
                Ok(stripped) => keypath = stripped,
                Err(_) => break root.as_ref().join(keypath),
            }
        };
        std::fs::create_dir_all(&keypath_directory)
            .expect("Cannot ensure directory for keys");

        let filename = format!(
            "{}-{}-{}-zfs-aes-256-gcm.key",
            id.vendor, id.serial, id.model
        );
        let path: Utf8PathBuf =
            [keypath_directory.as_str(), &filename].iter().collect();
        Keypath(path)
    }
}

#[derive(Debug)]
pub struct EncryptionDetails {
    pub keypath: Keypath,
    pub epoch: u64,
}

#[derive(Debug, Default)]
pub struct SizeDetails {
    pub quota: Option<ByteCount>,
    pub reservation: Option<ByteCount>,
    pub compression: CompressionAlgorithm,
}

#[derive(Debug, Clone)]
pub struct DatasetProperties {
    /// The Uuid of the dataset
    pub id: Option<DatasetUuid>,
    /// The full name of the dataset.
    pub name: String,
    /// Identifies whether or not the dataset is mounted.
    pub mounted: bool,
    /// Remaining space in the dataset and descendants.
    pub avail: ByteCount,
    /// Space used by dataset and descendants.
    pub used: ByteCount,
    /// Maximum space usable by dataset and descendants.
    pub quota: Option<ByteCount>,
    /// Minimum space guaranteed to dataset and descendants.
    pub reservation: Option<ByteCount>,
    /// The compression algorithm used for this dataset.
    ///
    /// This probably aligns with a value from
    /// [omicron_common::disk::CompressionAlgorithm], but is left as an untyped
    /// string so that unexpected compression formats don't prevent inventory
    /// from being collected.
    pub compression: String,
}

impl DatasetProperties {
    const ZFS_GET_PROPS: &'static str =
        "oxide:uuid,name,mounted,avail,used,quota,reservation,compression";
}

impl TryFrom<&DatasetProperties> for SharedDatasetConfig {
    type Error = anyhow::Error;

    fn try_from(
        props: &DatasetProperties,
    ) -> Result<SharedDatasetConfig, Self::Error> {
        Ok(SharedDatasetConfig {
            compression: props.compression.parse()?,
            quota: props.quota,
            reservation: props.reservation,
        })
    }
}

impl DatasetProperties {
    /// Parses dataset properties, assuming that the caller is providing the
    /// output of the following command as stdout:
    ///
    /// zfs get \
    ///     [maybe depth arguments] \
    ///     -Hpo name,property,value,source $ZFS_GET_PROPS $DATASETS
    fn parse_many(
        stdout: &str,
    ) -> Result<Vec<DatasetProperties>, anyhow::Error> {
        let name_prop_val_source_list = stdout.trim().split('\n');

        let mut datasets: BTreeMap<&str, BTreeMap<&str, _>> = BTreeMap::new();
        for name_prop_val_source in name_prop_val_source_list {
            // "-H" indicates that these columns are tab-separated;
            // each column may internally have whitespace.
            let mut iter = name_prop_val_source.split('\t');

            let (name, prop, val, source) = (
                iter.next().context("Missing 'name'")?,
                iter.next().context("Missing 'property'")?,
                iter.next().context("Missing 'value'")?,
                iter.next().context("Missing 'source'")?,
            );
            if let Some(extra) = iter.next() {
                bail!("Unexpected column data: '{extra}'");
            }

            let props = datasets.entry(name).or_default();
            props.insert(prop, (val, source));
        }

        datasets
            .into_iter()
            .map(|(dataset_name, props)| {
                let id = props
                    .get("oxide:uuid")
                    .filter(|(prop, source)| {
                        // Dataset UUIDs are properties that are optionally attached to
                        // datasets. However, some datasets are nested - to avoid them
                        // from propagating, we explicitly ignore this value if it is
                        // inherited.
                        //
                        // This can be the case for the "zone" filesystem root, which
                        // can propagate this property to a child zone without it set.
                        !source.starts_with("inherited") && *prop != "-"
                    })
                    .map(|(prop, _source)| {
                        prop.parse::<DatasetUuid>()
                            .context("Failed to parse UUID")
                    })
                    .transpose()?;
                let name = dataset_name.to_string();
                // Although the illumos man pages say the only valid options for
                // "mounted" are "yes" and "no", "-" is also an observed output.
                // We interpret that value, and anything other than "yes"
                // explicitly as "not mounted".
                let mounted = props
                    .get("mounted")
                    .map(|(prop, _source)| prop.to_string())
                    .ok_or(anyhow!("Missing 'mounted'"))?
                    == "yes";
                let avail = props
                    .get("available")
                    .map(|(prop, _source)| prop)
                    .ok_or(anyhow!("Missing 'available'"))?
                    .parse::<u64>()
                    .context("Failed to parse 'available'")?
                    .try_into()?;
                let used = props
                    .get("used")
                    .map(|(prop, _source)| prop)
                    .ok_or(anyhow!("Missing 'used'"))?
                    .parse::<u64>()
                    .context("Failed to parse 'used'")?
                    .try_into()?;

                // The values of "quota" and "reservation" can be either "-" or
                // "0" when they are not actually set. To be cautious, we treat
                // both of these values as "the value has not been set
                // explicitly". As a result, setting either of these values
                // explicitly to zero is indistinguishable from setting them
                // with a value of "none".
                let quota = props
                    .get("quota")
                    .filter(|(prop, _source)| *prop != "-" && *prop != "0")
                    .map(|(prop, _source)| {
                        prop.parse::<u64>().context("Failed to parse 'quota'")
                    })
                    .transpose()?
                    .and_then(|v| ByteCount::try_from(v).ok());
                let reservation = props
                    .get("reservation")
                    .filter(|(prop, _source)| *prop != "-" && *prop != "0")
                    .map(|(prop, _source)| {
                        prop.parse::<u64>()
                            .context("Failed to parse 'reservation'")
                    })
                    .transpose()?
                    .and_then(|v| ByteCount::try_from(v).ok());
                let compression = props
                    .get("compression")
                    .map(|(prop, _source)| prop.to_string())
                    .ok_or_else(|| anyhow!("Missing 'compression'"))?;

                Ok(DatasetProperties {
                    id,
                    name,
                    mounted,
                    avail,
                    used,
                    quota,
                    reservation,
                    compression,
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

#[derive(Debug, Copy, Clone)]
pub enum PropertySource {
    Local,
    Default,
    Inherited,
    Temporary,
    None,
}

impl fmt::Display for PropertySource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ps = match self {
            PropertySource::Local => "local",
            PropertySource::Default => "default",
            PropertySource::Inherited => "inherited",
            PropertySource::Temporary => "temporary",
            PropertySource::None => "none",
        };
        write!(f, "{ps}")
    }
}

#[derive(Copy, Clone, Debug)]
pub enum WhichDatasets {
    SelfOnly,
    SelfAndChildren,
}

fn build_zfs_set_key_value_pairs(
    size_details: Option<SizeDetails>,
    dataset_id: Option<DatasetUuid>,
) -> Vec<(&'static str, String)> {
    let mut props = Vec::new();
    if let Some(SizeDetails { quota, reservation, compression }) = size_details
    {
        let quota = quota
            .map(|q| q.to_bytes().to_string())
            .unwrap_or_else(|| String::from("none"));
        props.push(("quota", quota));

        let reservation = reservation
            .map(|r| r.to_bytes().to_string())
            .unwrap_or_else(|| String::from("none"));
        props.push(("reservation", reservation));

        let compression = compression.to_string();
        props.push(("compression", compression));
    }

    if let Some(id) = dataset_id {
        props.push(("oxide:uuid", id.to_string()));
    }

    props
}

/// Describes the ZFS "canmount" options.
#[derive(Copy, Clone, Debug)]
pub enum CanMount {
    On,
    Off,
    NoAuto,
}

impl CanMount {
    fn wants_mounting(&self) -> bool {
        match self {
            CanMount::On => true,
            CanMount::Off | CanMount::NoAuto => false,
        }
    }
}

/// Arguments to [Zfs::ensure_dataset].
pub struct DatasetEnsureArgs<'a> {
    /// The full path of the ZFS dataset.
    pub name: &'a str,

    /// The expected mountpoint of this filesystem.
    ///
    /// If the filesystem already exists, and is not mounted here, an error is
    /// returned.
    ///
    /// If creating a dataset, this adds the "mountpoint=..." option.
    pub mountpoint: Mountpoint,

    /// Identifies whether or not the dataset should be mounted.
    ///
    /// If "On": The dataset is mounted (unless it is also zoned).
    /// If "Off/NoAuto": The dataset is not mounted.
    ///
    /// If creating a dataset, this adds the "canmount=..." option.
    pub can_mount: CanMount,

    /// Identifies whether or not this filesystem should be used in a zone.
    ///
    /// If creating a dataset, this add the "zoned=on" option.
    pub zoned: bool,

    /// Ensures a filesystem as an encryption root.
    ///
    /// For new filesystems, this supplies the key, and all datasets within this
    /// root are implicitly encrypted. For existing filesystems, ensures that
    /// they are mounted (and that keys are loaded), but does not verify the
    /// input details.
    pub encryption_details: Option<EncryptionDetails>,

    /// Optional properties that can be set for the dataset regarding
    /// space usage.
    ///
    /// Can be used to change settings on new or existing datasets.
    pub size_details: Option<SizeDetails>,

    /// An optional UUID of the dataset.
    ///
    /// If provided, this is set as the value "oxide:uuid" through "zfs set".
    ///
    /// Can be used to change settings on new or existing datasets.
    pub id: Option<DatasetUuid>,

    /// ZFS options passed to "zfs create" with the "-o" flag.
    ///
    /// Only used when the filesystem is being created.
    /// Each string in this optional Vec should have the format "key=value".
    pub additional_options: Option<Vec<String>>,
}

// If this dataset will have a well-defined mountpoint, we ensure an immutable directory exists
// here.
//
// This is intended to mitigate issues like:
// - https://github.com/oxidecomputer/omicron/issues/7874
// - https://github.com/oxidecomputer/omicron/issues/4203
//
// Suppose we have a dataset, named "crypt/debug", within a parent dataset named "crypt".
//
// Suppose "crypt" is mounted at "/storage/crypt", and we want to mount "crypt/debug" as
// "/storage/crypt/debug".
//
// When we create the debug dataset, we are creating a directory within the "crypt" dataset, and
// mounting our new dataset on top of it. Without modification, this presents a threat: the
// "/storage/crypt/debug" directory is a regular directory, and if the underlying dataset is
// unmounted (explicitly, or due to a reboot) data could be placed UNDERNEATH the mount point.
//
// To mitigate this issue, we create the mountpoint ahead-of-time, and set the immutable
// property on it. This prevents the mountpoint from being used as anything other than a
// mountpoint.
//
// If this function is called on a mountwhich which is already mounted, an error
// is returned.
async fn ensure_empty_immutable_mountpoint(
    mountpoint: &Utf8Path,
) -> Result<(), MountpointError> {
    if mountpoint
        .try_exists()
        .map_err(|err| MountpointError::CheckExists(err))?
    {
        // If the mountpoint exists, confirm nothing is already mounted
        // on it.
        let mut command = Command::new(ZFS);
        let cmd = command.args(&[
            "get",
            "-Hpo",
            "value",
            "mountpoint",
            mountpoint.as_str(),
        ]);
        let output = execute_async(cmd)
            .await
            .map_err(|err| MountpointError::CheckMounted(err))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let Some(dir_mountpoint) = stdout.trim().lines().next() else {
            return Err(MountpointError::CheckMountedParse(stdout.to_string()));
        };

        // If this is a viable mount directory, we'd see something like:
        //
        // Root directory:      /
        // Proposed Mountpoint: /foo
        //
        // $ zfs get -Hpo value mountpoint /foo
        // /
        //
        // This means: The mountpoint of "/foo" is the root directory, "/"
        //
        // However, if "/foo" was already used as a mountpoint, we'd see:
        //
        // $ zfs get -Hpo value mountpoint /foo
        // /foo
        //
        // This means: The mountpoint of "/foo" is "/foo" - it is already
        // mounted!
        if dir_mountpoint == mountpoint {
            return Err(MountpointError::AlreadyMounted);
        }
    } else {
        // If the mountpoint did not exist, create it
        std::fs::create_dir_all(mountpoint)
            .map_err(|err| MountpointError::CreateMountpointDirectory(err))?;
    }

    // "making a directory empty" and "making a directory immutable" cannot
    // be atomic together. Here's how we cope:
    //
    // - If necessary, we make the directory mutable, and try to clear it out.
    // - We then re-set the directory to immutable.
    let immutablity = is_directory_immutable(mountpoint).await?;
    if is_directory_empty(mountpoint)?
        && immutablity.as_immutable_as_filesystem_allows()
    {
        return Ok(());
    }

    if immutablity.can_set_immutable() {
        make_directory_mutable(mountpoint).await?;
    }
    ensure_mountpoint_empty(mountpoint)?;
    if immutablity.can_set_immutable() {
        make_directory_immutable(mountpoint).await?;
    }

    // This concurrent error case is a bit exceptional: we briefly made the
    // directory mutable, tried to empty it out, and made it immutable again.
    // However, while this was happening, someone must have added entries to the
    // directory.
    //
    // This is probably a bug on the side of "whoever is adding these files".
    if !is_directory_empty(mountpoint)? {
        return Err(MountpointError::DirectoryNotEmpty);
    }
    return Ok(());
}

fn is_directory_empty(path: &Utf8Path) -> Result<bool, MountpointError> {
    Ok(path
        .read_dir_utf8()
        .map_err(|err| MountpointError::Readdir(err))?
        .next()
        .is_none())
}

fn ensure_mountpoint_empty(path: &Utf8Path) -> Result<(), MountpointError> {
    if is_directory_empty(path)? {
        return Ok(());
    }

    let (Some(parent), Some(file)) = (path.parent(), path.file_name()) else {
        return Err(MountpointError::BadMountpoint);
    };

    // The directory is not empty. Let's make a new directory,
    // with the "old-under-mountpoint-" prefix, and move all data there.

    let prefix = format!("{MOUNTPOINT_TRANSFER_PREFIX}{file}-");
    let destination_dir = Utf8TempDir::with_prefix_in(prefix, parent)
        .map_err(|err| MountpointError::CreateTransferDirectory(err))?
        .keep();

    let entries =
        path.read_dir_utf8().map_err(|err| MountpointError::Readdir(err))?;
    for entry in entries {
        let entry = entry.map_err(|err| MountpointError::ReaddirEntry(err))?;

        // This would not work for renaming recursively, but we're only renaming
        // a single directory's worth of files.
        let src = entry.path();
        let dst = destination_dir.as_path().join(entry.file_name());

        std::fs::rename(src, &dst).map_err(|err| MountpointError::Rename {
            src: src.to_path_buf(),
            dst,
            err,
        })?;
    }

    Ok(())
}

async fn make_directory_immutable(
    path: &Utf8Path,
) -> Result<(), MountpointError> {
    let mut command = Command::new(PFEXEC);
    let cmd = command.args(&["chmod", "S+ci", path.as_str()]);
    execute_async(cmd)
        .await
        .map_err(|err| MountpointError::MakeImmutable(err))?;
    Ok(())
}

async fn make_directory_mutable(
    path: &Utf8Path,
) -> Result<(), MountpointError> {
    let mut command = Command::new(PFEXEC);
    let cmd = command.args(&["chmod", "S-ci", path.as_str()]);
    execute_async(cmd)
        .await
        .map_err(|err| MountpointError::MakeMutable(err))?;
    Ok(())
}

#[derive(Debug)]
enum Immutability {
    Yes,
    No,
    Unsupported,
}

impl Immutability {
    fn as_immutable_as_filesystem_allows(&self) -> bool {
        match self {
            Immutability::Yes => true,
            Immutability::No => false,
            Immutability::Unsupported => true,
        }
    }

    fn can_set_immutable(&self) -> bool {
        match self {
            Immutability::Yes => true,
            Immutability::No => true,
            Immutability::Unsupported => false,
        }
    }
}

async fn is_directory_immutable(
    path: &Utf8Path,
) -> Result<Immutability, MountpointError> {
    let mut command = Command::new(PFEXEC);
    let cmd = command.args(&["ls", "-d/v", path.as_str()]);
    let output = execute_async(cmd)
        .await
        .map_err(|err| MountpointError::MakeImmutable(err))?;

    // NOTE: Experimenting with "truss ls -d/v" shows that it seems to be using
    // the https://illumos.org/man/2/acl API, but we will need to likely bring
    // our own bindings here to call those APIs from Rust.
    //
    // See: https://github.com/oxidecomputer/omicron/issues/7900
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.trim().lines();
    let Some(attr_line) = lines.nth(1) else {
        return Err(MountpointError::ParseImmutable(
            crate::ExecutionError::ParseFailure(stdout.to_string()),
        ));
    };

    let attrs = attr_line
        .trim()
        .trim_start_matches("{")
        .trim_end_matches("}")
        .split(",");

    let mut result = Immutability::Unsupported;
    for attr in attrs {
        if attr == "immutable" {
            result = Immutability::Yes;
        }
        if attr == "noimmutable" {
            result = Immutability::No;
        }
    }

    return Ok(result);
}

struct DatasetMountInfo {
    exists: bool,
    mounted: bool,
}

impl DatasetMountInfo {
    fn exists(mounted: bool) -> Self {
        Self { exists: true, mounted }
    }

    fn does_not_exist() -> Self {
        Self { exists: false, mounted: false }
    }
}

impl Zfs {
    /// Lists all datasets within a pool or existing dataset.
    ///
    /// Strips the input `name` from the output dataset names.
    pub async fn list_datasets(
        name: &str,
    ) -> Result<Vec<String>, ListDatasetsError> {
        let mut command = Command::new(ZFS);
        let cmd = command.args(&["list", "-d", "1", "-rHpo", "name", name]);

        let output = execute_async(cmd)
            .await
            .map_err(|err| ListDatasetsError { name: name.to_string(), err })?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let filesystems: Vec<String> = stdout
            .trim()
            .split('\n')
            .filter(|n| *n != name)
            .map(|s| {
                String::from(s.strip_prefix(&format!("{}/", name)).unwrap())
            })
            .collect();
        Ok(filesystems)
    }

    /// Get information about datasets within a list of zpools / datasets.
    /// Returns properties for all input datasets, and optionally, for
    /// their children (depending on the value of [WhichDatasets] is provided
    /// as input).
    ///
    /// This function is similar to [Zfs::list_datasets], but provides a more
    /// substantial results about the datasets found.
    ///
    /// Sorts results and de-duplicates them by name.
    pub async fn get_dataset_properties(
        datasets: &[String],
        which: WhichDatasets,
    ) -> Result<Vec<DatasetProperties>, anyhow::Error> {
        let mut command = Command::new(ZFS);
        let cmd = command.arg("get");

        // Restrict to just filesystems and volumes to be consistent with
        // `list_datasets()`. By default, `zfs list` (used by `list_datasets()`)
        // lists only filesystems and volumes, but `zfs get` (which we're using
        // here) includes all kinds of datasets. Our parsing below expects an
        // integral value for `available`; snapshots do not have that, and we
        // don't want to trip over parsing failures for them.
        cmd.args(&["-t", "filesystem,volume"]);

        match which {
            WhichDatasets::SelfOnly => (),
            WhichDatasets::SelfAndChildren => {
                cmd.args(&["-d", "1"]);
            }
        }
        cmd.args(&["-Hpo", "name,property,value,source"]);

        // Note: this is tightly coupled with the layout of DatasetProperties
        cmd.arg(DatasetProperties::ZFS_GET_PROPS);
        cmd.args(datasets);

        // We are intentionally ignoring the output status of this command.
        //
        // If one or more dataset doesn't exist, we can still read stdout to
        // see about the ones that do exist.
        let output = cmd.output().await.map_err(|err| {
            anyhow!(
                "Failed to get dataset properties for {datasets:?}: {err:?}"
            )
        })?;
        let stdout = String::from_utf8(output.stdout)?;

        DatasetProperties::parse_many(&stdout)
    }

    /// Return the name of a dataset for a ZFS object.
    ///
    /// The object can either be a dataset name, or a path, in which case it
    /// will be resolved to the _mounted_ ZFS dataset containing that path.
    pub async fn get_dataset_name(
        object: &str,
    ) -> Result<String, ListDatasetsError> {
        let mut command = Command::new(ZFS);
        let cmd = command.args(&["get", "-Hpo", "value", "name", object]);
        execute_async(cmd)
            .await
            .map(|output| {
                String::from_utf8_lossy(&output.stdout).trim().to_string()
            })
            .map_err(|err| ListDatasetsError { name: object.to_string(), err })
    }

    /// Destroys a dataset.
    pub async fn destroy_dataset(
        name: &str,
    ) -> Result<(), DestroyDatasetError> {
        let mut command = Command::new(PFEXEC);
        let cmd = command.args(&[ZFS, "destroy", "-r", name]);
        execute_async(cmd).await.map_err(|err| {
            let variant = match err {
                crate::ExecutionError::CommandFailure(info)
                    if info.stderr.contains("does not exist") =>
                {
                    DestroyDatasetErrorVariant::NotFound
                }
                _ => DestroyDatasetErrorVariant::Other(err),
            };
            DestroyDatasetError { name: name.to_string(), err: variant }
        })?;
        Ok(())
    }

    /// Ensures that a ZFS dataset is mounted
    ///
    /// Returns an error if the dataset exists, but cannot be mounted.
    pub async fn ensure_dataset_mounted_and_exists(
        name: &str,
        mountpoint: &Mountpoint,
    ) -> Result<(), EnsureDatasetError> {
        Self::ensure_dataset_mounted_and_exists_inner(name, mountpoint)
            .await
            .map_err(|err| EnsureDatasetError { name: name.to_string(), err })?;
        Ok(())
    }

    async fn ensure_dataset_mounted_and_exists_inner(
        name: &str,
        mountpoint: &Mountpoint,
    ) -> Result<(), EnsureDatasetErrorRaw> {
        let mount_info = Self::dataset_exists(name, mountpoint).await?;
        if !mount_info.exists {
            return Err(EnsureDatasetErrorRaw::DoesNotExist);
        }

        if !mount_info.mounted {
            Self::ensure_dataset_mounted(name, mountpoint).await?;
        }
        return Ok(());
    }

    async fn ensure_dataset_mounted(
        name: &str,
        mountpoint: &Mountpoint,
    ) -> Result<(), EnsureDatasetErrorRaw> {
        ensure_empty_immutable_mountpoint(&mountpoint.0).await.map_err(
            |err| EnsureDatasetErrorRaw::MountpointCreation {
                mountpoint: mountpoint.0.to_path_buf(),
                err,
            },
        )?;
        Self::mount_dataset(name).await?;
        Ok(())
    }

    /// Creates a new ZFS dataset unless one already exists.
    ///
    /// Refer to [DatasetEnsureArgs] for details on the supplied arguments.
    pub async fn ensure_dataset(
        args: DatasetEnsureArgs<'_>,
    ) -> Result<(), EnsureDatasetError> {
        let name = args.name.to_string();
        Self::ensure_dataset_inner(args)
            .await
            .map_err(|err| EnsureDatasetError { name, err })
    }

    async fn ensure_dataset_inner(
        DatasetEnsureArgs {
            name,
            mountpoint,
            can_mount,
            zoned,
            encryption_details,
            size_details,
            id,
            additional_options,
        }: DatasetEnsureArgs<'_>,
    ) -> Result<(), EnsureDatasetErrorRaw> {
        let dataset_info = Self::dataset_exists(name, &mountpoint).await?;

        // Non-zoned datasets with an explicit mountpoint and the
        // "canmount=on" property should be mounted within the global zone.
        //
        // Zoned datasets are mounted when their zones are booted, so
        // we don't do this mountpoint manipulation for them.
        let wants_mounting =
            !zoned && !dataset_info.mounted && can_mount.wants_mounting();
        let props = build_zfs_set_key_value_pairs(size_details, id);

        if dataset_info.exists {
            // If the dataset already exists: Update properties which might
            // have changed, and ensure it has been mounted if it needs
            // to be mounted.
            Self::set_values(name, props.as_slice())
                .await
                .map_err(|err| EnsureDatasetErrorRaw::from(err.err))?;

            if wants_mounting {
                Self::ensure_dataset_mounted(name, &mountpoint).await?;
            }

            return Ok(());
        }

        // If the dataset doesn't exist, create it.

        // We'll ensure they have an empty immutable mountpoint before
        // creating the dataset itself, which will also mount it.
        if wants_mounting {
            let path = &mountpoint.0;
            ensure_empty_immutable_mountpoint(&path).await.map_err(|err| {
                EnsureDatasetErrorRaw::MountpointCreation {
                    mountpoint: path.to_path_buf(),
                    err,
                }
            })?;
        }

        let mut command = Command::new(PFEXEC);
        let cmd = command.args(&[ZFS, "create"]);
        if zoned {
            cmd.args(&["-o", "zoned=on"]);
        }
        if let Some(details) = encryption_details {
            let keyloc = format!("keylocation=file://{}", details.keypath);
            let epoch = format!("oxide:epoch={}", details.epoch);
            cmd.args(&[
                "-o",
                "encryption=aes-256-gcm",
                "-o",
                "keyformat=raw",
                "-o",
                &keyloc,
                "-o",
                &epoch,
            ]);
        }

        match can_mount {
            CanMount::On => cmd.args(&["-o", "canmount=on"]),
            CanMount::Off => cmd.args(&["-o", "canmount=off"]),
            CanMount::NoAuto => cmd.args(&["-o", "canmount=noauto"]),
        };

        if let Some(opts) = additional_options {
            for o in &opts {
                cmd.args(&["-o", &o]);
            }
        }

        cmd.args(&["-o", &format!("mountpoint={}", mountpoint), name]);

        execute_async(cmd)
            .await
            .map_err(|err| EnsureDatasetErrorRaw::from(err))?;

        // For non-root, we ensure that the currently running process has the
        // ability to act on the underlying mountpoint.
        let user = whoami::username();
        if user != "root" {
            if !zoned {
                let mut command = Command::new(PFEXEC);
                let mount = format!("{mountpoint}");
                let cmd = command.args(["chown", "-R", &user, &mount]);
                execute_async(cmd)
                    .await
                    .map_err(|err| EnsureDatasetErrorRaw::from(err))?;
            }
        }

        Self::set_values(name, props.as_slice())
            .await
            .map_err(|err| EnsureDatasetErrorRaw::from(err.err))?;

        Ok(())
    }

    // Mounts a dataset, loading keys if necessary.
    async fn mount_dataset(name: &str) -> Result<(), EnsureDatasetErrorRaw> {
        let mut command = Command::new(PFEXEC);
        let cmd = command.args(&[ZFS, "mount", "-l", name]);
        execute_async(cmd)
            .await
            .map_err(|err| EnsureDatasetErrorRaw::MountFsFailed(err))?;
        Ok(())
    }

    pub async fn mount_overlay_dataset(
        name: &str,
    ) -> Result<(), EnsureDatasetError> {
        let mut command = Command::new(PFEXEC);
        let cmd = command.args(&[ZFS, "mount", "-O", name]);
        execute_async(cmd).await.map_err(|err| EnsureDatasetError {
            name: name.to_string(),
            err: EnsureDatasetErrorRaw::MountOverlayFsFailed(err),
        })?;
        Ok(())
    }

    // Return (true, mounted) if the dataset exists, (false, false) otherwise,
    // where mounted is if the dataset is mounted.
    async fn dataset_exists(
        name: &str,
        mountpoint: &Mountpoint,
    ) -> Result<DatasetMountInfo, EnsureDatasetErrorRaw> {
        let mut command = Command::new(ZFS);
        let cmd = command.args(&[
            "list",
            "-Hpo",
            "name,type,mountpoint,mounted",
            name,
        ]);
        // If the list command returns any valid output, validate it.
        if let Ok(output) = execute_async(cmd).await {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let values: Vec<&str> = stdout.trim().split('\t').collect();
            if &values[..3] != &[name, "filesystem", &mountpoint.to_string()] {
                return Err(EnsureDatasetErrorRaw::Output(stdout.to_string()));
            }
            let mounted = values[3] == "yes";
            Ok(DatasetMountInfo::exists(mounted))
        } else {
            Ok(DatasetMountInfo::does_not_exist())
        }
    }

    /// Set the value of an Oxide-managed ZFS property.
    pub async fn set_oxide_value(
        filesystem_name: &str,
        name: &str,
        value: &str,
    ) -> Result<(), SetValueError> {
        Zfs::set_value(filesystem_name, &format!("oxide:{}", name), value).await
    }

    async fn set_value(
        filesystem_name: &str,
        name: &str,
        value: &str,
    ) -> Result<(), SetValueError> {
        Self::set_values(filesystem_name, &[(name, value)]).await
    }

    async fn set_values<K: std::fmt::Display, V: std::fmt::Display>(
        filesystem_name: &str,
        name_values: &[(K, V)],
    ) -> Result<(), SetValueError> {
        if name_values.is_empty() {
            return Ok(());
        }

        let mut command = Command::new(PFEXEC);
        let cmd = command.args(&[ZFS, "set"]);
        for (name, value) in name_values {
            cmd.arg(format!("{name}={value}"));
        }
        cmd.arg(filesystem_name);
        execute_async(cmd).await.map_err(|err| SetValueError {
            filesystem: filesystem_name.to_string(),
            values: name_values
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .join(","),
            err,
        })?;
        Ok(())
    }

    /// Get the value of an Oxide-managed ZFS property.
    pub async fn get_oxide_value(
        filesystem_name: &str,
        name: &str,
    ) -> Result<String, GetValueError> {
        let property = format!("oxide:{name}");
        let [value] = Self::get_values(
            filesystem_name,
            &[&property],
            Some(PropertySource::Local),
        )
        .await?;
        Ok(value)
    }

    /// Calls "zfs get" with a single value
    pub async fn get_value(
        filesystem_name: &str,
        name: &str,
    ) -> Result<String, GetValueError> {
        let [value] = Self::get_values(filesystem_name, &[name], None).await?;
        Ok(value)
    }

    /// List all extant snapshots.
    pub async fn list_snapshots() -> Result<Vec<Snapshot>, ListSnapshotsError> {
        let mut command = Command::new(ZFS);
        let cmd = command.args(&["list", "-H", "-o", "name", "-t", "snapshot"]);
        execute_async(cmd)
            .await
            .map(|output| {
                let stdout = String::from_utf8_lossy(&output.stdout);
                stdout
                    .trim()
                    .lines()
                    .map(|line| {
                        let (filesystem, snap_name) =
                            line.split_once('@').unwrap();
                        Snapshot {
                            filesystem: filesystem.to_string(),
                            snap_name: snap_name.to_string(),
                        }
                    })
                    .collect()
            })
            .map_err(ListSnapshotsError::from)
    }

    /// Create a snapshot of a filesystem.
    ///
    /// A list of properties, as name-value tuples, may be passed to this
    /// method, for creating properties directly on the snapshots.
    pub async fn create_snapshot<'a>(
        filesystem: &'a str,
        snap_name: &'a str,
        properties: &'a [(&'a str, &'a str)],
    ) -> Result<(), CreateSnapshotError> {
        let mut command = Command::new(PFEXEC);
        let mut cmd = command.args([ZFS, "snapshot"]);
        for (name, value) in properties.iter() {
            cmd = cmd.arg("-o").arg(&format!("{name}={value}"));
        }
        cmd.arg(&format!("{filesystem}@{snap_name}"));
        execute_async(cmd).await.map(|_| ()).map_err(|err| {
            CreateSnapshotError {
                filesystem: filesystem.to_string(),
                snap_name: snap_name.to_string(),
                err,
            }
        })
    }

    /// Destroy a named snapshot of a filesystem.
    pub async fn destroy_snapshot(
        filesystem: &str,
        snap_name: &str,
    ) -> Result<(), DestroySnapshotError> {
        let mut command = Command::new(PFEXEC);
        let path = format!("{filesystem}@{snap_name}");
        let cmd = command.args(&[ZFS, "destroy", &path]);
        execute_async(cmd).await.map(|_| ()).map_err(|err| {
            DestroySnapshotError {
                filesystem: filesystem.to_string(),
                snap_name: snap_name.to_string(),
                err,
            }
        })
    }

    /// Calls "zfs get" to acquire multiple values
    ///
    /// - `names`: The properties being acquired
    /// - `source`: The optional property source (origin of the property)
    /// Defaults to "all sources" when unspecified.
    pub async fn get_values<const N: usize>(
        filesystem_name: &str,
        names: &[&str; N],
        source: Option<PropertySource>,
    ) -> Result<[String; N], GetValueError> {
        let mut cmd = Command::new(PFEXEC);
        let all_names = names
            .into_iter()
            .map(|n| match *n {
                "all" => Err(GetValueError {
                    filesystem: filesystem_name.to_string(),
                    name: "all".to_string(),
                    err: GetValueErrorRaw::InvalidValueAll,
                }),
                n => Ok(n),
            })
            .collect::<Result<Vec<&str>, GetValueError>>()?
            .join(",");

        cmd.args(&[ZFS, "get", "-Hpo", "value"]);
        if let Some(source) = source {
            cmd.args(&["-s", &source.to_string()]);
        }
        cmd.arg(&all_names);
        cmd.arg(filesystem_name);
        let output =
            execute_async(&mut cmd).await.map_err(|err| GetValueError {
                filesystem: filesystem_name.to_string(),
                name: format!("{:?}", names),
                err: err.into(),
            })?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let values = stdout.trim();

        const EMPTY_STRING: String = String::new();
        let mut result: [String; N] = [EMPTY_STRING; N];

        for (i, value) in values.lines().enumerate() {
            let value = value.trim();
            if value == "-" {
                return Err(GetValueError {
                    filesystem: filesystem_name.to_string(),
                    name: names[i].to_string(),
                    err: GetValueErrorRaw::MissingValue,
                });
            }
            result[i] = value.to_string();
        }
        Ok(result)
    }

    pub async fn ensure_dataset_volume(
        name: String,
        size: ByteCount,
        block_size: u32,
    ) -> Result<(), EnsureDatasetVolumeError> {
        let mut command = Command::new(PFEXEC);
        let cmd = command.args(&[ZFS, "create"]);

        cmd.args(&[
            "-V",
            &size.to_bytes().to_string(),
            "-o",
            &format!("volblocksize={}", block_size),
            &name,
        ]);

        // The command to create a dataset is not idempotent and will fail with
        // "dataset already exists" if the volume is created already. Eat this
        // and return Ok instead.

        match execute_async(cmd).await {
            Ok(_) => Ok(()),

            Err(crate::ExecutionError::CommandFailure(info))
                if info.stderr.contains("dataset already exists") =>
            {
                // Validate that the total size and volblocksize are what is
                // being requested: these cannot be changed once the volume is
                // created.

                let [actual_size, actual_block_size] =
                    Self::get_values(&name, &["volsize", "volblocksize"], None)
                        .await
                        .map_err(|err| {
                            EnsureDatasetVolumeError::get_value(
                                name.clone(),
                                err,
                            )
                        })?;

                let actual_size: u64 = actual_size.parse().map_err(|_| {
                    EnsureDatasetVolumeError::value_parse(
                        name.clone(),
                        String::from("volsize"),
                        actual_size,
                    )
                })?;

                let actual_block_size: u32 =
                    actual_block_size.parse().map_err(|_| {
                        EnsureDatasetVolumeError::value_parse(
                            name.clone(),
                            String::from("volblocksize"),
                            actual_block_size,
                        )
                    })?;

                if actual_size != size.to_bytes() {
                    return Err(EnsureDatasetVolumeError::value_mismatch(
                        name.clone(),
                        String::from("volsize"),
                        size.to_bytes(),
                        actual_size,
                    ));
                }

                if actual_block_size != block_size {
                    return Err(EnsureDatasetVolumeError::value_mismatch(
                        name.clone(),
                        String::from("volblocksize"),
                        u64::from(block_size),
                        u64::from(actual_block_size),
                    ));
                }

                Ok(())
            }

            Err(err) => Err(EnsureDatasetVolumeError::execution(name, err)),
        }
    }
}

/// A read-only snapshot of a ZFS filesystem.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub filesystem: String,
    pub snap_name: String,
}

impl Snapshot {
    /// Return the full path to the snapshot directory within the filesystem.
    ///
    /// NB: Be careful when calling this method as it may return a `Utf8PathBuf`
    /// that does not actually map to a real filesystem path. On helios systems
    /// `rpool/ROOT/<BE>` for example will will return
    /// "legacy/.zfs/snapshot/<SNAP_NAME>" because the mountpoint of the dataset
    /// is a "legacy" mount. Additionally a fileystem with no mountpoint will
    /// have a zfs mountpoint property of "-".
    pub async fn full_path(&self) -> Result<Utf8PathBuf, GetValueError> {
        // TODO (omicron#8023):
        // When a mountpoint is returned as "legacy" we could go fish around in
        // "/etc/mnttab". That would probably mean making this function return a
        // result of an option.
        let mountpoint = Zfs::get_value(&self.filesystem, "mountpoint").await?;
        Ok(Utf8PathBuf::from(mountpoint)
            .join(format!(".zfs/snapshot/{}", self.snap_name)))
    }
}

impl fmt::Display for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}@{}", self.filesystem, self.snap_name)
    }
}

/// Returns all datasets managed by Omicron
pub async fn get_all_omicron_datasets_for_delete() -> anyhow::Result<Vec<String>>
{
    let mut datasets = vec![];

    // Collect all datasets within Oxide zpools.
    //
    // This includes cockroachdb, clickhouse, and crucible datasets.
    let zpools = crate::zpool::Zpool::list().await?;
    for pool in &zpools {
        let internal =
            pool.kind() == omicron_common::zpool_name::ZpoolKind::Internal;
        let pool = pool.to_string();
        for dataset in &Zfs::list_datasets(&pool).await? {
            // Avoid erasing crashdump, backing data and swap datasets on
            // internal pools. The swap device may be in use.
            if internal
                && (["crash", "backing", "swap"].contains(&dataset.as_str())
                    || dataset.starts_with("backing/"))
            {
                continue;
            }

            datasets.push(format!("{pool}/{dataset}"));
        }
    }

    // Collect all datasets for ramdisk-based Oxide zones, if any exist.
    if let Ok(ramdisk_datasets) =
        Zfs::list_datasets(&ZONE_ZFS_RAMDISK_DATASET).await
    {
        for dataset in &ramdisk_datasets {
            datasets.push(format!("{}/{dataset}", ZONE_ZFS_RAMDISK_DATASET));
        }
    };

    Ok(datasets)
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(target_os = "illumos")]
    #[tokio::test]
    async fn directory_mutability() {
        let dir = Utf8TempDir::new_in("/var/tmp").unwrap();
        let immutablity = is_directory_immutable(dir.path()).await.unwrap();
        assert!(
            matches!(immutablity, Immutability::No),
            "new directory should be mutable, is: {:?}",
            immutablity
        );

        make_directory_immutable(dir.path()).await.unwrap();
        let immutablity = is_directory_immutable(dir.path()).await.unwrap();
        assert!(
            matches!(immutablity, Immutability::Yes),
            "directory should be immutable"
        );

        make_directory_mutable(dir.path()).await.unwrap();
        let immutablity = is_directory_immutable(dir.path()).await.unwrap();
        assert!(
            matches!(immutablity, Immutability::No),
            "directory should be mutable"
        );
    }

    // This test validates that "get_values" at least parses correctly.
    //
    // To minimize test setup, we rely on a zfs dataset named "rpool" existing,
    // but do not modify it within this test.
    #[cfg(target_os = "illumos")]
    #[tokio::test]
    async fn get_values_of_rpool() {
        // If the rpool exists, it should have a name.
        let values = Zfs::get_values("rpool", &["name"; 1], None)
            .await
            .expect("Failed to query rpool type");
        assert_eq!(values[0], "rpool");

        // We don't really care if any local properties are set, we just don't
        // want this to throw an error.
        let _values =
            Zfs::get_values("rpool", &["name"; 1], Some(PropertySource::Local))
                .await
                .expect("Failed to query rpool type");

        // Also, the "all" property should not be queryable. It's normally fine
        // to pass this value, it just returns a variable number of properties,
        // which doesn't work with the current implementation's parsing.
        let err = Zfs::get_values("rpool", &["all"; 1], None)
            .await
            .expect_err("Should not be able to query for 'all' property");

        assert!(
            matches!(err.err, GetValueErrorRaw::InvalidValueAll),
            "Unexpected error: {err}"
        );
    }

    #[test]
    fn parse_dataset_props() {
        let input = "dataset_name\tavailable\t1234\t-\n\
             dataset_name\tused\t5678\t-\n\
             dataset_name\tname\tI_AM_IGNORED\t-\n\
             dataset_name\tmounted\tyes\t-\n\
             dataset_name\tcompression\toff\tinherited from parent";
        let props = DatasetProperties::parse_many(&input)
            .expect("Should have parsed data");
        assert_eq!(props.len(), 1);

        assert_eq!(props[0].id, None);
        assert_eq!(props[0].name, "dataset_name");
        assert_eq!(props[0].mounted, true);
        assert_eq!(props[0].avail.to_bytes(), 1234);
        assert_eq!(props[0].used.to_bytes(), 5678);
        assert_eq!(props[0].quota, None);
        assert_eq!(props[0].reservation, None);
        assert_eq!(props[0].compression, "off");
    }

    #[test]
    fn parse_dataset_too_many_columns() {
        let input = "dataset_name\tavailable\t1234\t-\tEXTRA\n\
             dataset_name\tused\t5678\t-\n\
             dataset_name\tname\tI_AM_IGNORED\t-\n\
             dataset_name\tmounted\tyes\t-\n\
             dataset_name\tcompression\toff\tinherited from parent";
        let err = DatasetProperties::parse_many(&input)
            .expect_err("Should have parsed data");
        assert!(
            err.to_string().contains("Unexpected column data: 'EXTRA'"),
            "{err}"
        );
    }

    #[test]
    fn parse_dataset_props_with_optionals() {
        let input = "dataset_name\toxide:uuid\td4e1e554-7b98-4413-809e-4a42561c3d0c\tlocal\n\
             dataset_name\tmounted\tyes\t-\n\
             dataset_name\tavailable\t1234\t-\n\
             dataset_name\tused\t5678\t-\n\
             dataset_name\tquota\t111\t-\n\
             dataset_name\treservation\t222\t-\n\
             dataset_name\tcompression\toff\tinherited from parent";
        let props = DatasetProperties::parse_many(&input)
            .expect("Should have parsed data");
        assert_eq!(props.len(), 1);
        assert_eq!(
            props[0].id,
            Some("d4e1e554-7b98-4413-809e-4a42561c3d0c".parse().unwrap())
        );
        assert_eq!(props[0].name, "dataset_name");
        assert_eq!(props[0].mounted, true);
        assert_eq!(props[0].avail.to_bytes(), 1234);
        assert_eq!(props[0].used.to_bytes(), 5678);
        assert_eq!(props[0].quota.map(|q| q.to_bytes()), Some(111));
        assert_eq!(props[0].reservation.map(|r| r.to_bytes()), Some(222));
        assert_eq!(props[0].compression, "off");
    }

    #[test]
    fn parse_dataset_bad_uuid() {
        let input = "dataset_name\toxide:uuid\tbad\t-\n\
             dataset_name\tavailable\t1234\t-\n\
             dataset_name\tused\t5678\t-";

        let err = DatasetProperties::parse_many(&input)
            .expect_err("Should have failed to parse");
        assert!(
            format!("{err:#}").contains("error parsing UUID (dataset)"),
            "{err}"
        );
    }

    #[test]
    fn parse_dataset_bad_avail() {
        let input = "dataset_name\tavailable\tBADAVAIL\t-\n\
             dataset_name\tmounted\t-\t-\n\
             dataset_name\tused\t5678\t-";
        let err = DatasetProperties::parse_many(&input)
            .expect_err("Should have failed to parse");
        assert!(
            format!("{err:#}").contains("invalid digit found in string"),
            "{err}"
        );
    }

    #[test]
    fn parse_dataset_bad_usage() {
        let input = "dataset_name\tavailable\t1234\t-\n\
             dataset_name\tmounted\t-\t-\n\
             dataset_name\tused\tBADUSAGE\t-";
        let err = DatasetProperties::parse_many(&input)
            .expect_err("Should have failed to parse");
        assert!(
            format!("{err:#}").contains("invalid digit found in string"),
            "{err}"
        );
    }

    #[test]
    fn parse_dataset_bad_quota() {
        let input = "dataset_name\tavailable\t1234\t-\n\
             dataset_name\tused\t5678\t-\n\
             dataset_name\tmounted\t-\t-\n\
             dataset_name\tquota\tBADQUOTA\t-";
        let err = DatasetProperties::parse_many(&input)
            .expect_err("Should have failed to parse");
        assert!(
            format!("{err:#}").contains("invalid digit found in string"),
            "{err}"
        );
    }

    #[test]
    fn parse_dataset_bad_reservation() {
        let input = "dataset_name\tavailable\t1234\t-\n\
             dataset_name\tused\t5678\t-\n\
             dataset_name\tquota\t111\t-\n\
             dataset_name\tmounted\t-\t-\n\
             dataset_name\treservation\tBADRES\t-";
        let err = DatasetProperties::parse_many(&input)
            .expect_err("Should have failed to parse");
        assert!(
            format!("{err:#}").contains("invalid digit found in string"),
            "{err}"
        );
    }

    #[test]
    fn parse_dataset_missing_fields() {
        let expect_missing = |input: &str, what: &str| {
            let err = DatasetProperties::parse_many(input)
                .expect_err("Should have failed to parse");
            let err = format!("{err:#}");
            assert!(err.contains(&format!("Missing {what}")), "{err}");
        };

        expect_missing(
            "dataset_name\tused\t5678\t-\n\
             dataset_name\tquota\t111\t-\n\
             dataset_name\treservation\t222\t-\n\
             dataset_name\tmounted\tyes\t-\n\
             dataset_name\tcompression\toff\tinherited",
            "'available'",
        );
        expect_missing(
            "dataset_name\tavailable\t1234\t-\n\
             dataset_name\tquota\t111\t-\n\
             dataset_name\treservation\t222\t-\n\
             dataset_name\tmounted\tyes\t-\n\
             dataset_name\tcompression\toff\tinherited",
            "'used'",
        );
        expect_missing(
            "dataset_name\tavailable\t1234\t-\n\
             dataset_name\tused\t5678\t-\n\
             dataset_name\tquota\t111\t-\n\
             dataset_name\tmounted\tyes\t-\n\
             dataset_name\treservation\t222\t-",
            "'compression'",
        );
        expect_missing(
            "dataset_name\tavailable\t1234\t-\n\
             dataset_name\tused\t5678\t-\n\
             dataset_name\tquota\t111\t-\n\
             dataset_name\treservation\t222\t-",
            "'mounted'",
        );
    }

    #[test]
    fn parse_dataset_uuid_ignored_if_inherited() {
        let input = "dataset_name\toxide:uuid\tb8698ede-60c2-4e16-b792-d28c165cfd12\tinherited from parent\n\
             dataset_name\tavailable\t1234\t-\n\
             dataset_name\tused\t5678\t-\n\
             dataset_name\tmounted\t-\t-\n\
             dataset_name\tcompression\toff\t-";
        let props = DatasetProperties::parse_many(&input)
            .expect("Should have parsed data");
        assert_eq!(props.len(), 1);
        assert_eq!(props[0].id, None);
    }

    #[test]
    fn parse_dataset_uuid_ignored_if_dash() {
        let input = "dataset_name\toxide:uuid\t-\t-\n\
             dataset_name\tavailable\t1234\t-\n\
             dataset_name\tused\t5678\t-\n\
             dataset_name\tmounted\t-\t-\n\
             dataset_name\tcompression\toff\t-";
        let props = DatasetProperties::parse_many(&input)
            .expect("Should have parsed data");
        assert_eq!(props.len(), 1);
        assert_eq!(props[0].id, None);
    }

    #[test]
    fn parse_quota_ignored_if_default() {
        let input = "dataset_name\tquota\t0\tdefault\n\
             dataset_name\tavailable\t1234\t-\n\
             dataset_name\tused\t5678\t-\n\
             dataset_name\tmounted\t-\t-\n\
             dataset_name\tcompression\toff\t-";
        let props = DatasetProperties::parse_many(&input)
            .expect("Should have parsed data");
        assert_eq!(props.len(), 1);
        assert_eq!(props[0].quota, None);
    }

    #[test]
    fn parse_reservation_ignored_if_default() {
        let input = "dataset_name\treservation\t0\tdefault\n\
             dataset_name\tavailable\t1234\t-\n\
             dataset_name\tused\t5678\t-\n\
             dataset_name\tmounted\t-\t-\n\
             dataset_name\tcompression\toff\t-";
        let props = DatasetProperties::parse_many(&input)
            .expect("Should have parsed data");
        assert_eq!(props.len(), 1);
        assert_eq!(props[0].reservation, None);
    }

    #[test]
    fn parse_sorts_and_dedups() {
        let input = "foo\tavailable\t111\t-\n\
             foo\tused\t111\t-\n\
             foo\tcompression\toff\t-\n\
             foo\tavailable\t111\t-\n\
             foo\tused\t111\t-\n\
             foo\tcompression\toff\t-\n\
             foo\tmounted\t-\t-\n\
             foo\tmounted\t-\t-\n\
             bar\tavailable\t222\t-\n\
             bar\tmounted\tyes\t-\n\
             bar\tused\t222\t-\n\
             bar\tcompression\toff\t-";

        let props = DatasetProperties::parse_many(&input)
            .expect("Should have parsed data");
        assert_eq!(props.len(), 2);
        assert_eq!(props[0].name, "bar");
        assert_eq!(props[0].used, 222.into());
        assert_eq!(props[0].mounted, true);
        assert_eq!(props[1].name, "foo");
        assert_eq!(props[1].used, 111.into());
        assert_eq!(props[1].mounted, false);
    }
}
