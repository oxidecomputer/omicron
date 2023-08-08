// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! Tools for collecting and inspecting service bundles for zones.

use crate::params::ZoneBundleCause;
use crate::params::ZoneBundleMetadata;
use camino::FromPathBufError;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use flate2::bufread::GzDecoder;
use illumos_utils::running_zone::is_oxide_smf_log_file;
use illumos_utils::running_zone::RunningZone;
use illumos_utils::zone::AdmError;
use slog::Logger;
use std::io::Cursor;
use std::path::PathBuf;
use std::time::SystemTime;
use tar::Archive;
use tar::Builder;
use tar::Header;
use uuid::Uuid;

/// Context for creating a bundle of a specified zone.
#[derive(Debug, Default)]
pub struct ZoneBundleContext {
    /// The directories into which the zone bundles are written.
    pub storage_dirs: Vec<Utf8PathBuf>,
    /// The reason or cause for creating a zone bundle.
    pub cause: ZoneBundleCause,
    /// Extra directories searched for logfiles for the name zone.
    ///
    /// Logs are periodically archived out of their original location, and onto
    /// one or more U.2 drives. This field is used to specify that archive
    /// location, so that rotated logs for the zone's services may be found.
    pub extra_log_dirs: Vec<Utf8PathBuf>,
    /// Any zone-specific commands that will be part of the zone bundle.
    ///
    /// These should be specified as a list of strings, as passed into
    /// `RunningZone::run_cmd()`.
    pub zone_specific_commands: Vec<Vec<String>>,
}

// The set of zone-wide commands, which don't require any details about the
// processes we've launched in the zone.
const ZONE_WIDE_COMMANDS: [&[&str]; 6] = [
    &["ptree"],
    &["uptime"],
    &["last"],
    &["who"],
    &["svcs", "-p"],
    &["netstat", "-an"],
];

// The name for zone bundle metadata files.
const ZONE_BUNDLE_METADATA_FILENAME: &str = "metadata.toml";

/// Errors related to managing service zone bundles.
#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("TOML serialization failure")]
    Serialization(#[from] toml::ser::Error),

    #[error("TOML deserialization failure")]
    Deserialization(#[from] toml::de::Error),

    #[error("No zone named '{name}' is available for bundling")]
    NoSuchZone { name: String },

    #[error("No storage available for bundles")]
    NoStorage,

    #[error("Failed to join zone bundling task")]
    Task(#[from] tokio::task::JoinError),

    #[error("Failed to create bundle: {0}")]
    BundleFailed(#[from] anyhow::Error),

    #[error("Zone error")]
    Zone(#[from] AdmError),

    #[error(transparent)]
    PathBuf(#[from] FromPathBufError),

    #[error("Zone '{name}' cannot currently be bundled")]
    Unavailable { name: String },
}

/// Create a service bundle for the provided zone.
///
/// This runs a series of debugging commands in the zone, to collect data about
/// the state of the zone and any Oxide service processes running inside. The
/// data is packaged into a tarball, and placed in the provided output
/// directories.
pub async fn create(
    log: &Logger,
    zone: &RunningZone,
    context: &ZoneBundleContext,
) -> Result<ZoneBundleMetadata, BundleError> {
    // Fetch the directory into which we'll store data, and ensure it exists.
    if context.storage_dirs.is_empty() {
        warn!(log, "no directories available for zone bundles");
        return Err(BundleError::NoStorage);
    }
    info!(log, "creating zone bundle"; "zone" => zone.name());
    let mut zone_bundle_dirs = Vec::with_capacity(context.storage_dirs.len());
    for dir in context.storage_dirs.iter() {
        let bundle_dir = dir.join(zone.name());
        debug!(log, "creating bundle directory"; "dir" => %bundle_dir);
        tokio::fs::create_dir_all(&bundle_dir).await?;
        zone_bundle_dirs.push(bundle_dir);
    }

    // Create metadata and the tarball writer.
    //
    // We'll write the contents of the bundle into a gzipped tar archive,
    // including metadata and a file for the output of each command we run in
    // the zone.
    let zone_metadata = ZoneBundleMetadata::new(zone.name(), context.cause);
    let filename = format!("{}.tar.gz", zone_metadata.id.bundle_id);
    let full_path = zone_bundle_dirs[0].join(&filename);
    let file = match tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&full_path)
        .await
    {
        Ok(f) => f.into_std().await,
        Err(e) => {
            error!(
                log,
                "failed to create bundle file";
                "zone" => zone.name(),
                "file" => %full_path,
                "error" => ?e,
            );
            return Err(BundleError::from(e));
        }
    };
    debug!(
        log,
        "created bundle tarball file";
        "zone" => zone.name(),
        "path" => %full_path
    );
    let gz = flate2::GzBuilder::new()
        .filename(filename.as_str())
        .write(file, flate2::Compression::best());
    let mut builder = Builder::new(gz);

    // Helper function to write an array of bytes into the tar archive, with
    // the provided name.
    fn insert_data<W: std::io::Write>(
        builder: &mut Builder<W>,
        name: &str,
        contents: &[u8],
    ) -> Result<(), BundleError> {
        let mtime = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| anyhow::anyhow!("failed to compute mtime: {e}"))?
            .as_secs();

        let mut hdr = Header::new_ustar();
        hdr.set_size(contents.len().try_into().unwrap());
        hdr.set_mode(0o444);
        hdr.set_mtime(mtime);
        hdr.set_entry_type(tar::EntryType::Regular);
        // NOTE: This internally sets the path and checksum.
        builder
            .append_data(&mut hdr, name, Cursor::new(contents))
            .map_err(BundleError::from)
    }

    // Write the metadata file itself, in TOML format.
    let contents = toml::to_string(&zone_metadata)?;
    insert_data(
        &mut builder,
        ZONE_BUNDLE_METADATA_FILENAME,
        contents.as_bytes(),
    )?;
    debug!(
        log,
        "wrote zone bundle metadata";
        "zone" => zone.name(),
    );
    for cmd in ZONE_WIDE_COMMANDS {
        debug!(
            log,
            "running zone bundle command";
            "zone" => zone.name(),
            "command" => ?cmd,
        );
        let output = match zone.run_cmd(cmd) {
            Ok(s) => s,
            Err(e) => format!("{}", e),
        };
        let contents = format!("Command: {:?}\n{}", cmd, output).into_bytes();
        if let Err(e) = insert_data(&mut builder, cmd[0], &contents) {
            error!(
                log,
                "failed to save zone bundle command output";
                "zone" => zone.name(),
                "command" => ?cmd,
                "error" => ?e,
            );
        }
    }

    // Run any caller-requested zone-specific commands.
    for (i, cmd) in context.zone_specific_commands.iter().enumerate() {
        if cmd.is_empty() {
            continue;
        }
        debug!(
            log,
            "running user-requested zone bundle command";
            "zone" => zone.name(),
            "command" => ?cmd,
        );
        let output = match zone.run_cmd(cmd) {
            Ok(s) => s,
            Err(e) => format!("{}", e),
        };
        let contents = format!("Command: {:?}\n{}", cmd, output).into_bytes();

        // We'll insert the index into the filename as well, since it's
        // plausible that users will run multiple executions of the same
        // command.
        let filename = format!("zone-specific-{}-{}", i, &cmd[0]);
        if let Err(e) = insert_data(&mut builder, &filename, &contents) {
            error!(
                log,
                "failed to save zone bundle command output";
                "zone" => zone.name(),
                "command" => ?cmd,
                "error" => ?e,
            );
        }
    }

    // Debugging commands run on the specific processes this zone defines.
    const ZONE_PROCESS_COMMANDS: [&str; 3] = [
        "pfiles", "pstack",
        "pargs",
        // TODO-completeness: We may want `gcore`, since that encompasses
        // the above commands and much more. It seems like overkill now,
        // however.
    ];
    let procs = match zone.service_processes() {
        Ok(p) => {
            debug!(
                log,
                "enumerated service processes";
                "zone" => zone.name(),
                "procs" => ?p,
            );
            p
        }
        Err(e) => {
            error!(
                log,
                "failed to enumerate zone service processes";
                "zone" => zone.name(),
                "error" => ?e,
            );
            let err = anyhow::anyhow!(
                "failed to enumerate zone service processes: {e}"
            );
            return Err(BundleError::from(err));
        }
    };
    for svc in procs.into_iter() {
        let pid_s = svc.pid.to_string();
        for cmd in ZONE_PROCESS_COMMANDS {
            let args = &[cmd, &pid_s];
            debug!(
                log,
                "running zone bundle command";
                "zone" => zone.name(),
                "command" => ?args,
            );
            let output = match zone.run_cmd(args) {
                Ok(s) => s,
                Err(e) => format!("{}", e),
            };
            let contents =
                format!("Command: {:?}\n{}", args, output).into_bytes();

            // There may be multiple Oxide service processes for which we
            // want to capture the command output. Name each output after
            // the command and PID to disambiguate.
            let filename = format!("{}.{}", cmd, svc.pid);
            if let Err(e) = insert_data(&mut builder, &filename, &contents) {
                error!(
                    log,
                    "failed to save zone bundle command output";
                    "zone" => zone.name(),
                    "command" => ?args,
                    "error" => ?e,
                );
            }
        }

        // We may need to extract log files that have been archived out of the
        // zone filesystem itself. See `crate::dump_setup` for the logic which
        // does this.
        let archived_log_files = find_archived_log_files(
            log,
            zone.name(),
            &svc.service_name,
            &context.extra_log_dirs,
        )
        .await;

        // Copy any log files, current and rotated, into the tarball as
        // well.
        //
        // Safety: This pathbuf was retrieved by locating an existing file
        // on the filesystem, so we're sure it has a name and the unwrap is
        // safe.
        debug!(
            log,
            "appending current log file to zone bundle";
            "zone" => zone.name(),
            "log_file" => %svc.log_file,
        );
        if let Err(e) = builder.append_path_with_name(
            &svc.log_file,
            svc.log_file.file_name().unwrap(),
        ) {
            error!(
                log,
                "failed to append current log file to zone bundle";
                "zone" => zone.name(),
                "log_file" => %svc.log_file,
                "error" => ?e,
            );
            return Err(e.into());
        }
        for f in svc.rotated_log_files.iter().chain(archived_log_files.iter()) {
            debug!(
                log,
                "appending rotated log file to zone bundle";
                "zone" => zone.name(),
                "log_file" => %f,
            );
            if let Err(e) =
                builder.append_path_with_name(f, f.file_name().unwrap())
            {
                error!(
                    log,
                    "failed to append rotated log file to zone bundle";
                    "zone" => zone.name(),
                    "log_file" => %f,
                    "error" => ?e,
                );
                return Err(e.into());
            }
        }
    }

    // Finish writing out the tarball itself.
    builder
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to build bundle: {e}"))?;

    // Copy the bundle to the other locations. We really want the bundles to
    // be duplicates, not an additional, new bundle.
    for other_dir in zone_bundle_dirs[1..].iter() {
        let to = other_dir.join(&filename);
        debug!(log, "copying bundle"; "from" => %full_path, "to" => %to);
        tokio::fs::copy(&full_path, to).await?;
    }

    info!(log, "finished zone bundle"; "metadata" => ?zone_metadata);
    Ok(zone_metadata)
}

// Find log files for the specified zone / SMF service, which may have been
// archived out to a U.2 dataset.
//
// Note that errors are logged, rather than failing the whole function, so that
// one failed listing does not prevent collecting any other log files.
async fn find_archived_log_files(
    log: &Logger,
    zone_name: &str,
    svc_name: &str,
    dirs: &[Utf8PathBuf],
) -> Vec<Utf8PathBuf> {
    // The `dirs` should be things like
    // `/pool/ext/<ZPOOL_UUID>/crypt/debug/<ZONE_NAME>`, but it's really up to
    // the caller to verify these exist and possibly contain what they expect.
    //
    // Within that, we'll just look for things that appear to be Oxide-managed
    // SMF service log files.
    let mut files = Vec::new();
    for dir in dirs.iter() {
        if dir.exists() {
            let mut rd = match tokio::fs::read_dir(&dir).await {
                Ok(rd) => rd,
                Err(e) => {
                    error!(
                        log,
                        "failed to read zone debug directory";
                        "directory" => ?dir,
                        "reason" => ?e,
                    );
                    continue;
                }
            };
            loop {
                match rd.next_entry().await {
                    Ok(None) => break,
                    Ok(Some(entry)) => {
                        let Ok(path) = Utf8PathBuf::try_from(entry.path()) else {
                            error!(
                                log,
                                "skipping possible archived log file with \
                                non-UTF-8 path";
                                "path" => ?entry.path(),
                            );
                            continue;
                        };
                        let fname = path.file_name().unwrap();
                        if is_oxide_smf_log_file(fname)
                            && fname.contains(svc_name)
                        {
                            debug!(
                                log,
                                "found archived log file";
                                "zone_name" => zone_name,
                                "service_name" => svc_name,
                                "path" => ?path,
                            );
                            files.push(path);
                        }
                    }
                    Err(e) => {
                        error!(
                            log,
                            "failed to fetch zone debug directory entry";
                            "directory" => ?dir,
                            "reason" => ?e,
                        );
                    }
                }
            }
        } else {
            // The logic in `dump_setup` picks some U.2 in which to start
            // archiving logs, and thereafter tries to keep placing new ones
            // there, subject to space constraints. It's not really an error for
            // there to be no entries for the named zone in any particular U.2
            // debug dataset.
            slog::trace!(
                log,
                "attempting to find archived log files in \
                non-existent directory";
                "directory" => ?dir,
            );
        }
    }
    files
}

// Extract the zone bundle metadata from a file, if it exists.
fn extract_zone_bundle_metadata_impl(
    path: &std::path::PathBuf,
) -> Result<ZoneBundleMetadata, BundleError> {
    // Build a reader for the whole archive.
    let reader = std::fs::File::open(path).map_err(BundleError::from)?;
    let buf_reader = std::io::BufReader::new(reader);
    let gz = GzDecoder::new(buf_reader);
    let mut archive = Archive::new(gz);

    // Find the metadata entry, if it exists.
    let entries = archive.entries()?;
    let Some(md_entry) = entries
        // The `Archive::entries` iterator
        // returns a result, so filter to those
        // that are OK first.
        .filter_map(Result::ok)
        .find(|entry| {
            entry
                .path()
                .map(|p| p.to_str() == Some(ZONE_BUNDLE_METADATA_FILENAME))
                .unwrap_or(false)
        })
    else {
        return Err(BundleError::from(
            anyhow::anyhow!("Zone bundle is missing metadata file")
        ));
    };

    // Extract its contents and parse as metadata.
    let contents = std::io::read_to_string(md_entry)?;
    toml::from_str(&contents).map_err(BundleError::from)
}

/// List the extant zone bundles for the provided zone, in the provided
/// directory.
pub async fn list_bundles_for_zone(
    log: &Logger,
    path: &Utf8Path,
    zone_name: &str,
) -> Result<Vec<(Utf8PathBuf, ZoneBundleMetadata)>, BundleError> {
    let mut bundles = Vec::new();
    let zone_bundle_dir = path.join(zone_name);
    if zone_bundle_dir.is_dir() {
        let mut dir = tokio::fs::read_dir(zone_bundle_dir)
            .await
            .map_err(BundleError::from)?;
        while let Some(zone_bundle) =
            dir.next_entry().await.map_err(BundleError::from)?
        {
            let bundle_path = zone_bundle.path();
            debug!(
                log,
                "checking possible zone bundle";
                "bundle_path" => %bundle_path.display(),
            );

            // Zone bundles _should_ be named like:
            //
            // .../bundle/zone/<zone_name>/<bundle_id>.tar.gz.
            //
            // However, really a zone bundle is any tarball with the
            // right metadata file, which contains a TOML-serialized
            // `ZoneBundleMetadata` file. Try to create an archive out
            // of each file we find in this directory, and parse out a
            // metadata file.
            let tarball = bundle_path.to_owned();
            let metadata = match extract_zone_bundle_metadata(tarball).await {
                Ok(md) => md,
                Err(e) => {
                    error!(
                        log,
                        "failed to read zone bundle metadata";
                        "error" => ?e,
                    );
                    return Err(e);
                }
            };
            debug!(log, "found zone bundle"; "metadata" => ?metadata);
            bundles.push((Utf8PathBuf::try_from(bundle_path)?, metadata));
        }
    }
    Ok(bundles)
}

/// Extract zone bundle metadata from the provided file, if possible.
pub async fn extract_zone_bundle_metadata(
    path: PathBuf,
) -> Result<ZoneBundleMetadata, BundleError> {
    let task = tokio::task::spawn_blocking(move || {
        extract_zone_bundle_metadata_impl(&path)
    });
    task.await?
}

/// Get the path to a zone bundle, if it exists.
pub async fn get_zone_bundle_path(
    log: &Logger,
    directories: &[Utf8PathBuf],
    zone_name: &str,
    id: &Uuid,
) -> Result<Option<Utf8PathBuf>, BundleError> {
    for path in directories {
        debug!(log, "searching zone bundle directory"; "directory" => ?path);
        let zone_bundle_dir = path.join(zone_name);
        if zone_bundle_dir.is_dir() {
            let mut dir = tokio::fs::read_dir(zone_bundle_dir)
                .await
                .map_err(BundleError::from)?;
            while let Some(zone_bundle) =
                dir.next_entry().await.map_err(BundleError::from)?
            {
                let path = zone_bundle.path();
                let metadata = match extract_zone_bundle_metadata(path).await {
                    Ok(md) => md,
                    Err(e) => {
                        error!(
                            log,
                            "failed to read zone bundle metadata";
                            "error" => ?e,
                        );
                        return Err(e);
                    }
                };
                let bundle_id = &metadata.id;
                if bundle_id.zone_name == zone_name
                    && bundle_id.bundle_id == *id
                {
                    return Utf8PathBuf::try_from(zone_bundle.path())
                        .map(|p| Some(p))
                        .map_err(BundleError::from);
                }
            }
        }
    }
    Ok(None)
}
