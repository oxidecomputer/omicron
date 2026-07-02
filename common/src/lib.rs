// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Oxide Control Plane
//!
//! The overall architecture for the Oxide Control Plane is described in [RFD
//! 61](https://61.rfd.oxide.computer/).  This crate implements common facilities
//! used in the control plane.  Other top-level crates implement pieces of the
//! control plane (e.g., `omicron_nexus`).
//!
//! The best documentation for the control plane is RFD 61 and the rustdoc in
//! this crate.  Since this crate doesn't provide externally-consumable
//! interfaces, the rustdoc (generated with `--document-private-items`) is
//! intended primarily for engineers working on this crate.

// We only use rustdoc for internal documentation, including private items, so
// it's expected that we'll have links to private items in the docs.
#![allow(rustdoc::private_intra_doc_links)]

pub mod address;
pub mod api;
pub mod backoff;
pub mod cmd;
pub mod disk;
pub mod policy;
pub mod resolvable_files;
pub mod snake_case_result;
pub mod update;
pub mod vlan;
pub mod zpool_name;

/// A type that allows adding file and line numbers to log messages
/// automatically. It should be instantiated at the root logger of each
/// executable that desires this functionality, as in the following example.
/// ```ignore
///     slog::Logger::root(drain, o!(FileKv))
/// ```
pub struct FileKv;

impl slog::KV for FileKv {
    fn serialize(
        &self,
        record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        // Only log file information when severity is at least info level
        if record.level() > slog::Level::Info {
            return Ok(());
        }
        serializer.emit_arguments(
            "file".into(),
            &format_args!("{}:{}", record.file(), record.line()),
        )
    }
}

/// Returns the current time, truncated to the previous microsecond.
///
/// This exists because the database doesn't store nanosecond-precision, so if
/// we store nanosecond-precision timestamps, then DateTime conversion is lossy
/// when round-tripping through the database.  That's rather inconvenient.
///
/// To truncate an arbitrary timestamp to database precision, use
/// [`timestamp_db_precision`].
pub fn now_db_precision() -> chrono::DateTime<chrono::Utc> {
    timestamp_db_precision(chrono::Utc::now())
}

/// Truncates a [`chrono::DateTime`]`<`[`chrono::Utc`]`>` to the previous
/// microsecond.
///
/// This exists because the database doesn't store nanosecond-precision, so if
/// we store nanosecond-precision timestamps, then `DateTime`conversion is lossy
/// when round-tripping through the database.  That's rather inconvenient.
pub fn timestamp_db_precision(
    ts: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    let nanosecs = ts.timestamp_subsec_nanos();
    let micros = ts.timestamp_subsec_micros();
    let only_nanos = nanosecs - micros * 1000;
    ts - std::time::Duration::from_nanos(u64::from(only_nanos))
}

/// Format a [`std::time::Duration`] as a human-readable string
/// (e.g. `"1h 5m 23ms"`), truncated to millisecond precision.
pub fn format_duration_ms(duration: std::time::Duration) -> String {
    // Ignore units smaller than a millisecond.
    let elapsed = std::time::Duration::from_millis(
        u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
    );
    humantime::format_duration(elapsed).to_string()
}

/// Format a [`chrono::TimeDelta`] as a human-readable string (see
/// [`format_duration_ms`]).
pub fn format_time_delta(time_delta: chrono::TimeDelta) -> String {
    match time_delta.to_std() {
        Ok(d) => format_duration_ms(d),
        Err(_) => String::from("<time delta out of range>"),
    }
}

/// The system version of the Oxide software (RFD 557): the `N.P.C` form,
/// without the `+gitHHHH` build metadata that a published release carries.
///
/// Under current policy, each new release is a major version bump, and
/// generally referred to only by the major version (e.g. 8.0.0 is referred
/// to as "v8", "version 8", or "release 8" to customers). The use of semantic
/// versioning is mostly to hedge for perhaps wanting something more granular in
/// the future.
///
/// The releng build process appends build metadata to this to produce the full
/// version string. It is the compile-time fallback for [`system_version`],
/// which prefers the full stamped string when running in a deployed zone.
pub const SYSTEM_VERSION: semver::Version = semver::Version::new(22, 0, 0);

/// Path, inside a deployed zone, of the file holding the full stamped system
/// version string (e.g. `21.0.0-0.ci+git0abc1234def`).
///
/// `omicron-package stamp` writes the version into each zone image's top-level
/// `oxide.json` metadata, but that metadata is only readable from the global
/// zone (by re-opening the image tarball); a process running *inside* a zone
/// cannot see it. To make the full version available to Nexus at runtime, the
/// stamp step also writes it to this path within the zone filesystem. The file
/// is absent in dev/test builds and in unstamped packages.
const SYSTEM_VERSION_PATH: &str = "/var/oxide/system-version";

/// The full version of the running system software, as served by `/v1/version`.
///
/// Returns the stamped version string from [`SYSTEM_VERSION_PATH`] when present
/// (a deployed, stamped zone), otherwise the compile-time [`SYSTEM_VERSION`]
/// core. The result is read once and cached: a system update replaces the zone
/// image and restarts the process, so the file cannot change under a running
/// server.
pub fn system_version() -> &'static semver::Version {
    static VERSION: std::sync::OnceLock<semver::Version> =
        std::sync::OnceLock::new();
    VERSION.get_or_init(|| {
        read_system_version(std::path::Path::new(SYSTEM_VERSION_PATH))
    })
}

/// Read and parse the stamped system version from `path`, falling back to
/// [`SYSTEM_VERSION`] when the file is absent or does not contain a valid
/// semver. Split out from [`system_version`] (which caches) to be testable.
fn read_system_version(path: &std::path::Path) -> semver::Version {
    match std::fs::read_to_string(path) {
        Ok(contents) => contents.trim().parse().unwrap_or(SYSTEM_VERSION),
        Err(_) => SYSTEM_VERSION,
    }
}

pub const OMICRON_DPD_TAG: &str = "omicron";

/// A wrapper struct that does nothing other than elide the inner value from
/// [`std::fmt::Debug`] output.
///
/// We define this within Omicron instead of using one of the many available
/// crates that do the same thing because it's trivial to do so, and we want the
/// flexibility to add traits to this type without needing to wait on upstream
/// to add an optional dependency.
///
/// If you want to use this for secrets, consider that it might not do
/// everything you expect (it does not zeroize memory on drop, nor get in the
/// way of you removing the inner value from this wrapper struct).
#[derive(
    Clone, Copy, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct NoDebug<T>(pub T);

impl<T> std::fmt::Debug for NoDebug<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "..")
    }
}

/// Produce an OpenAPI schema describing a hex string of a specific byte length.
///
/// Used by versioned sled-agent types to preserve schema compatibility. New
/// code should use `byte_wrapper::HexArray<N>` which implements `JsonSchema`
/// directly.
pub fn hex_schema<const N: usize>(
    generator: &mut schemars::SchemaGenerator,
) -> schemars::schema::Schema {
    use schemars::JsonSchema;

    let mut schema: schemars::schema::SchemaObject =
        <String>::json_schema(generator).into();
    schema.format = Some(format!("hex string ({N} bytes)"));
    schema.into()
}

/// A simple wrapper around a byte slice that provides a [`std::fmt::Debug`]
/// impl which writes the bytes as a hex string.
///
/// # Example
///
/// ```
/// # use omicron_common::BytesToHexDebug;
/// assert_eq!(
///     format!("{:?}", BytesToHexDebug(&[1, 234, 56, 255, 11])),
///     "01ea38ff0b",
/// );
/// assert_eq!(
///     format!("{:?}", BytesToHexDebug("Hello World!".as_bytes())),
///     "48656c6c6f20576f726c6421",
/// );
/// ```
pub struct BytesToHexDebug<'a>(pub &'a [u8]);

impl std::fmt::Debug for BytesToHexDebug<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::fmt::Write;
        const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
        for b in self.0 {
            let upper_nib = HEX_CHARS[(b >> 4) as usize] as char;
            let lower_nib = HEX_CHARS[(b & 0xF) as usize] as char;
            f.write_char(upper_nib)?;
            f.write_char(lower_nib)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{SYSTEM_VERSION, read_system_version};
    use camino_tempfile::NamedUtf8TempFile;
    use std::io::Write;

    fn write_version_file(contents: &str) -> NamedUtf8TempFile {
        let mut file = NamedUtf8TempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn system_version_falls_back_when_file_absent() {
        let path = camino::Utf8Path::new("/nonexistent/system-version");
        assert_eq!(read_system_version(path.as_std_path()), SYSTEM_VERSION);
    }

    #[test]
    fn system_version_reads_full_stamped_string() {
        // The stamped string carries prerelease and build metadata that the
        // compile-time SYSTEM_VERSION core lacks. The trailing newline tests
        // that whitespace around the version is trimmed.
        const STAMPED_VERSION: &str = "21.0.0-0.ci+git0abc1234def";
        let expected: semver::Version = STAMPED_VERSION.parse().unwrap();
        let file = write_version_file(&format!("{STAMPED_VERSION}\n"));
        let version = read_system_version(file.path().as_std_path());
        assert_eq!(version, expected);
    }

    #[test]
    fn system_version_falls_back_on_unparseable_file() {
        let file = write_version_file("not a version");
        assert_eq!(
            read_system_version(file.path().as_std_path()),
            SYSTEM_VERSION
        );
    }
}
