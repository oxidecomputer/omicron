// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database schema versions and upgrades
//!
//! For details, see schema/crdb/README.adoc in the root of this repository.

use anyhow::{bail, ensure, Context};
use camino::Utf8Path;
use omicron_common::api::external::SemverVersion;
use once_cell::sync::Lazy;
use std::collections::BTreeMap;

/// The version of the database schema this particular version of Nexus was
/// built against
///
/// This must be updated when you change the database schema.  Refer to
/// schema/crdb/README.adoc in the root of this repository for details.
pub const SCHEMA_VERSION: SemverVersion = SemverVersion::new(53, 0, 0);

/// List of all past database schema versions, in *reverse* order
///
/// If you want to change the Omicron database schema, you must update this.
static KNOWN_VERSIONS: Lazy<Vec<KnownVersion>> = Lazy::new(|| {
    vec![
        // +- The next version goes here!  Duplicate this line, uncomment
        // |  the *second* copy, then update that copy for your version,
        // |  leaving the first copy as an example for the next person.
        // v
        // KnownVersion::new(next_int, "unique-dirname-with-the-sql-files"),
        KnownVersion::new(53, "drop-service-table"),
        KnownVersion::new(52, "blueprint-physical-disk"),
        KnownVersion::new(51, "blueprint-disposition-column"),
        KnownVersion::new(50, "add-lookup-disk-by-volume-id-index"),
        KnownVersion::new(49, "physical-disk-state-and-policy"),
        KnownVersion::new(48, "add-metrics-producers-time-modified-index"),
        KnownVersion::new(47, "add-view-for-bgp-peer-configs"),
        KnownVersion::new(46, "first-named-migration"),
        // The first many schema versions only vary by major or patch number and
        // their path is predictable based on the version number.  (This was
        // historically a problem because two pull requests both adding a new
        // schema version might merge cleanly but produce an invalid result.)
        KnownVersion::legacy(45, 0),
        KnownVersion::legacy(44, 0),
        KnownVersion::legacy(43, 0),
        KnownVersion::legacy(42, 0),
        KnownVersion::legacy(41, 0),
        KnownVersion::legacy(40, 0),
        KnownVersion::legacy(39, 0),
        KnownVersion::legacy(38, 0),
        KnownVersion::legacy(37, 1),
        KnownVersion::legacy(37, 0),
        KnownVersion::legacy(36, 0),
        KnownVersion::legacy(35, 0),
        KnownVersion::legacy(34, 0),
        KnownVersion::legacy(33, 1),
        KnownVersion::legacy(33, 0),
        KnownVersion::legacy(32, 0),
        KnownVersion::legacy(31, 0),
        KnownVersion::legacy(30, 0),
        KnownVersion::legacy(29, 0),
        KnownVersion::legacy(28, 0),
        KnownVersion::legacy(27, 0),
        KnownVersion::legacy(26, 0),
        KnownVersion::legacy(25, 0),
        KnownVersion::legacy(24, 0),
        KnownVersion::legacy(23, 1),
        KnownVersion::legacy(23, 0),
        KnownVersion::legacy(22, 0),
        KnownVersion::legacy(21, 0),
        KnownVersion::legacy(20, 0),
        KnownVersion::legacy(19, 0),
        KnownVersion::legacy(18, 0),
        KnownVersion::legacy(17, 0),
        KnownVersion::legacy(16, 0),
        KnownVersion::legacy(15, 0),
        KnownVersion::legacy(14, 0),
        KnownVersion::legacy(13, 0),
        KnownVersion::legacy(12, 0),
        KnownVersion::legacy(11, 0),
        KnownVersion::legacy(10, 0),
        KnownVersion::legacy(9, 0),
        KnownVersion::legacy(8, 0),
        KnownVersion::legacy(7, 0),
        KnownVersion::legacy(6, 0),
        KnownVersion::legacy(5, 0),
        KnownVersion::legacy(4, 0),
        KnownVersion::legacy(3, 3),
        KnownVersion::legacy(3, 2),
        KnownVersion::legacy(3, 1),
        KnownVersion::legacy(3, 0),
        KnownVersion::legacy(2, 0),
        KnownVersion::legacy(1, 0),
    ]
});

/// The earliest supported schema version.
pub const EARLIEST_SUPPORTED_VERSION: SemverVersion =
    SemverVersion::new(1, 0, 0);

/// Describes one version of the database schema
#[derive(Debug, Clone)]
struct KnownVersion {
    /// All versions have an associated SemVer.  We only use the major number in
    /// terms of determining compatibility.
    semver: SemverVersion,

    /// Path relative to the root of the schema ("schema/crdb" in the root of
    /// this repo) where this version's update SQL files are stored
    relative_path: String,
}

impl KnownVersion {
    /// Generate a `KnownVersion` for a new schema version
    ///
    /// `major` should be the next available integer (one more than the previous
    /// version's major number).
    ///
    /// `relative_path` is the path relative to "schema/crdb" (from the root of
    /// this repository) where the SQL files live that will update the schema
    /// from the previous version to this version.
    fn new(major: u64, relative_path: &str) -> KnownVersion {
        let semver = SemverVersion::new(major, 0, 0);
        KnownVersion { semver, relative_path: relative_path.to_owned() }
    }

    /// Generate a `KnownVersion` for a version that predates the current
    /// directory naming scheme
    ///
    /// These versions varied in both major and patch numbers and the path to
    /// their SQL files was predictable based solely on the version.
    ///
    /// **This should not be used for new schema versions.**
    fn legacy(major: u64, patch: u64) -> KnownVersion {
        let semver = SemverVersion::new(major, 0, patch);
        let relative_path = semver.to_string();
        KnownVersion { semver, relative_path }
    }
}

impl std::fmt::Display for KnownVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.semver.fmt(f)
    }
}

/// Load and inspect the set of all known schema versions
#[derive(Debug, Clone)]
pub struct AllSchemaVersions {
    versions: BTreeMap<SemverVersion, SchemaVersion>,
}

impl AllSchemaVersions {
    /// Load the set of all known schema versions from the given directory tree
    ///
    /// The directory should contain exactly one directory for each version.
    /// Each version's directory should contain the SQL files that carry out
    /// schema migration from the previous version.  See schema/crdb/README.adoc
    /// for details.
    pub fn load(
        schema_directory: &Utf8Path,
    ) -> Result<AllSchemaVersions, anyhow::Error> {
        Self::load_known_versions(schema_directory, KNOWN_VERSIONS.iter())
    }

    /// Load a specific set of known schema versions using the legacy
    /// conventions from the given directory tree
    ///
    /// This is only provided for certain integration tests.
    #[doc(hidden)]
    pub fn load_specific_legacy_versions<'a>(
        schema_directory: &Utf8Path,
        versions: impl Iterator<Item = &'a SemverVersion>,
    ) -> Result<AllSchemaVersions, anyhow::Error> {
        let known_versions: Vec<_> = versions
            .map(|v| {
                assert_eq!(v.0.minor, 0);
                KnownVersion::legacy(v.0.major, v.0.patch)
            })
            .collect();

        Self::load_known_versions(schema_directory, known_versions.iter())
    }

    fn load_known_versions<'a>(
        schema_directory: &Utf8Path,
        known_versions: impl Iterator<Item = &'a KnownVersion>,
    ) -> Result<AllSchemaVersions, anyhow::Error> {
        let mut versions = BTreeMap::new();
        for known_version in known_versions {
            let version_path =
                schema_directory.join(&known_version.relative_path);
            let schema_version = SchemaVersion::load_from_directory(
                known_version.semver.clone(),
                &version_path,
            )
            .with_context(|| {
                format!(
                    "loading schema version {} from {:?}",
                    known_version.semver, schema_directory,
                )
            })?;

            versions.insert(known_version.semver.clone(), schema_version);
        }

        Ok(AllSchemaVersions { versions })
    }

    /// Iterate over the set of all known schema versions in order starting with
    /// the earliest supported version
    pub fn iter_versions(&self) -> impl Iterator<Item = &SchemaVersion> {
        self.versions.values()
    }

    /// Return whether `version` is a known schema version
    pub fn contains_version(&self, version: &SemverVersion) -> bool {
        self.versions.contains_key(version)
    }

    /// Iterate over the known schema versions within `bounds`
    ///
    /// This is generally used to iterate over all the schema versions between
    /// two specific versions.
    pub fn versions_range<R>(
        &self,
        bounds: R,
    ) -> impl Iterator<Item = &'_ SchemaVersion>
    where
        R: std::ops::RangeBounds<SemverVersion>,
    {
        self.versions.range(bounds).map(|(_, v)| v)
    }
}

/// Describes a single version of the schema, including the SQL steps to get
/// from the previous version to the current one
#[derive(Debug, Clone)]
pub struct SchemaVersion {
    semver: SemverVersion,
    upgrade_from_previous: Vec<SchemaUpgradeStep>,
}

impl SchemaVersion {
    /// Reads a "version directory" and reads all SQL changes into a result Vec.
    ///
    /// Files that do not begin with "up" and end with ".sql" are ignored. The
    /// collection of `up*.sql` files must fall into one of these two
    /// conventions:
    ///
    /// * "up.sql" with no other files
    /// * "up1.sql", "up2.sql", ..., beginning from 1, optionally with leading
    ///   zeroes (e.g., "up01.sql", "up02.sql", ...). There is no maximum value,
    ///   but there may not be any gaps (e.g., if "up2.sql" and "up4.sql" exist,
    ///   so must "up3.sql") and there must not be any repeats (e.g., if
    ///   "up1.sql" exists, "up01.sql" must not exist).
    ///
    /// Any violation of these two rules will result in an error. Collections of
    /// the second form (`up1.sql`, ...) will be sorted numerically.
    fn load_from_directory(
        semver: SemverVersion,
        directory: &Utf8Path,
    ) -> Result<SchemaVersion, anyhow::Error> {
        let mut up_sqls = vec![];
        let entries = directory
            .read_dir_utf8()
            .with_context(|| format!("Failed to readdir {directory}"))?;
        for entry in entries {
            let entry = entry.with_context(|| {
                format!("Reading {directory:?}: invalid entry")
            })?;
            let pathbuf = entry.into_path();

            // Ensure filename ends with ".sql"
            if pathbuf.extension() != Some("sql") {
                continue;
            }

            // Ensure filename begins with "up", and extract anything in between
            // "up" and ".sql".
            let Some(remaining_filename) = pathbuf
                .file_stem()
                .and_then(|file_stem| file_stem.strip_prefix("up"))
            else {
                continue;
            };

            // Ensure the remaining filename is either empty (i.e., the filename
            // is exactly "up.sql") or parseable as an unsigned integer. We give
            // "up.sql" the "up_number" 0 (checked in the loop below), and
            // require any other number to be nonzero.
            if remaining_filename.is_empty() {
                up_sqls.push((0, pathbuf));
            } else {
                let Ok(up_number) = remaining_filename.parse::<u64>() else {
                    bail!(
                        "invalid filename (non-numeric `up*.sql`): {pathbuf}",
                    );
                };
                ensure!(
                    up_number != 0,
                    "invalid filename (`up*.sql` numbering must start at 1): \
                     {pathbuf}",
                );
                up_sqls.push((up_number, pathbuf));
            }
        }
        up_sqls.sort();

        // Validate that we have a reasonable sequence of `up*.sql` numbers.
        match up_sqls.as_slice() {
            [] => bail!("no `up*.sql` files found"),
            [(up_number, path)] => {
                // For a single file, we allow either `up.sql` (keyed as
                // up_number=0) or `up1.sql`; reject any higher number.
                ensure!(
                    *up_number <= 1,
                    "`up*.sql` numbering must start at 1: found first file \
                     {path}"
                );
            }
            _ => {
                for (i, (up_number, path)) in up_sqls.iter().enumerate() {
                    // We have 2 or more `up*.sql`; they should be numbered
                    // exactly 1..=up_sqls.len().
                    if i as u64 + 1 != *up_number {
                        // We know we have at least two elements, so report an
                        // error referencing either the next item (if we're
                        // first) or the previous item (if we're not first).
                        let (path_a, path_b) = if i == 0 {
                            let (_, next_path) = &up_sqls[1];
                            (path, next_path)
                        } else {
                            let (_, prev_path) = &up_sqls[i - 1];
                            (prev_path, path)
                        };
                        bail!("invalid `up*.sql` sequence: {path_a}, {path_b}");
                    }
                }
            }
        }

        // This collection of `up*.sql` files is valid.  Read them all, in
        // order.
        let mut steps = vec![];
        for (_, path) in up_sqls.into_iter() {
            let sql = std::fs::read_to_string(&path)
                .with_context(|| format!("Cannot read {path}"))?;
            // unwrap: `file_name()` is documented to return `None` only when
            // the path is `..`.  But we got this path from reading the
            // directory, and that process explicitly documents that it skips
            // `..`.
            steps.push(SchemaUpgradeStep {
                label: path.file_name().unwrap().to_string(),
                sql,
            });
        }

        Ok(SchemaVersion { semver, upgrade_from_previous: steps })
    }

    /// Returns the semver for this schema version
    pub fn semver(&self) -> &SemverVersion {
        &self.semver
    }

    /// Returns true if this schema version is the one that the current program
    /// thinks is the latest (current) one
    pub fn is_current_software_version(&self) -> bool {
        self.semver == SCHEMA_VERSION
    }

    /// Iterate over the SQL steps required to update the database schema from
    /// the previous version to this one
    pub fn upgrade_steps(&self) -> impl Iterator<Item = &SchemaUpgradeStep> {
        self.upgrade_from_previous.iter()
    }
}

impl std::fmt::Display for SchemaVersion {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        self.semver.fmt(f)
    }
}

/// Describes a single file containing a schema change, as SQL.
#[derive(Debug, Clone)]
pub struct SchemaUpgradeStep {
    label: String,
    sql: String,
}

impl SchemaUpgradeStep {
    /// Returns a human-readable name for this step (the name of the file it
    /// came from)
    pub fn label(&self) -> &str {
        self.label.as_ref()
    }

    /// Returns the actual SQL to execute for this step
    pub fn sql(&self) -> &str {
        self.sql.as_ref()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use camino_tempfile::Utf8TempDir;

    #[test]
    fn test_known_versions() {
        if let Err(error) = verify_known_versions(
            // The real list is defined in reverse order for developer
            // convenience so we reverse it before processing.
            KNOWN_VERSIONS.iter().rev(),
            &EARLIEST_SUPPORTED_VERSION,
            &SCHEMA_VERSION,
            // Versions after 45 obey our modern, stricter rules.
            45,
        ) {
            panic!("problem with static configuration: {:#}", error);
        }
    }

    // (Test the test function)
    #[test]
    fn test_verify() {
        // EARLIEST_SUPPORTED_VERSION is somehow wrong
        let error = verify_known_versions(
            [&KnownVersion::legacy(2, 0), &KnownVersion::legacy(3, 0)],
            &SemverVersion::new(1, 0, 0),
            &SemverVersion::new(3, 0, 0),
            100,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "EARLIEST_SUPPORTED_VERSION is not the earliest in KNOWN_VERSIONS"
        );

        // SCHEMA_VERSION was not updated
        let error = verify_known_versions(
            [&KnownVersion::legacy(1, 0), &KnownVersion::legacy(2, 0)],
            &SemverVersion::new(1, 0, 0),
            &SemverVersion::new(1, 0, 0),
            100,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "latest KNOWN_VERSION is 2.0.0, but SCHEMA_VERSION is 1.0.0"
        );

        // Latest version was duplicated instead of bumped (legacy)
        let error = verify_known_versions(
            [
                &KnownVersion::legacy(1, 0),
                &KnownVersion::legacy(2, 0),
                &KnownVersion::legacy(2, 0),
            ],
            &EARLIEST_SUPPORTED_VERSION,
            &SemverVersion::new(2, 0, 0),
            100,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "KNOWN_VERSION 2.0.0 appears directly after 2.0.0, but is not later"
        );

        // Latest version was duplicated instead of bumped (modern case)
        let error = verify_known_versions(
            [
                &KnownVersion::legacy(1, 0),
                &KnownVersion::new(2, "dir1"),
                &KnownVersion::new(2, "dir2"),
            ],
            &EARLIEST_SUPPORTED_VERSION,
            &SemverVersion::new(2, 0, 0),
            100,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "KNOWN_VERSION 2.0.0 appears directly after 2.0.0, but is not later"
        );

        // Version added out of order
        let error = verify_known_versions(
            [
                &KnownVersion::legacy(1, 0),
                &KnownVersion::legacy(2, 0),
                &KnownVersion::legacy(1, 3),
            ],
            &EARLIEST_SUPPORTED_VERSION,
            &SemverVersion::new(3, 0, 0),
            100,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "KNOWN_VERSION 1.0.3 appears directly after 2.0.0, but is not later"
        );

        // Gaps are not allowed.
        let error = verify_known_versions(
            [
                &KnownVersion::legacy(1, 0),
                &KnownVersion::legacy(2, 0),
                &KnownVersion::legacy(4, 0),
            ],
            &EARLIEST_SUPPORTED_VERSION,
            &SemverVersion::new(4, 0, 0),
            100,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "KNOWN_VERSION 4.0.0 appears directly after 2.0.0, but its major \
            number is neither the same nor one greater"
        );

        // For the strict case, the patch level can't be non-zero.  You can only
        // make this mistake by using `KnownVersion::legacy()` for a new
        // version.
        let error = verify_known_versions(
            [
                &KnownVersion::legacy(1, 0),
                &KnownVersion::legacy(2, 0),
                &KnownVersion::legacy(3, 2),
            ],
            &EARLIEST_SUPPORTED_VERSION,
            &SemverVersion::new(3, 0, 2),
            2,
        )
        .unwrap_err();
        assert_eq!(format!("{error:#}"), "new patch versions must be zero");

        // For the strict case, the directory name cannot contain the version at
        // all.  You can only make this mistake by using
        // `KnownVersion::legacy()` for a new version.
        let error = verify_known_versions(
            [
                &KnownVersion::legacy(1, 0),
                &KnownVersion::legacy(2, 0),
                &KnownVersion::legacy(3, 0),
            ],
            &EARLIEST_SUPPORTED_VERSION,
            &SemverVersion::new(3, 0, 0),
            2,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "the relative path for a version should not contain the \
            version itself"
        );
    }

    fn verify_known_versions<'a, I>(
        // list of known versions in order from earliest to latest
        known_versions: I,
        earliest: &SemverVersion,
        latest: &SemverVersion,
        min_strict_major: u64,
    ) -> Result<(), anyhow::Error>
    where
        I: IntoIterator<Item = &'a KnownVersion>,
    {
        let mut known_versions = known_versions.into_iter();

        // All known versions should be unique and increasing.
        let first =
            known_versions.next().expect("expected at least one KNOWN_VERSION");
        ensure!(
            first.semver == *earliest,
            "EARLIEST_SUPPORTED_VERSION is not the earliest in KNOWN_VERSIONS"
        );

        let mut prev = first;
        for v in known_versions {
            println!("checking known version: {} -> {}", prev, v);
            ensure!(
                v.semver > prev.semver,
                "KNOWN_VERSION {} appears directly after {}, but is not later",
                v,
                prev
            );

            // We currently make sure there are no gaps in the major number.
            // This is not strictly necessary but if this isn't true then it was
            // probably a mistake.
            //
            // It's allowed for the major numbers to be the same because a few
            // past schema versions only bumped the patch number for whatever
            // reason.
            ensure!(
                v.semver.0.major == prev.semver.0.major
                    || v.semver.0.major == prev.semver.0.major + 1,
                "KNOWN_VERSION {} appears directly after {}, but its major \
                number is neither the same nor one greater",
                v,
                prev
            );

            // We never allowed minor versions to be zero and it is not
            // currently possible to even construct one that had a non-zero
            // minor number.
            ensure!(v.semver.0.minor == 0, "new minor versions must be zero");

            // We changed things after version 45 to require that:
            //
            // (1) the major always be bumped (the minor and patch must be zero)
            // (2) users choose a unique directory name for the SQL files.  It
            //     would defeat the point if people used the semver for
            //
            // After version 45, we do not allow non-zero minor or patch
            // numbers.
            if v.semver.0.major > min_strict_major {
                ensure!(
                    v.semver.0.patch == 0,
                    "new patch versions must be zero"
                );
                ensure!(
                    !v.relative_path.contains(&v.semver.to_string()),
                    "the relative path for a version should not contain the \
                    version itself"
                );
            }

            prev = v;
        }

        ensure!(
            prev.semver == *latest,
            "latest KNOWN_VERSION is {}, but SCHEMA_VERSION is {}",
            prev,
            latest
        );

        Ok(())
    }

    // Confirm that `SchemaVersion::load_from_directory()` rejects `up*.sql`
    // files where the `*` doesn't contain a positive integer.
    #[tokio::test]
    async fn test_reject_invalid_up_sql_names() {
        for (invalid_filename, error_prefix) in [
            ("upA.sql", "invalid filename (non-numeric `up*.sql`)"),
            ("up1a.sql", "invalid filename (non-numeric `up*.sql`)"),
            ("upaaa1.sql", "invalid filename (non-numeric `up*.sql`)"),
            ("up-3.sql", "invalid filename (non-numeric `up*.sql`)"),
            (
                "up0.sql",
                "invalid filename (`up*.sql` numbering must start at 1)",
            ),
            (
                "up00.sql",
                "invalid filename (`up*.sql` numbering must start at 1)",
            ),
            (
                "up000.sql",
                "invalid filename (`up*.sql` numbering must start at 1)",
            ),
        ] {
            let tempdir = Utf8TempDir::new().unwrap();
            let filename = tempdir.path().join(invalid_filename);
            _ = tokio::fs::File::create(&filename).await.unwrap();
            let maybe_schema = SchemaVersion::load_from_directory(
                SemverVersion::new(12, 0, 0),
                tempdir.path(),
            );
            match maybe_schema {
                Ok(upgrade) => {
                    panic!(
                        "unexpected success on {invalid_filename} \
                         (produced {upgrade:?})"
                    );
                }
                Err(error) => {
                    assert_eq!(
                        format!("{error:#}"),
                        format!("{error_prefix}: {filename}")
                    );
                }
            }
        }
    }

    // Confirm that `SchemaVersion::load_from_directory()` rejects a directory
    // with no appropriately-named files.
    #[tokio::test]
    async fn test_reject_no_up_sql_files() {
        for filenames in [
            &[] as &[&str],
            &["README.md"],
            &["foo.sql", "bar.sql"],
            &["up1sql", "up2sql"],
        ] {
            let tempdir = Utf8TempDir::new().unwrap();
            for filename in filenames {
                _ = tokio::fs::File::create(tempdir.path().join(filename))
                    .await
                    .unwrap();
            }

            let maybe_schema = SchemaVersion::load_from_directory(
                SemverVersion::new(12, 0, 0),
                tempdir.path(),
            );
            match maybe_schema {
                Ok(upgrade) => {
                    panic!(
                        "unexpected success on {filenames:?} \
                         (produced {upgrade:?})"
                    );
                }
                Err(error) => {
                    assert_eq!(
                        format!("{error:#}"),
                        "no `up*.sql` files found"
                    );
                }
            }
        }
    }

    // Confirm that `SchemaVersion::load_from_directory()` rejects collections
    // of `up*.sql` files with individually-valid names but that do not pass the
    // rules of the entire collection.
    #[tokio::test]
    async fn test_reject_invalid_up_sql_collections() {
        for invalid_filenames in [
            &["up.sql", "up1.sql"] as &[&str],
            &["up1.sql", "up01.sql"],
            &["up1.sql", "up3.sql"],
            &["up1.sql", "up2.sql", "up3.sql", "up02.sql"],
        ] {
            let tempdir = Utf8TempDir::new().unwrap();
            for filename in invalid_filenames {
                _ = tokio::fs::File::create(tempdir.path().join(filename))
                    .await
                    .unwrap();
            }

            let maybe_schema = SchemaVersion::load_from_directory(
                SemverVersion::new(12, 0, 0),
                tempdir.path(),
            );
            match maybe_schema {
                Ok(upgrade) => {
                    panic!(
                        "unexpected success on {invalid_filenames:?} \
                         (produced {upgrade:?})"
                    );
                }
                Err(error) => {
                    let message = format!("{error:#}");
                    assert!(
                        message.starts_with("invalid `up*.sql` sequence: "),
                        "message did not start with expected prefix: \
                         {message:?}"
                    );
                }
            }
        }
    }

    // Confirm that `SchemaVersion::load_from_directory()` accepts legal
    // collections of `up*.sql` filenames.
    #[tokio::test]
    async fn test_allows_valid_up_sql_collections() {
        for filenames in [
            &["up.sql"] as &[&str],
            &["up1.sql", "up2.sql"],
            &[
                "up01.sql", "up02.sql", "up03.sql", "up04.sql", "up05.sql",
                "up06.sql", "up07.sql", "up08.sql", "up09.sql", "up10.sql",
                "up11.sql",
            ],
            &["up00001.sql", "up00002.sql", "up00003.sql"],
        ] {
            let tempdir = Utf8TempDir::new().unwrap();
            for filename in filenames {
                _ = tokio::fs::File::create(tempdir.path().join(filename))
                    .await
                    .unwrap();
            }

            let maybe_schema = SchemaVersion::load_from_directory(
                SemverVersion::new(12, 0, 0),
                tempdir.path(),
            );
            match maybe_schema {
                Ok(_) => (),
                Err(message) => {
                    panic!("unexpected failure on {filenames:?}: {message:?}");
                }
            }
        }
    }
}
