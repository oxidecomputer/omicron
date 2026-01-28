// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for working with the testing data used in the test suite

use super::filesystem::FileLister;
use super::filesystem::Filename;
use super::planning::ArchiveKind;
use super::planning::ArchivePlan;
use super::planning::ArchivePlanner;
use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use chrono::DateTime;
use chrono::Utc;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use regex::Regex;
use slog::Logger;
use slog::debug;
use std::collections::BTreeSet;
use std::sync::LazyLock;
use std::sync::Mutex;
use strum::Display;
use strum::EnumDiscriminants;
use strum::EnumIter;
use strum::IntoDiscriminant;
use strum::IntoEnumIterator;

/// Loads the filenames in the test data
pub(crate) fn load_test_files() -> anyhow::Result<IdOrdMap<TestFile>> {
    load_test_data_paths()?
        .into_iter()
        .map(|path| {
            TestFileKind::try_from(path.as_ref())
                .context("may need to update load_test_files()?")
                .map(|kind| TestFile { path, kind })
        })
        .collect()
}

fn load_test_data_paths() -> anyhow::Result<BTreeSet<Utf8PathBuf>> {
    let path = "test-data/debug-files.txt";
    std::fs::read_to_string(&path)
        .with_context(|| format!("read {path:?}"))?
        .lines()
        .enumerate()
        .map(|(i, l)| (i, l.trim()))
        .filter(|(_i, l)| !l.is_empty() && !l.starts_with("#"))
        .map(|(i, l)| {
            Utf8PathBuf::try_from(l).map_err(|_err| {
                anyhow!("{path:?} line {}: non-UTF8 file path", i + 1)
            })
        })
        .collect()
}

/// Test that our test data includes all the kinds of things that we expect.
/// If you see this test failing, presumably you updated the test data and
/// you'll need to make sure it's still representative.
#[test]
fn test_test_data() {
    // Load the test data and determine what kind of file each one is.
    let files = load_test_files().unwrap();

    // Create a set of all the kinds of test files that we have not seen so
    // far.  We'll remove from this set as we find files of this kind.  Any
    // kinds left over at the end are missing from our test data.
    let mut all_kinds: BTreeSet<_> =
        TestFileKindDiscriminants::iter().collect();
    // We don't care about finding the "ignored" kind.
    all_kinds.remove(&TestFileKindDiscriminants::Ignored);
    for test_file in files {
        println!("{} {}", test_file.kind, test_file.path);
        all_kinds.remove(&test_file.kind.discriminant());
    }

    if !all_kinds.is_empty() {
        panic!("missing file in test data for kinds: {:?}", all_kinds);
    }
}

/// Plan an archive operation based on the testing data
pub(crate) fn test_archive<'a>(
    log: &Logger,
    test_files: &IdOrdMap<TestFile>,
    output_dir: &Utf8Path,
    what: ArchiveKind,
    lister: &'a TestLister,
) -> ArchivePlan<'a> {
    // Construct sources that correspond with the test data.
    let cores_datasets: BTreeSet<_> = test_files
        .iter()
        .filter_map(|test_file| test_file.kind.cores_directory())
        .collect();
    let zone_infos: BTreeSet<_> = test_files
        .iter()
        .filter_map(|test_file| test_file.kind.zone_info())
        .collect();

    // Plan an archival pass.
    let mut planner =
        ArchivePlanner::new_with_lister(log, what, output_dir, lister);

    for cores_dir in cores_datasets {
        debug!(log, "including cores directory"; "cores_dir" => %cores_dir);
        planner.include_cores_directory(cores_dir);
    }

    for (zone_name, zone_root) in zone_infos {
        debug!(
            log,
            "including zone";
            "zone_name" => zone_name,
            "zone_root" => %zone_root,
        );
        planner.include_zone(zone_name, zone_root);
    }

    planner.into_plan()
}

/// Describes one file path in the testing data
#[derive(Clone)]
pub(crate) struct TestFile {
    /// path to the file
    pub path: Utf8PathBuf,
    /// what kind of file we determined it to be, based on its path
    pub kind: TestFileKind,
}

impl IdOrdItem for TestFile {
    type Key<'a> = &'a Utf8Path;

    fn key(&self) -> Self::Key<'_> {
        &self.path
    }

    id_upcast!();
}

/// Describes what kind of file we're looking at and what source it's in
#[derive(Clone, Debug, Display, EnumIter, EnumDiscriminants)]
#[strum_discriminants(derive(EnumIter, Ord, PartialOrd))]
pub(crate) enum TestFileKind {
    ProcessCoreDump {
        cores_directory: String,
    },
    LogSmfRotated {
        zone_name: String,
        zone_root: String,
    },
    LogSmfLive {
        zone_name: String,
        zone_root: String,
    },
    LogSyslogRotated {
        zone_name: String,
        zone_root: String,
    },
    LogSyslogLive {
        zone_name: String,
        zone_root: String,
    },
    DebugDropbox {
        zone_name: String,
        zone_root: String,
    },
    GlobalLogSmfRotated,
    GlobalLogSmfLive,
    GlobalLogSyslogRotated,
    GlobalLogSyslogLive,
    /// files we don't especially care about, but are in the test data to
    /// ensure that they don't create a problem
    Ignored,
}

impl TestFileKind {
    /// Returns information about the cores directory this file is in, if any
    pub fn cores_directory(&self) -> Option<&Utf8Path> {
        match self {
            TestFileKind::ProcessCoreDump { cores_directory } => {
                Some(Utf8Path::new(cores_directory))
            }
            TestFileKind::LogSmfRotated { .. }
            | TestFileKind::LogSmfLive { .. }
            | TestFileKind::LogSyslogRotated { .. }
            | TestFileKind::LogSyslogLive { .. }
            | TestFileKind::DebugDropbox { .. }
            | TestFileKind::GlobalLogSmfRotated
            | TestFileKind::GlobalLogSmfLive
            | TestFileKind::GlobalLogSyslogRotated
            | TestFileKind::GlobalLogSyslogLive
            | TestFileKind::Ignored => None,
        }
    }

    /// Returns information about the zone this file is in, if any
    pub fn zone_info(&self) -> Option<(&str, &Utf8Path)> {
        match self {
            TestFileKind::ProcessCoreDump { .. } | TestFileKind::Ignored => {
                None
            }
            TestFileKind::LogSmfRotated { zone_name, zone_root }
            | TestFileKind::LogSmfLive { zone_name, zone_root }
            | TestFileKind::LogSyslogRotated { zone_name, zone_root }
            | TestFileKind::LogSyslogLive { zone_name, zone_root }
            | TestFileKind::DebugDropbox { zone_name, zone_root } => {
                Some((zone_name, Utf8Path::new(zone_root)))
            }
            TestFileKind::GlobalLogSmfRotated
            | TestFileKind::GlobalLogSmfLive
            | TestFileKind::GlobalLogSyslogRotated
            | TestFileKind::GlobalLogSyslogLive => {
                Some(("global", Utf8Path::new("/")))
            }
        }
    }
}

static RE_CORES_DATASET: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("^(/pool/int/[^/]+/crash)/[^/]+$").unwrap());

static RE_NONGLOBAL_ZONE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new("^/pool/ext/[^/]+/crypt/zone/([^/]+)/root").unwrap()
});

impl TryFrom<&Utf8Path> for TestFileKind {
    type Error = anyhow::Error;

    fn try_from(value: &Utf8Path) -> Result<Self, Self::Error> {
        let s = value.as_str();

        if let Some(c) = RE_CORES_DATASET.captures(s) {
            let (_, [cores_directory]) = c.extract();
            let cores_directory = cores_directory.to_owned();
            if s.ends_with("bounds") {
                Ok(TestFileKind::Ignored)
            } else if s.contains("/core.") {
                Ok(TestFileKind::ProcessCoreDump { cores_directory })
            } else {
                Err(anyhow!("unknown cores dataset test file kind"))
            }
        } else if let Some(c) = RE_NONGLOBAL_ZONE.captures(s) {
            let (zone_root, [zone_name]) = c.extract();
            let zone_root = zone_root.to_owned();
            let zone_name = zone_name.to_owned();
            if s.ends_with("/messages") {
                Ok(TestFileKind::LogSyslogLive { zone_name, zone_root })
            } else if s.contains("/messages.") {
                Ok(TestFileKind::LogSyslogRotated { zone_name, zone_root })
            } else if s.contains("/var/svc/log") {
                if s.ends_with(".log") {
                    Ok(TestFileKind::LogSmfLive { zone_name, zone_root })
                } else {
                    Ok(TestFileKind::LogSmfRotated { zone_name, zone_root })
                }
            } else if s.contains("/var/debug_drop") {
                // XXX-dap find constant for this
                Ok(TestFileKind::DebugDropbox { zone_name, zone_root })
            } else {
                Err(anyhow!("unknown non-global zone test file kind"))
            }
        } else {
            if s == "/var/adm/messages" {
                Ok(TestFileKind::GlobalLogSyslogLive)
            } else if s.starts_with("/var/adm") && s.contains("/messages.") {
                Ok(TestFileKind::GlobalLogSyslogRotated)
            } else if s.starts_with("/var/svc/log") {
                if s.ends_with(".log") {
                    Ok(TestFileKind::GlobalLogSmfLive)
                } else {
                    Ok(TestFileKind::GlobalLogSmfRotated)
                }
            } else {
                Err(anyhow!("unknown test file kind"))
            }
        }
    }
}

/// Implementation of `FileLister` built atop the testing data
pub(crate) struct TestLister<'a> {
    /// files in our fake filesystem
    files: BTreeSet<&'a Utf8Path>,
    /// describes the last path listed (used in tests to verify behavior)
    last_listed: Mutex<Option<Utf8PathBuf>>,
    /// inject errors when operating on this path
    injected_error: Option<&'a Utf8Path>,
}

impl<'a> TestLister<'a> {
    /// Returns a lister that reports no files
    pub fn empty() -> Self {
        Self::new::<_, &'a str>(std::iter::empty())
    }

    /// Returns a lister for the test data
    pub fn new_for_test_data(files: &'a IdOrdMap<TestFile>) -> Self {
        Self::new(files.iter().map(|test_file| test_file.path.as_path()))
    }

    /// Returns a lister backed by the specified files
    pub fn new<I, P>(files: I) -> Self
    where
        I: IntoIterator<Item = &'a P>,
        P: AsRef<Utf8Path> + ?Sized + 'a,
    {
        Self {
            files: files.into_iter().map(|p| p.as_ref()).collect(),
            last_listed: Mutex::new(None),
            injected_error: None,
        }
    }

    /// Configure this lister to inject errors when accessing this path
    ///
    /// Clears any previously injected error.
    pub fn inject_error(&mut self, fail_path: &'a Utf8Path) {
        self.injected_error = Some(fail_path);
    }

    pub fn last_listed(&self) -> Option<Utf8PathBuf> {
        self.last_listed.lock().unwrap().clone()
    }
}

impl FileLister for TestLister<'_> {
    fn list_files(
        &self,
        path: &Utf8Path,
    ) -> Vec<Result<Filename, anyhow::Error>> {
        // Keep track of the last path that was listed.
        *self.last_listed.lock().unwrap() = Some(path.to_owned());

        // Inject any errors we've been configured to inject.
        if let Some(fail_path) = self.injected_error {
            if path == fail_path {
                return vec![Err(anyhow!("injected error for {fail_path:?}"))];
            }
        }

        // Create a directory listing from the files in our test data.
        self.files
            .iter()
            .filter_map(|file_path| {
                let directory =
                    file_path.parent().expect("test file has a parent");
                (directory == path).then(|| {
                    let filename = file_path
                        .file_name()
                        .expect("test file has a filename");
                    Ok(Filename::try_from(filename.to_owned())
                        .expect("filename has no slashes"))
                })
            })
            .collect()
    }

    fn file_mtime(
        &self,
        path: &Utf8Path,
    ) -> Result<Option<DateTime<Utc>>, anyhow::Error> {
        if let Some(fail_path) = self.injected_error {
            if path == fail_path {
                bail!("injected error for {fail_path:?}");
            }
        }

        Ok(Some("2025-12-12T16:51:00-07:00".parse().unwrap()))
    }

    fn file_exists(&self, path: &Utf8Path) -> Result<bool, anyhow::Error> {
        Ok(self.files.contains(path))
    }
}
