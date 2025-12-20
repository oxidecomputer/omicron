// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rules used for determining what debug data to collect

use super::FileLister;
use super::Filename;
use anyhow::anyhow;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use chrono::DateTime;
use chrono::Utc;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use std::sync::LazyLock;

/// Describes a source of debug data
///
/// In practice, this corresponds to either:
///
/// * the root filesystem of an illumos **zone**
/// * a **cores** (also called **crash**) dataset where user process core dumps
///   and kernel crash dumps are initially stored
#[derive(Clone)]
pub(crate) struct Source {
    pub(crate) input_prefix: Utf8PathBuf,
    pub(crate) output_prefix: Utf8PathBuf,
}

/// Describes debug data to be archived from within some `Source`.
///
/// Rules specify a path within the source where the files are found (e.g.,
/// "var/svc/log") and a pattern for specifying files within that directory that
/// should be covered by the rule (e.g., "*.log").  The rule is applied across
/// several sources (in this case: illumos zones).  A rule might cover "all the
/// files in a given cores dataset" or "the rotated SMF log files for a given
/// zone".
///
/// It may be easiest to understand this by example.  See [`ALL_RULES`] for all
/// of the rules.
///
/// There are basically two kinds of rules:
///
/// * **Zone** rules are applied to root filesystems of illumos zones,
///   including the global zone and non-global zones.  These have scope
///   `RuleScope::ZoneMutable` or `RuleScope::ZoneAlways`.  These describe how
///   to find the zone's log files.
///
/// * **Cores** rules are applied to cores datasets (also known as "crash
///   datasets"), which contain kernel crash dumps and process core dumps.
pub(crate) struct Rule {
    /// human-readable description of the rule
    pub label: &'static str,
    /// identifies what types of sources this rule is supposed to be applied to
    pub rule_scope: RuleScope,
    /// identifies the path to a directory within a source's input directory
    /// that contains the data described by this rule
    pub directory: Utf8PathBuf,
    /// describes which files within `directory` are identified by this rule
    glob_pattern: glob::Pattern, // XXX-dap consider regex?
    /// configures whether the original files associated with this rule should
    /// be deleted once they're archived
    ///
    /// For example, rotated log files are deleted when archived.  Live log
    /// files are not.
    pub delete_original: bool,
    /// Describes how to construct the name of a file that's being archived
    // XXX-dap these are all static -- maybe use &'static dyn NamingRule?
    pub naming: Box<dyn NamingRule + Send + Sync>,
}

impl Rule {
    /// Returns true if this rule specifies that the given `filename` should be
    /// archived
    pub(crate) fn include_file(&self, filename: &Filename) -> bool {
        self.glob_pattern.matches(filename.as_ref())
    }
}

impl IdOrdItem for Rule {
    type Key<'a> = &'static str;
    fn key(&self) -> Self::Key<'_> {
        self.label
    }
    id_upcast!();
}

/// Describes what Sources a rule can be applied to
pub(crate) enum RuleScope {
    /// this rule applies to all cores directories
    CoresDirectory,
    /// this rule applies to zone roots for "everything" collections, but not
    /// "immutable" ones
    ZoneMutable,
    /// this rule applies to zone roots always, regardless of whether or not
    /// we're collecting immutable data only
    ZoneAlways,
}

/// path within a zone's root filesystem to its SMF logs
static VAR_SVC_LOG: &str = "var/svc/log";
/// path within a zone's root filesystem to its syslog
static VAR_ADM: &str = "var/adm";

/// List of all archive rules in the system
///
/// **NOTE:** If you change these rules, you may also need to update the testing
/// data used by the test suite.  The test suite uses path names from real
/// systems to test various properties about these rules:
///
/// * that all files in the test data are covered by exactly one rule
///   (rules should not specify overlapping files)
/// * that all rules are covered by the test data
pub(crate) static ALL_RULES: LazyLock<IdOrdMap<Rule>> = LazyLock::new(|| {
    let rules = [
        Rule {
            label: "process core files and kernel crash dumps",
            rule_scope: RuleScope::CoresDirectory,
            directory: ".".parse().unwrap(),
            glob_pattern: "*".parse().unwrap(),
            delete_original: true,
            naming: Box::new(NameIdentity),
        },
        Rule {
            label: "live SMF log files",
            rule_scope: RuleScope::ZoneMutable,
            directory: VAR_SVC_LOG.parse().unwrap(),
            glob_pattern: "*.log".parse().unwrap(),
            delete_original: false,
            naming: Box::new(NameLiveLogFile),
        },
        Rule {
            label: "live syslog files",
            rule_scope: RuleScope::ZoneMutable,
            directory: VAR_ADM.parse().unwrap(),
            glob_pattern: "messages".parse().unwrap(),
            delete_original: false,
            naming: Box::new(NameLiveLogFile),
        },
        Rule {
            label: "rotated SMF log files",
            rule_scope: RuleScope::ZoneAlways,
            directory: VAR_SVC_LOG.parse().unwrap(),
            glob_pattern: "*.log.*".parse().unwrap(), // XXX-dap digits
            delete_original: true,
            naming: Box::new(NameRotatedLogFile),
        },
        Rule {
            label: "rotated syslog files",
            rule_scope: RuleScope::ZoneAlways,
            directory: VAR_ADM.parse().unwrap(),
            glob_pattern: "messages.*".parse().unwrap(), // XXX-dap digits
            delete_original: true,
            naming: Box::new(NameRotatedLogFile),
        },
    ];

    // We could do this more concisely with a `collect()` or `IdOrdMap::from`,
    // but those would silently discard duplicates.  We want to detect these and
    // provide a clear error message.
    let mut rv = IdOrdMap::new();
    for rule in rules {
        let label = rule.label;
        if let Err(_) = rv.insert_unique(rule) {
            panic!("found multiple rules with the same label: {:?}", label);
        }
    }

    rv
});

/// Describes how to construct an archived file's final name based on its
/// original name and mtime
///
/// `archived_file_name` is provided with `lister`, which can be used to
/// determine if the desired output filename already exists and choose another
/// name.  **If the name of an existing file is returned, that file will be
/// overwritten by the file that's being archived.**
pub(crate) trait NamingRule {
    fn archived_file_name(
        &self,
        source_file_name: &Filename,
        source_file_mtime: Option<DateTime<Utc>>,
        lister: &dyn FileLister,
        output_directory: &Utf8Path,
    ) -> Result<Filename, anyhow::Error>;
}

pub(crate) const MAX_COLLIDING_FILENAMES: u16 = 30;

/// `NamingRule` that's used for rotated log files
///
/// These files are typically named `foo.0`, `foo.1`, etc.  The integer at the
/// end is provided by logadm(8) and has no meaning for us.  This implementation
/// replaces that integer with the file's `mtime` as a Unix timestamp.  When
/// that would collide with an existing filename, it increments the `mtime`
/// until it gets a unique value (up to `MAX_COLLIDING_FILENAMES` tries).  This
/// behavior is historical and should potentially be revisited.
pub(crate) struct NameRotatedLogFile;
impl NamingRule for NameRotatedLogFile {
    fn archived_file_name(
        &self,
        source_file_name: &Filename,
        source_file_mtime: Option<DateTime<Utc>>,
        lister: &dyn FileLister,
        output_directory: &Utf8Path,
    ) -> Result<Filename, anyhow::Error> {
        let filename_base = match source_file_name.as_ref().rsplit_once('.') {
            Some((base, _extension)) => base,
            None => source_file_name.as_ref(),
        };

        let mtime_as_seconds =
            source_file_mtime.unwrap_or_else(|| Utc::now()).timestamp();
        for i in 0..MAX_COLLIDING_FILENAMES {
            let rv =
                format!("{filename_base}.{}", mtime_as_seconds + i64::from(i));
            let dest = output_directory.join(&rv);
            if !lister.file_exists(&dest)? {
                // unwrap(): we started with a valid `Filename` and did not add
                // any slashes here.
                return Ok(Filename::try_from(rv).unwrap());
            }
        }

        Err(anyhow!(
            "failed to choose archive file name for file {source_file_name:?} \
             because there are too many files with colliding names (at least \
             {MAX_COLLIDING_FILENAMES})"
        ))
    }
}

/// `NamingRule` that's used for live log files
///
/// These files can have an arbitrary name `foo`.  (SMF log files have a `.log`
/// suffix, but syslog files do not.)  For historical reasons, this uses the
/// same implementation as `NameRotatedLogFile`.  This behavior should probably
/// be revisited.
struct NameLiveLogFile;
impl NamingRule for NameLiveLogFile {
    fn archived_file_name(
        &self,
        source_file_name: &Filename,
        source_file_mtime: Option<DateTime<Utc>>,
        lister: &dyn FileLister,
        output_directory: &Utf8Path,
    ) -> Result<Filename, anyhow::Error> {
        // XXX-dap follow-up
        NameRotatedLogFile.archived_file_name(
            source_file_name,
            source_file_mtime,
            lister,
            output_directory,
        )
    }
}

/// `NamingRule` that's used for files whose names get preserved across archival
///
/// This includes kernel crash dumps, process core dumps, etc.  This behavior is
/// historical.  It does not account for cases where the output filename already
/// exists, which means those files may be overwritten.
// XXX-dap follow up
struct NameIdentity;
impl NamingRule for NameIdentity {
    fn archived_file_name(
        &self,
        source_file_name: &Filename,
        _source_file_mtime: Option<DateTime<Utc>>,
        _lister: &dyn FileLister,
        _output_directory: &Utf8Path,
    ) -> Result<Filename, anyhow::Error> {
        Ok(source_file_name.clone())
    }
}
