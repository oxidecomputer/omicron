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

pub(crate) struct Rule {
    pub label: &'static str,
    pub rule_scope: RuleScope,
    pub directory: Utf8PathBuf,
    glob_pattern: glob::Pattern, // XXX-dap consider regex?
    pub delete_original: bool,
    // XXX-dap these are all static -- maybe use &'static dyn NamingRule?
    pub naming: Box<dyn NamingRule + Send + Sync>,
}

impl Rule {
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

static VAR_SVC_LOG: &str = "var/svc/log";
static VAR_ADM: &str = "var/adm";

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
pub(crate) struct NameRotatedLogFile;
impl NamingRule for NameRotatedLogFile {
    fn archived_file_name(
        &self,
        source_file_name: &Filename,
        source_file_mtime: Option<DateTime<Utc>>,
        lister: &dyn FileLister,
        output_directory: &Utf8Path,
    ) -> Result<Filename, anyhow::Error> {
        // XXX-dap TODO-doc
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

        // XXX-dap better message
        Err(anyhow!("too many files with colliding names"))
    }
}

struct NameLiveLogFile;
impl NamingRule for NameLiveLogFile {
    fn archived_file_name(
        &self,
        source_file_name: &Filename,
        source_file_mtime: Option<DateTime<Utc>>,
        lister: &dyn FileLister,
        output_directory: &Utf8Path,
    ) -> Result<Filename, anyhow::Error> {
        // XXX-dap should work better
        NameRotatedLogFile.archived_file_name(
            source_file_name,
            source_file_mtime,
            lister,
            output_directory,
        )
    }
}

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

#[cfg(test)]
mod test {}
