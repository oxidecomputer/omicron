// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Runtime configuration for the fault management subsystem.
//!
//! The default configuration is defined by [`FmConfig::default`]. This
//! configuration may be persistently overridden by creating rows in the
//! `fm_config` database table. Overrides are versioned: changes are made by
//! inserting a new config with the next version number, rather than by updating
//! the current one in place. [`FmConfigView::source`] indicates whether a
//! configuration came from the current default or a database override.
//!
//! Configuration values are represented by two types: a [`FmConfigParam`]
//! holds unvalidated values (a new override requested by a caller, or a row
//! read back from the database), while a [`FmConfig`] has been validated
//! against the invariants described in [`FmConfigParam`]'s documentation.

use super::display::const_max_len;
use std::fmt;
use std::num::NonZeroU32;

use chrono::{DateTime, Utc};
use omicron_common::api::external::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An unvalidated FM configuration override at a particular version, such as
/// a request to insert a new FM config version.
///
/// An insert succeeds only if `version` is exactly one greater than the
/// current latest override version (or 1, if no overrides exist), and if the
/// configuration values pass validation. The configuration values in this
/// type are unvalidated, and their invariants may not hold:
///
/// - [`Self::sitrep_limit`] must be at least [`FmConfig::MIN_SITREP_LIMIT`],
///   and no more than [`FmConfig::MAX_LIMIT`],
/// - [`Self::history_pruning_threshold`] must be at least
///   [`FmConfig::MIN_HISTORY_PRUNING_THRESHOLD`], and no more than
///   [`FmConfig::MAX_LIMIT`],
/// - [`Self::history_pruning_threshold`] must be strictly less than
///   [`Self::sitrep_limit`],
/// - [`Self::comment`] must be non-empty.
///
/// The configuration values are validated by converting this type into a
/// [`FmConfig`] via `TryFrom`. Whether `version` is actually the next version
/// can only be determined by the database at insert time, so it is not
/// checked by that conversion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FmConfigParam {
    /// The version of the configuration.
    pub version: NonZeroU32,

    /// A comment describing why the default configuration was overridden.
    ///
    /// A comment is mandatory when overriding the default configuration, so
    /// this must be non-empty.
    pub comment: String,

    /// BREAK GLASS TO COMPLETELY DISABLE FM ANALYSIS
    pub analysis_enabled: bool,

    /// The maximum number of sitreps to keep in the database.
    ///
    /// If the number of sitreps exceeds this limit, the FM analysis background
    /// task will not produce a new sitrep until old ones are deleted.
    ///
    /// This limit applies to both committed sitreps in the history *and*
    /// orphaned sitreps left behind when multiple Nexuses race to commit a
    /// sitrep.
    pub sitrep_limit: u32,

    /// The maximum number of sitreps committed to the `fm_sitrep_history`
    /// table. If the number of sitreps exceeds this threshold, the
    /// `fm_sitrep_history_pruner` background task will remove the oldest
    /// entries from the history.
    pub history_pruning_threshold: u32,
}

/// A view of the current fault management configuration.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct FmConfigView {
    pub config: FmConfig,
    pub source: FmConfigSource,
}

impl FmConfigView {
    /// Returns a multi-line displayer for this view, with each line indented
    /// by `indent` spaces.
    pub fn display_multiline(&self, indent: usize) -> impl fmt::Display + '_ {
        struct DisplayView<'a> {
            view: &'a FmConfigView,
            indent: usize,
        }

        impl fmt::Display for DisplayView<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let DisplayView {
                    view: FmConfigView { config, source },
                    indent,
                } = self;
                write!(f, "{}", source.display_multiline(*indent))?;
                write!(f, "{}", config.display_multiline(*indent))
            }
        }

        DisplayView { view: self, indent }
    }
}

impl fmt::Display for FmConfigView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.display_multiline(0).fmt(f)
    }
}

/// Where a [`FmConfigView`]'s configuration came from.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FmConfigSource {
    #[default]
    Default,

    /// The fault management configuration was overridden.
    Override {
        /// The version of the override.
        version: NonZeroU32,
        /// The time at which this override was created.
        time_modified: DateTime<Utc>,
        /// A comment describing why the default configuration was overridden.
        comment: String,
    },
}

impl FmConfigSource {
    /// Returns a multi-line displayer for this source, with each line
    /// indented by `indent` spaces.
    ///
    /// The [`fmt::Display`] implementation for this type formats the source
    /// on a single line instead.
    pub fn display_multiline(&self, indent: usize) -> impl fmt::Display + '_ {
        struct DisplaySource<'a> {
            source: &'a FmConfigSource,
            indent: usize,
        }

        impl fmt::Display for DisplaySource<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let DisplaySource { source, indent } = self;

                match source {
                    FmConfigSource::Default => {
                        writeln!(f, "{:>indent$}{SOURCE:<WIDTH$} default", "")
                    }
                    FmConfigSource::Override {
                        version,
                        time_modified,
                        comment,
                    } => {
                        writeln!(
                            f,
                            "{:>indent$}{SOURCE:<WIDTH$} override",
                            ""
                        )?;
                        writeln!(
                            f,
                            "{:>indent$}{VERSION:<WIDTH$} {version}",
                            ""
                        )?;
                        writeln!(
                            f,
                            "{:>indent$}{TIME_MODIFIED:<WIDTH$} {}",
                            "",
                            humantime::format_rfc3339_millis(
                                (*time_modified).into()
                            )
                        )?;
                        writeln!(f, "{:>indent$}{COMMENT:<WIDTH$}", "")?;
                        // If the comment is multi-line, write it out
                        // line-by-line so that we can indent them.
                        let comment_indent = indent + 2;
                        for line in comment.lines() {
                            writeln!(f, "{:>comment_indent$}{line}", "")?;
                        }
                        Ok(())
                    }
                }
            }
        }

        DisplaySource { source: self, indent }
    }
}

impl fmt::Display for FmConfigSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => write!(f, "default"),
            Self::Override { version, .. } => {
                write!(f, "override (v{version})")
            }
        }
    }
}

/// A validated fault management configuration.
///
/// Note that deserializing a `FmConfig` (e.g., from a serialized
/// [`FmConfigView`]) does *not* re-validate the fields; deserialization is
/// intended for round-tripping values that were validated when they were
/// first constructed.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct FmConfig {
    /// BREAK GLASS TO COMPLETELY DISABLE FM ANALYSIS
    pub analysis_enabled: bool,

    /// The maximum number of sitreps to keep in the database.
    ///
    /// If the number of sitreps exceeds this limit, the FM analysis background
    /// task will not produce a new sitrep until old ones are deleted.
    ///
    /// This limit applies to both committed sitreps in the history *and*
    /// orphaned sitreps left behind when multiple Nexuses race to commit a
    /// sitrep.
    pub sitrep_limit: NonZeroU32,

    /// The maximum number of sitreps committed to the `fm_sitrep_history`
    /// table. If the number of sitreps exceeds this threshold, the
    /// `fm_sitrep_history_pruner` background task will remove the oldest
    /// entries from the history.
    pub history_pruning_threshold: NonZeroU32,
}

impl FmConfig {
    /// The minimum permitted value of [`Self::sitrep_limit`].
    ///
    /// This must be greater than [`Self::MIN_HISTORY_PRUNING_THRESHOLD`], to
    /// allow for new sitreps to be committed when the history has yet to be
    /// pruned to the limit.
    ///
    /// **Note:** This value should be kept in sync with the minimum value
    /// enforced by the CHECK constraint on the `fm_config` table.
    pub const MIN_SITREP_LIMIT: NonZeroU32 = NonZeroU32::new(5).unwrap();

    /// The default value of [`Self::sitrep_limit`], used when there is no
    /// config override set.
    pub const DEFAULT_SITREP_LIMIT: NonZeroU32 = NonZeroU32::new(2500).unwrap();

    /// The minimum permitted value of [`Self::history_pruning_threshold`].
    ///
    /// The current sitrep must always be retained, so at least two sitreps
    /// must exist before any may be deleted.
    ///
    /// **Note:** This value should be kept in sync with the minimum value
    /// enforced by the CHECK constraint on the `fm_config` table.
    pub const MIN_HISTORY_PRUNING_THRESHOLD: NonZeroU32 =
        NonZeroU32::new(2).unwrap();

    /// The default value of [`Self::history_pruning_threshold`], used when
    /// there is no config override set.
    pub const DEFAULT_HISTORY_PRUNING_THRESHOLD: NonZeroU32 =
        NonZeroU32::new(2000).unwrap();

    /// Maximum value for [`Self::sitrep_limit`] and
    /// [`Self::history_pruning_threshold`].
    ///
    /// Because the implementation of checking whether a database table has
    /// reached a limit requires scanning up to `limit` rows from that table,
    /// checking against the limit will (perhaps surprisingly) become
    /// increasingly costly as the limit increases. Therefore, we must enforce
    /// an upper bound on how big the limits can be so we don't end up scanning
    /// hundreds of thousands of rows all the time. It is the author of this
    /// comment's opinion that it really seems like the daabase *should* be able
    /// to just sort of cache the number of rows in the table in a way that
    /// makes those queries O(1), but maybe that's hard for database reasons I
    /// don't understand. Anyway, 5000 is the value that the reconfigurator uses
    /// for its (hard-coded) blueprint limit, so using it as our limit on our
    /// limits (haha) means that in our worst-case config, we are just doing a
    /// scan that's as big as what the reconfigurator does all the time, which
    /// seems fine.
    ///
    /// **Note:** This value should be kept in sync with the maximum values
    /// enforced by the CHECK constraint on the `fm_config` table.
    pub const MAX_LIMIT: NonZeroU32 = NonZeroU32::new(5000).unwrap();

    /// Returns a multi-line displayer for this config, with each line
    /// indented by `indent` spaces.
    pub fn display_multiline(&self, indent: usize) -> impl fmt::Display + '_ {
        struct DisplayConfig<'a> {
            config: &'a FmConfig,
            indent: usize,
        }

        impl fmt::Display for DisplayConfig<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let DisplayConfig {
                    config:
                        FmConfig {
                            analysis_enabled,
                            sitrep_limit,
                            history_pruning_threshold,
                        },
                    indent,
                } = self;

                writeln!(
                    f,
                    "{:>indent$}{ANALYSIS_ENABLED:<WIDTH$} {analysis_enabled}",
                    ""
                )?;
                writeln!(
                    f,
                    "{:>indent$}{SITREP_LIMIT:<WIDTH$} {sitrep_limit}",
                    ""
                )?;
                writeln!(
                    f,
                    "{:>indent$}{HISTORY_PRUNING_THRESHOLD:<WIDTH$} \
                     {history_pruning_threshold}",
                    ""
                )
            }
        }

        DisplayConfig { config: self, indent }
    }
}

impl Default for FmConfig {
    fn default() -> Self {
        Self {
            analysis_enabled: true,
            sitrep_limit: Self::DEFAULT_SITREP_LIMIT,
            history_pruning_threshold: Self::DEFAULT_HISTORY_PRUNING_THRESHOLD,
        }
    }
}

impl TryFrom<&'_ FmConfigParam> for FmConfig {
    type Error = Error;

    fn try_from(value: &'_ FmConfigParam) -> Result<Self, Self::Error> {
        // This exhaustive destructuring exists to trigger compilation errors
        // when the `FmConfigParam` type changes, so that people are prompted
        // to update this validation (and the database insert query in
        // `nexus-db-queries`). If you get a compiler error here, you probably
        // added or removed a field, and will need to adjust both accordingly.
        //
        // `version` is not used by this conversion: it is not part of the
        // config itself, and its type already ensures it is nonzero.
        let &FmConfigParam {
            version: _,
            ref comment,
            analysis_enabled,
            sitrep_limit,
            history_pruning_threshold,
        } = value;

        // Technically, the comment is not actually part of the `FmConfig`
        // struct, but this conversion is where we validate everything else, so
        // check that the comment is valid here.
        if comment.is_empty() {
            return Err(Error::invalid_value(
                "comment",
                "a non-empty comment is required when overriding the fault \
                 management config",
            ));
        }
        if comment.trim().is_empty() {
            return Err(Error::invalid_value(
                "comment",
                "you sneaky bastard! you thought you could trick me by \
                 leaving a comment that was entirely whitespace, but I already \
                 thought of that. you're going to have to at least type the \
                 letter 'a' or a period or something.",
            ));
        }

        fn check_limit(
            value: u32,
            field: &str,
            min: NonZeroU32,
            max: NonZeroU32,
        ) -> Result<NonZeroU32, Error> {
            let value = NonZeroU32::new(value).ok_or_else(|| {
                Error::invalid_value(field, format!("{field} must be nonzero"))
            })?;

            if value < min {
                return Err(Error::invalid_value(
                    field,
                    format!("{field} must be at least {min} (got {value})"),
                ));
            }

            if value > max {
                return Err(Error::invalid_value(
                    field,
                    format!(
                        "{field} must be less than or equal to {max} (got \
                         {value})",
                    ),
                ));
            }
            Ok(value)
        }

        let sitrep_limit = check_limit(
            sitrep_limit,
            "sitrep_limit",
            Self::MIN_SITREP_LIMIT,
            Self::MAX_LIMIT,
        )?;
        let history_pruning_threshold = check_limit(
            history_pruning_threshold,
            "history_pruning_threshold",
            Self::MIN_HISTORY_PRUNING_THRESHOLD,
            Self::MAX_LIMIT,
        )?;

        if history_pruning_threshold >= sitrep_limit {
            return Err(Error::invalid_value(
                "history_pruning_threshold",
                format!(
                    "sitrep history pruning threshold ({history_pruning_threshold}) \
                     must be less than the total sitrep limit \
                    ({sitrep_limit})",
                ),
            ));
        }
        Ok(Self { analysis_enabled, sitrep_limit, history_pruning_threshold })
    }
}

const SOURCE: &str = "source:";
const VERSION: &str = "  version:";
const TIME_MODIFIED: &str = "  modified at:";
const COMMENT: &str = "  comment:";
const ANALYSIS_ENABLED: &str = "analysis enabled:";
const SITREP_LIMIT: &str = "sitrep limit:";
const HISTORY_PRUNING_THRESHOLD: &str = "sitrep history pruning threshold:";
const WIDTH: usize = const_max_len(&[
    SOURCE,
    VERSION,
    TIME_MODIFIED,
    COMMENT,
    ANALYSIS_ENABLED,
    SITREP_LIMIT,
    HISTORY_PRUNING_THRESHOLD,
]);

#[cfg(test)]
mod tests {
    use super::*;

    const V1: NonZeroU32 = NonZeroU32::new(1).unwrap();

    fn param(
        sitrep_limit: u32,
        history_pruning_threshold: u32,
    ) -> FmConfigParam {
        FmConfigParam {
            version: V1,
            comment: "test config".to_string(),
            analysis_enabled: true,
            sitrep_limit,
            history_pruning_threshold,
        }
    }

    #[test]
    fn test_sitrep_limit_nonzero() {
        let err = FmConfig::try_from(&param(0, 2)).unwrap_err();
        assert!(
            err.to_string().contains("sitrep_limit must be nonzero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_min_sitrep_limit() {
        let err = FmConfig::try_from(&param(2, 2)).unwrap_err();
        assert!(
            err.to_string().contains("sitrep_limit must be at least"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_nonzero_history_pruning_threshold() {
        let err = FmConfig::try_from(&param(100, 0)).unwrap_err();
        assert!(
            err.to_string()
                .contains("history_pruning_threshold must be nonzero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_min_history_pruning_threshold() {
        let err = FmConfig::try_from(&param(100, 1)).unwrap_err();
        assert!(
            err.to_string()
                .contains("history_pruning_threshold must be at least"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_history_pruning_threshold_must_be_less_than_sitrep_limit() {
        for threshold in [100, 101] {
            let err = FmConfig::try_from(&param(100, threshold)).unwrap_err();
            assert!(
                err.to_string()
                    .contains("must be less than the total sitrep limit"),
                "unexpected error: {err}"
            );
        }
    }
}
