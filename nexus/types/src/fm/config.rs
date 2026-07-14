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

use super::const_max_len;
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
/// - `sitrep_limit` must be at least [`FmConfig::MIN_SITREP_LIMIT`],
/// - `sitrep_deletion_threshold` must be at least
///   [`FmConfig::MIN_SITREP_DELETION_THRESHOLD`],
/// - `sitrep_deletion_threshold` must be strictly less than `sitrep_limit`.
///
/// The configuration values are validated by converting this type into a
/// [`FmConfig`] via `TryFrom`. Whether `version` is actually the next version
/// can only be determined by the database at insert time, so it is not
/// checked by that conversion.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct FmConfigParam {
    /// The version of the configuration.
    pub version: NonZeroU32,

    /// The maximum number of historical sitreps to keep in the database.
    ///
    /// If the number of sitreps in the history exceeds this limit, the FM
    /// analysis background task will not produce a new sitrep until old ones
    /// are deleted.
    pub sitrep_limit: u32,

    /// The number of sitreps in the history at which the sitrep GC background
    /// task will begin deleting the oldest sitreps.
    pub sitrep_deletion_threshold: u32,
}

/// A view of the current fault management configuration.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    JsonSchema,
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
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FmConfigSource {
    #[default]
    Default,

    Override {
        /// The version of the override.
        version: NonZeroU32,
        /// The time at which this override was inserted.
        time_modified: DateTime<Utc>,
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
                    FmConfigSource::Override { version, time_modified } => {
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
                        )
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
    /// The maximum number of historical sitreps to keep in the database.
    ///
    /// If the number of sitreps in the history exceeds this limit, the FM
    /// analysis background task will not produce a new sitrep until old ones
    /// are deleted.
    pub sitrep_limit: NonZeroU32,

    /// The number of sitreps in the history at which the sitrep GC background
    /// task will begin deleting the oldest sitreps.
    pub sitrep_deletion_threshold: NonZeroU32,
}

impl FmConfig {
    /// The minimum permitted value of `sitrep_limit`.
    ///
    /// This is one greater than [`Self::MIN_SITREP_DELETION_THRESHOLD`], as
    /// the deletion threshold must be less than the limit.
    pub const MIN_SITREP_LIMIT: NonZeroU32 = NonZeroU32::new(3).unwrap();

    /// The minimum permitted value of `sitrep_deletion_threshold`.
    ///
    /// The current sitrep must always be retained, so at least two sitreps
    /// must exist before any may be deleted.
    pub const MIN_SITREP_DELETION_THRESHOLD: NonZeroU32 =
        NonZeroU32::new(2).unwrap();

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
                    config: FmConfig { sitrep_limit, sitrep_deletion_threshold },
                    indent,
                } = self;

                writeln!(
                    f,
                    "{:>indent$}{SITREP_LIMIT:<WIDTH$} {sitrep_limit}",
                    ""
                )?;
                writeln!(
                    f,
                    "{:>indent$}{SITREP_DELETION_THRESHOLD:<WIDTH$} \
                     {sitrep_deletion_threshold}",
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
            sitrep_limit: NonZeroU32::new(1000).unwrap(),
            sitrep_deletion_threshold: NonZeroU32::new(900).unwrap(),
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
            sitrep_limit,
            sitrep_deletion_threshold,
        } = value;

        fn check_minimum(
            value: u32,
            field: &str,
            min: NonZeroU32,
        ) -> Result<NonZeroU32, Error> {
            let value = NonZeroU32::new(value).ok_or_else(|| {
                Error::invalid_value(field, format!("{field} must be nonzero"))
            })?;
            if value < min {
                return Err(Error::invalid_value(
                    field,
                    format!("{field} must be at least {min} (got {value})",),
                ));
            }
            Ok(value)
        }

        let sitrep_limit = check_minimum(
            sitrep_limit,
            "sitrep_limit",
            Self::MIN_SITREP_LIMIT,
        )?;
        let sitrep_deletion_threshold = check_minimum(
            sitrep_deletion_threshold,
            "sitrep_deletion_threshold",
            Self::MIN_SITREP_DELETION_THRESHOLD,
        )?;

        if sitrep_deletion_threshold >= sitrep_limit {
            return Err(Error::invalid_value(
                "sitrep_deletion_threshold",
                format!(
                    "sitrep deletion threshold ({sitrep_deletion_threshold}) \
                     must be less than the sitrep limit ({sitrep_limit})",
                ),
            ));
        }
        Ok(Self { sitrep_limit, sitrep_deletion_threshold })
    }
}

const SOURCE: &str = "source:";
const VERSION: &str = "  version:";
const TIME_MODIFIED: &str = "  modified at:";
const SITREP_LIMIT: &str = "sitrep history limit:";
const SITREP_DELETION_THRESHOLD: &str = "sitrep deletion threshold:";
const WIDTH: usize = const_max_len(&[
    SOURCE,
    VERSION,
    TIME_MODIFIED,
    SITREP_LIMIT,
    SITREP_DELETION_THRESHOLD,
]);

#[cfg(test)]
mod tests {
    use super::*;

    const V1: NonZeroU32 = NonZeroU32::new(1).unwrap();

    #[test]
    fn test_sitrep_limit_nonzero() {
        let err = FmConfig::try_from(&FmConfigParam {
            version: V1,
            sitrep_limit: 0,
            sitrep_deletion_threshold: 2,
        })
        .unwrap_err();
        assert!(
            err.to_string().contains("sitrep_limit must be nonzero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_min_sitrep_limit() {
        let err = FmConfig::try_from(&FmConfigParam {
            version: V1,
            sitrep_limit: 2,
            sitrep_deletion_threshold: 2,
        })
        .unwrap_err();
        assert!(
            err.to_string().contains("sitrep_limit must be at least"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_nonzero_sitrep_deletion_threshold() {
        let err = FmConfig::try_from(&FmConfigParam {
            version: V1,
            sitrep_limit: 100,
            sitrep_deletion_threshold: 0,
        })
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("sitrep_deletion_threshold must be nonzero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_min_sitrep_deletion_threshold() {
        let err = FmConfig::try_from(&FmConfigParam {
            version: V1,
            sitrep_limit: 100,
            sitrep_deletion_threshold: 1,
        })
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("sitrep_deletion_threshold must be at least"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_sitrep_deletion_threshold_must_be_less_than_sitrep_limit() {
        for threshold in [100, 101] {
            let err = FmConfig::try_from(&FmConfigParam {
                version: V1,
                sitrep_limit: 100,
                sitrep_deletion_threshold: threshold,
            })
            .unwrap_err();
            assert!(
                err.to_string().contains("must be less than the sitrep limit"),
                "unexpected error: {err}"
            );
        }
    }
}
