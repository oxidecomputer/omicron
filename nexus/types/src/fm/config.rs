// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Runtime configuration for the fault management subsystem.
//!
//! FM configuration is versioned: changes are made by inserting a new config
//! with the next version number, rather than by updating the current one in
//! place. The database is seeded with a default config at version 1, so a
//! current config always exists.
//!
//! Configuration values are represented by two types: a [`FmConfigParam`]
//! holds unvalidated values (a new version requested by a caller, or a row
//! read back from the database), while a [`FmConfig`] has been validated
//! against the invariants described in [`FmConfigParam`]'s documentation.

use std::fmt;
use std::num::NonZeroU32;

use chrono::{DateTime, Utc};
use omicron_common::api::external::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An unvalidated FM configuration at a particular version, such as a request
/// to insert a new FM config version.
///
/// An insert succeeds only if `version` is exactly one greater than the
/// current latest version, and if the configuration values pass validation.
/// This type is entirely unvalidated, and its invariants may not hold:
///
/// - `version` must be nonzero,
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
    pub version: u32,

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

/// An FM config version as read back from the database.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct FmConfigView {
    pub config: FmConfig,
    pub time_modified: DateTime<Utc>,
}

impl FmConfigView {
    pub fn display(&self) -> FmConfigViewDisplay<'_> {
        FmConfigViewDisplay { view: self }
    }
}

#[derive(Clone, Debug)]
pub struct FmConfigViewDisplay<'a> {
    view: &'a FmConfigView,
}

impl fmt::Display for FmConfigViewDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { view: FmConfigView { config, time_modified } } = self;
        writeln!(
            f,
            "last modified at: {}",
            humantime::format_rfc3339_millis((*time_modified).into())
        )?;
        // No need for a newline here because .display() adds its own newline
        // at the end.
        write!(f, "{}", config.display())
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
    pub version: NonZeroU32,

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

    pub fn display(&self) -> FmConfigDisplay<'_> {
        FmConfigDisplay { config: self }
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
        let &FmConfigParam { version, sitrep_limit, sitrep_deletion_threshold } =
            value;

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

        let version = NonZeroU32::new(version).ok_or_else(|| {
            Error::invalid_value("version", "version must be nonzero")
        })?;
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
        Ok(Self { version, sitrep_limit, sitrep_deletion_threshold })
    }
}

#[derive(Clone, Debug)]
pub struct FmConfigDisplay<'a> {
    config: &'a FmConfig,
}

impl fmt::Display for FmConfigDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            config:
                FmConfig { version, sitrep_limit, sitrep_deletion_threshold },
        } = self;
        writeln!(f, "version: {version}")?;
        writeln!(f, "sitrep limit: {sitrep_limit}")?;
        writeln!(f, "sitrep deletion threshold: {sitrep_deletion_threshold}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_rejects_zero_limit() {
        let err = FmConfig::try_from(&FmConfigParam {
            version: 1,
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
    fn validation_rejects_too_small_limit() {
        let err = FmConfig::try_from(&FmConfigParam {
            version: 1,
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
    fn validation_rejects_zero_threshold() {
        let err = FmConfig::try_from(&FmConfigParam {
            version: 1,
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
    fn validation_rejects_too_small_threshold() {
        let err = FmConfig::try_from(&FmConfigParam {
            version: 1,
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
    fn validation_rejects_threshold_not_less_than_limit() {
        for threshold in [100, 101] {
            let err = FmConfig::try_from(&FmConfigParam {
                version: 1,
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
