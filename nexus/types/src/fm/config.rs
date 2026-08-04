// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Runtime configuration for the fault management subsystem.
//!
//! [`FmConfigParam`] is the params type for setting a new config, while
//! [`FmConfigView`] is the view representing an already-set config version.
//! [`FmConfig`] is a shared representation of the configuration values
//! themselves.
//!
//! The default configuration is defined by [`FmConfig::default`]. This
//! configuration may be persistently overridden by creating rows in the
//! `fm_config` database table. Overrides are versioned: changes are made by
//! inserting a new config with the next version number, rather than by updating
//! the current one in place. [`FmConfigView::source`] indicates whether a
//! configuration came from the current default or a database override.
use super::display::const_max_len;
use std::fmt;
use std::num::NonZeroU32;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use omicron_common::api::external::Error;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Parameters to insert a new FM configuration override at a particular
/// version.
///
/// A config override is accepted if and only if:
///
/// - [`Self::version`] is exactly one greater than the current latest override
///    version (or 1, if no overrides exist),
/// - [`Self::comment`] is a non-empty string, and does not  consist entirely
///   of whitespace characters, and,
/// - The values in [`Self::config`] pass [validation](FmConfig#validation).
///
/// The [`FmConfigParam::validate`] method checks all of these requirements and
/// is called prior to inserting a new config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FmConfigParam {
    /// The version of the configuration.
    pub version: NonZeroU32,

    /// A comment describing why the default configuration was overridden.
    ///
    /// A comment is mandatory when overriding the default configuration, so
    /// this must be non-empty.
    pub comment: String,

    /// The configuration itself.
    pub config: FmConfig,
}

impl FmConfigParam {
    pub fn validate(&self) -> Result<(), Error> {
        if self.comment.trim().is_empty() {
            return Err(Error::invalid_value(
                "comment",
                "when overriding the fault management configuration, \
                 please provide a non-empty, non-whitespace comment, for the \
                 benefit of future operators",
            ));
        }

        self.config.validate()
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(tag = "source", content = "value")]
#[serde(rename = "snake_case")]
pub enum Setting<V: SettingValue> {
    Override(V::Value),
    #[default]
    Default,
}

pub trait SettingValue {
    type Value: Serialize
        + DeserializeOwned
        + JsonSchema
        + Clone
        + PartialEq
        + Eq;
    const DEFAULT: Self::Value;
}

impl<V: SettingValue> FromStr for Setting<V>
where
    V::Value: FromStr,
{
    type Err = <V::Value as FromStr>::Err;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().eq_ignore_ascii_case("default") {
            return Ok(Self::Default);
        }

        s.parse::<V::Value>().map(Self::Override)
    }
}

impl<V: SettingValue> Setting<V> {
    pub fn new(value: V::Value) -> Self {
        Self::Override(value)
    }

    pub fn map_override<T>(self, f: impl FnOnce(V::Value) -> T) -> Option<T> {
        match self {
            Self::Override(val) => Some(f(val)),
            Self::Default => None,
        }
    }

    pub fn into_override(self) -> Option<V::Value> {
        self.map_override(core::convert::identity)
    }

    pub fn as_override(&self) -> Option<&V::Value> {
        match self {
            Self::Override(v) => Some(v),
            Self::Default => None,
        }
    }

    pub fn value(self) -> V::Value {
        match self {
            Self::Override(v) => v,
            Self::Default => V::DEFAULT,
        }
    }
}

impl<V: SettingValue> From<Option<V::Value>> for Setting<V> {
    fn from(value: Option<V::Value>) -> Self {
        match value {
            Some(v) => Self::Override(v),
            None => Self::Default,
        }
    }
}

impl<V: SettingValue> fmt::Display for Setting<V>
where
    V::Value: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Override(v) => {
                write!(f, "{v} (overriden)")
            }
            Self::Default => {
                write!(f, "{} (default)", V::DEFAULT)
            }
        }
    }
}

impl<V: SettingValue> fmt::Debug for Setting<V>
where
    V::Value: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => {
                f.debug_tuple("Setting::Default").field(&V::DEFAULT).finish()
            }
            Self::Override(v) => {
                f.debug_tuple("Setting::Override").field(v).finish()
            }
        }
    }
}

impl<V: SettingValue> PartialEq for Setting<V> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Default, Self::Default) => true,
            (Self::Override(this), Self::Override(that)) => this == that,
            // NOTE: should we check if an override happens to be the same as
            // the default value here? hm...no, because they have different
            // intents! defaults are "floating" at the current software
            // version's defined default, while overrides are *always* that
            // value.
            _ => false,
        }
    }
}

impl<V: SettingValue> Eq for Setting<V> {}

impl<V: SettingValue> Clone for Setting<V> {
    fn clone(&self) -> Self {
        match self {
            Self::Default => Self::Default,
            Self::Override(v) => Self::Override(v.clone()),
        }
    }
}

impl<V: SettingValue> Copy for Setting<V> where V::Value: Copy {}

impl<V: SettingValue> JsonSchema for Setting<V> {
    fn schema_name() -> String {
        <V::Value>::schema_name()
    }

    fn json_schema(
        generator: &mut schemars::SchemaGenerator,
    ) -> schemars::schema::Schema {
        <V::Value>::json_schema(generator)
    }
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
#[serde(tag = "type")]
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
                        for line in comment.lines() {
                            writeln!(f, "{:>indent$}    {line}", "")?;
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

/// A fault management configuration.
///
/// # Validation
///
/// For a config to be valid, the following requirements must be upheld:
///
/// - [`Self::sitrep_limit`] must be at least [`Self::MIN_SITREP_LIMIT`],
///   and no more than [`Self::MAX_LIMIT`],
/// - [`Self::history_pruning_threshold`] must be at least
///   [`Self::MIN_HISTORY_PRUNING_THRESHOLD`], and no more than
///   [`Self::MAX_LIMIT`],
/// - [`Self::history_pruning_threshold`] must be strictly less than
///   [`Self::sitrep_limit`],
///
/// These rules are checked by the [`Self::validate`] method, which is called
/// prior to accepting a config update.
#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct FmConfig {
    /// BREAK GLASS TO COMPLETELY DISABLE FM ANALYSIS
    pub analysis_enabled: Setting<AnalysisEnabled>,

    /// The maximum number of sitreps to keep in the database.
    ///
    /// If the number of sitreps exceeds this limit, the FM analysis background
    /// task will not produce a new sitrep until old ones are deleted.
    ///
    /// This limit applies to both committed sitreps in the history *and*
    /// orphaned sitreps left behind when multiple Nexuses race to commit a
    /// sitrep.
    pub sitrep_limit: Setting<SitrepLimit>,

    /// The maximum number of sitreps committed to the `fm_sitrep_history`
    /// table. If the number of sitreps exceeds this threshold, the
    /// `fm_sitrep_history_pruner` background task will remove the oldest
    /// entries from the history.
    pub history_pruning_threshold: Setting<HistoryPruningThreshold>,
}

use self::settings::*;
pub mod settings {
    use super::*;

    macro_rules! define_setting {
        ($Name:ident: $Type:ty = $default_value:expr) => {
            pub enum $Name {}
            impl SettingValue for $Name {
                type Value = $Type;
                const DEFAULT: Self::Value = { $default_value };
            }
        };
    }

    define_setting! { AnalysisEnabled: bool = true }
    define_setting! { SitrepLimit: NonZeroU32 = FmConfig::DEFAULT_SITREP_LIMIT }
    define_setting! {
        HistoryPruningThreshold: NonZeroU32 =
            FmConfig::DEFAULT_HISTORY_PRUNING_THRESHOLD
    }
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

    /// Validate this `FmConfig`.
    pub fn validate(&self) -> Result<(), Error> {
        fn check_limit(
            value: NonZeroU32,
            field: &str,
            min: NonZeroU32,
            max: NonZeroU32,
        ) -> Result<(), Error> {
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
            Ok(())
        }

        let sitrep_limit = self.sitrep_limit.value();
        let history_pruning_threshold = self.history_pruning_threshold.value();

        check_limit(
            sitrep_limit,
            "sitrep_limit",
            Self::MIN_SITREP_LIMIT,
            Self::MAX_LIMIT,
        )?;
        check_limit(
            history_pruning_threshold,
            "history_pruning_threshold",
            Self::MIN_HISTORY_PRUNING_THRESHOLD,
            Self::MAX_LIMIT,
        )?;

        if history_pruning_threshold >= sitrep_limit {
            return Err(Error::invalid_value(
                "history_pruning_threshold",
                format!(
                    "sitrep history pruning threshold \
                     ({history_pruning_threshold})  must be less than the \
                     total sitrep limit ({sitrep_limit})",
                ),
            ));
        }

        Ok(())
    }
}

const SOURCE: &str = "source:";
const VERSION: &str = "  version:";
const TIME_MODIFIED: &str = "  modified at:";
const COMMENT: &str = "  comment:";
const ANALYSIS_ENABLED: &str = "analysis enabled:";
const SITREP_LIMIT: &str = "sitrep limit:";
const HISTORY_PRUNING_THRESHOLD: &str = "history pruning threshold:";
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
            config: FmConfig {
                analysis_enabled: Setting::new(true),
                sitrep_limit: Setting::new(
                    NonZeroU32::new(sitrep_limit)
                        .expect("test sitrep_limit must be nonzero"),
                ),
                history_pruning_threshold: Setting::new(
                    NonZeroU32::new(history_pruning_threshold).expect(
                        "test history_pruning_threshold must be nonzero",
                    ),
                ),
            },
        }
    }

    #[test]
    fn test_min_sitrep_limit() {
        let err = param(2, 2).validate().unwrap_err();
        assert!(
            err.to_string().contains("sitrep_limit must be at least"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_min_history_pruning_threshold() {
        let err = param(100, 1).validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("history_pruning_threshold must be at least"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_history_pruning_threshold_must_be_less_than_sitrep_limit() {
        for threshold in [100, 101] {
            let err = param(100, threshold).validate().unwrap_err();
            assert!(
                err.to_string()
                    .contains("must be less than the total sitrep limit"),
                "unexpected error: {err}"
            );
        }
    }
}
