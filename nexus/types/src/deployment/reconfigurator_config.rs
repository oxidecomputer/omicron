// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Runtime configuration for reconfigurator

use std::fmt;

use chrono::{DateTime, TimeZone, Utc};
use daft::Diffable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::deployment::blueprint_display::{BpDiffState, KvList, KvPair};

macro_rules! diff_row {
    ($diff:expr, $label:expr) => {
        if $diff.before == $diff.after {
            KvPair::new(
                BpDiffState::Unchanged,
                $label,
                super::blueprint_display::linear_table_unchanged(&$diff.after),
            )
        } else {
            KvPair::new(
                BpDiffState::Modified,
                $label,
                super::blueprint_display::linear_table_modified(
                    &$diff.before,
                    &$diff.after,
                ),
            )
        }
    };
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct ReconfiguratorConfigParam {
    pub version: u32,
    pub config: ReconfiguratorConfig,
}

impl Default for ReconfiguratorConfigParam {
    fn default() -> Self {
        Self {
            // The first supported version is 1.
            version: 1,
            config: ReconfiguratorConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReconfiguratorConfigView {
    pub version: u32,
    pub config: ReconfiguratorConfig,
    pub time_modified: DateTime<Utc>,
}

impl ReconfiguratorConfigView {
    pub fn display(&self) -> ReconfiguratorConfigViewDisplay<'_> {
        ReconfiguratorConfigViewDisplay { view: self }
    }
}

impl Default for ReconfiguratorConfigView {
    fn default() -> Self {
        // Use the default values from `ReconfiguratorConfigParam`.
        let ReconfiguratorConfigParam { version, config } =
            ReconfiguratorConfigParam::default();
        Self {
            version,
            config,
            time_modified: Utc.with_ymd_and_hms(1970, 1, 1, 0, 1, 1).unwrap(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReconfiguratorConfigViewDisplay<'a> {
    view: &'a ReconfiguratorConfigView,
}

impl fmt::Display for ReconfiguratorConfigViewDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            view: ReconfiguratorConfigView { version, config, time_modified },
        } = self;
        writeln!(f, "version: {version}")?;
        writeln!(
            f,
            "modified time: {}",
            humantime::format_rfc3339_millis((*time_modified).into())
        )?;
        // No need for a newline here because .display() adds its own newline at
        // the end.
        write!(f, "{}", config.display())?;

        Ok(())
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Diffable,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct ReconfiguratorConfig {
    pub planner_enabled: bool,
    #[serde(default)]
    pub planner_config: PlannerConfig,
    pub tuf_repo_pruner_enabled: bool,
    pub disruption_policy: ReconfiguratorDisruptionPolicy,
}

impl ReconfiguratorConfig {
    pub fn display(&self) -> ReconfiguratorConfigDisplay<'_> {
        ReconfiguratorConfigDisplay { config: self }
    }
}

impl Default for ReconfiguratorConfig {
    fn default() -> Self {
        Self {
            planner_enabled: true,
            planner_config: PlannerConfig::default(),
            tuf_repo_pruner_enabled: true,
            disruption_policy: ReconfiguratorDisruptionPolicy::default(),
        }
    }
}

/// Controls how instances are disrupted during updates.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Diffable,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ReconfiguratorDisruptionPolicy {
    /// Terminate instances during updates -- do not attempt to migrate
    /// instances. This is currently the default.
    #[default]
    Terminate,

    /// Attempt to live-migrate instances, and terminate instances if migration
    /// is not possible.
    MigrateOrTerminate,

    /// Live-migrate any potentially migratable instances, and block
    /// reconfigurator progress if migration is not currently possible for
    /// operational reasons.
    ///
    /// This will still cause instances to be terminated if it is impossible for
    /// them to be live-migrated, such as if they use local storage.
    MigrateOnly,
}

impl fmt::Display for ReconfiguratorDisruptionPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReconfiguratorDisruptionPolicy::Terminate => write!(f, "terminate"),
            ReconfiguratorDisruptionPolicy::MigrateOrTerminate => {
                write!(f, "live-migrate or terminate")
            }
            ReconfiguratorDisruptionPolicy::MigrateOnly => {
                write!(f, "live-migrate only")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReconfiguratorConfigDisplay<'a> {
    config: &'a ReconfiguratorConfig,
}

impl fmt::Display for ReconfiguratorConfigDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            config:
                ReconfiguratorConfig {
                    planner_enabled,
                    planner_config: _,
                    tuf_repo_pruner_enabled,
                    disruption_policy,
                },
        } = self;
        writeln!(f, "tuf repo pruner enabled: {}", tuf_repo_pruner_enabled)?;
        writeln!(f, "disruption policy: {}", disruption_policy)?;
        writeln!(f, "planner enabled: {}", planner_enabled)?;

        Ok(())
    }
}

impl<'a> ReconfiguratorConfigDiff<'a> {
    pub fn display(&self) -> ReconfiguratorConfigDiffDisplay<'a, '_> {
        ReconfiguratorConfigDiffDisplay { diff: self }
    }
}

pub struct ReconfiguratorConfigDiffDisplay<'a, 'b> {
    diff: &'b ReconfiguratorConfigDiff<'a>,
}

impl fmt::Display for ReconfiguratorConfigDiffDisplay<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ReconfiguratorConfigDiff {
            planner_enabled,
            planner_config: _,
            tuf_repo_pruner_enabled,
            disruption_policy,
        } = self.diff;

        let list = KvList::new(
            None,
            vec![
                diff_row!(tuf_repo_pruner_enabled, "tuf repo pruner enabled"),
                diff_row!(planner_enabled, "planner enabled"),
                diff_row!(disruption_policy, "disruption policy"),
            ],
        );
        // No need for writeln! here because KvList adds its own newlines.
        write!(f, "{list}")?;

        // When there are fields in `PlannerConfigDiff`, this is where their
        // display would go.

        Ok(())
    }
}

// `PlannerConfig` is currently empty. Future planner-wide feature flags will be
// defined here.
#[derive(
    Clone,
    Copy,
    Debug,
    Diffable,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct PlannerConfig {}

impl PlannerConfig {
    pub fn display(&self) -> PlannerConfigDisplay<'_> {
        PlannerConfigDisplay { config: self }
    }
}

// Allow the clippy::derivable_impls lint: spell out the default values for
// these config values for clarity.
#[expect(clippy::derivable_impls)]
impl Default for PlannerConfig {
    fn default() -> Self {
        Self {}
    }
}

pub struct PlannerConfigDisplay<'a> {
    config: &'a PlannerConfig,
}

impl<'a> fmt::Display for PlannerConfigDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { config: PlannerConfig {} } = self;
        // `PlannerConfig` currently has no fields; once it gains some, we'll
        // render them here as a `KvList`. The 4-space indent matches where
        // those rows would appear.
        writeln!(f, "    (no current planner configs)")
    }
}
