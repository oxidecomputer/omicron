// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Runtime configuration for reconfigurator

use std::fmt::{self, Write};

use chrono::{DateTime, TimeZone, Utc};
use daft::Diffable;
use indent_write::fmt::IndentWriter;
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
    pub planner_config: PlannerConfig,
    pub tuf_repo_pruner_enabled: bool,
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
                    planner_config,
                    tuf_repo_pruner_enabled,
                },
        } = self;
        writeln!(f, "tuf repo pruner enabled: {}", tuf_repo_pruner_enabled)?;
        writeln!(f, "planner enabled: {}", planner_enabled)?;
        writeln!(f, "planner config:")?;
        // planner_config does its own indentation, so it's not necessary to
        // use IndentWriter here -- and it adds its own newlines so we don't
        // need to add any more.
        write!(f, "{}", planner_config.display())?;

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
            planner_config,
            tuf_repo_pruner_enabled,
        } = self.diff;

        let list = KvList::new(
            None,
            vec![
                diff_row!(tuf_repo_pruner_enabled, "tuf repo pruner enabled"),
                diff_row!(planner_enabled, "planner enabled"),
            ],
        );
        // No need for writeln! here because KvList adds its own newlines.
        write!(f, "{list}")?;

        let mut indented = IndentWriter::new("    ", f);
        writeln!(indented, "planner config:")?;
        write!(indented, "{}", planner_config.display())?;

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
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct PlannerConfig {
    /// Whether to add zones even if a mupdate override is present.
    ///
    /// Once Nexus-driven update is active on a customer system, we must not add
    /// new zones while the system is recovering from a MUPdate.
    ///
    /// This setting, which is off by default, allows us to add zones
    /// even if we've detected a recent MUPdate on the system.
    pub add_zones_with_mupdate_override: bool,
}

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
        // By default, we block zone additions on mupdate overrides being
        // present (see the docs on `add_zones_with_mupdate_override` above).
        Self { add_zones_with_mupdate_override: false }
    }
}

impl slog::KV for PlannerConfig {
    fn serialize(
        &self,
        _record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let Self { add_zones_with_mupdate_override } = self;
        serializer.emit_bool(
            slog::Key::from("add_zones_with_mupdate_override"),
            *add_zones_with_mupdate_override,
        )
    }
}

pub struct PlannerConfigDisplay<'a> {
    config: &'a PlannerConfig,
}

impl<'a> fmt::Display for PlannerConfigDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { config: PlannerConfig { add_zones_with_mupdate_override } } =
            self;
        let list = KvList::new(
            None,
            vec![KvPair::new_unchanged(
                "add zones with mupdate override",
                add_zones_with_mupdate_override.to_string(),
            )],
        );
        // No need for writeln! here because KvList adds its own newlines.
        write!(f, "{list}")
    }
}

impl<'a> PlannerConfigDiff<'a> {
    pub fn display<'b>(&'b self) -> PlannerConfigDiffDisplay<'a, 'b> {
        PlannerConfigDiffDisplay { diff: self }
    }
}

#[derive(Clone, Debug)]
pub struct PlannerConfigDiffDisplay<'a, 'b> {
    diff: &'b PlannerConfigDiff<'a>,
}

impl fmt::Display for PlannerConfigDiffDisplay<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let PlannerConfigDiff { add_zones_with_mupdate_override } = self.diff;

        let list = KvList::new(
            None,
            vec![diff_row!(
                add_zones_with_mupdate_override,
                "add zones with mupdate override"
            )],
        );

        // No need for writeln! here because KvList adds its own newlines.
        write!(f, "{list}")
    }
}
