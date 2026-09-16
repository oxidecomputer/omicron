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
    pub blueprint_pruner_enabled: bool,
    pub blueprint_pruner_nkeep: u32,
}

/// Default value for `ReconfiguratorConfig::blueprint_pruner_nkeep`
///
/// This is intended to cover 1-2 upgrades' worth of blueprints so that
/// developers and support can debug a live system back through its last
/// upgrade.
pub const DEFAULT_BLUEPRINT_PRUNER_NKEEP: u32 = 1000;

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
            blueprint_pruner_enabled: true,
            blueprint_pruner_nkeep: DEFAULT_BLUEPRINT_PRUNER_NKEEP,
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
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
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

impl ReconfiguratorDisruptionPolicy {
    pub const ALL_VARIANTS: &[Self] = <Self as strum::VariantArray>::VARIANTS;
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
                    planner_config,
                    tuf_repo_pruner_enabled,
                    blueprint_pruner_enabled,
                    blueprint_pruner_nkeep,
                },
        } = self;
        writeln!(f, "tuf repo pruner enabled: {}", tuf_repo_pruner_enabled)?;
        writeln!(f, "blueprint pruner enabled: {}", blueprint_pruner_enabled)?;
        writeln!(f, "blueprint pruner nkeep: {}", blueprint_pruner_nkeep)?;
        writeln!(f, "planner enabled: {}", planner_enabled)?;
        writeln!(f, "planner config:\n{}", planner_config.display())?;

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
            planner_config:
                PlannerConfigDiff { disruption_policy, sled_reboot_policy },
            tuf_repo_pruner_enabled,
            blueprint_pruner_enabled,
            blueprint_pruner_nkeep,
        } = self.diff;

        let list = KvList::new(
            None,
            vec![
                diff_row!(tuf_repo_pruner_enabled, "tuf repo pruner enabled"),
                diff_row!(planner_enabled, "planner enabled"),
                diff_row!(disruption_policy, "disruption policy"),
                diff_row!(sled_reboot_policy, "sled reboot policy"),
                diff_row!(blueprint_pruner_enabled, "blueprint pruner enabled"),
                diff_row!(blueprint_pruner_nkeep, "blueprint pruner nkeep"),
            ],
        );
        // No need for writeln! here because KvList adds its own newlines.
        write!(f, "{list}")?;

        Ok(())
    }
}

/// Controls how the planner interacts with sleds that it needs to reboot to
/// perform an update (e.g., of the SP or host OS).
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
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum PlannerSledRebootPolicy {
    /// Reboot sleds without evacuating them first.
    ///
    /// This results in the fastest update time, but is the most disruptive to
    /// instances on the sled: the sled will be rebooted out from under them.
    ///
    /// This is currently the default, but we expect to change the default to
    /// `Evacuate` once all the supporting work to enable that is complete.
    //
    // TODO-correctness: This must remain the default until all the pieces
    // required for sled evacuation land (in particular: the planner bits to
    // mark sleds for evacuation and the Nexus bg task to enact sled
    // evacuation). Once those are in place, we'll need to change this to
    // `Evacuate` and update any existing configs persisted in the db to
    // flip from this setting to `Evacuate`.
    #[default]
    ImmediateNoEvacuation,

    /// Evacuate sleds before rebooting.
    ///
    /// With this policy, when the planner needs to schedule an update that will
    /// cause a sled to reboot, it will first mark the sled for evacuation and
    /// then wait for the sled to be fully evacuated before scheduling that
    /// update.
    Evacuate,
}

impl fmt::Display for PlannerSledRebootPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ImmediateNoEvacuation => {
                write!(f, "immediate (no evacuation)")
            }
            Self::Evacuate => {
                write!(f, "evacuate")
            }
        }
    }
}

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
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct PlannerConfig {
    /// Policy for how the planner schedules updates that will induce reboots on
    /// sleds.
    ///
    /// If `sled_reboot_policy` is
    /// [`PlannerSledRebootPolicy::ImmediateNoEvacuation`], the planner will not
    /// mark sleds for evacuation at all, which means `disruption_policy` will
    /// be ignored.
    //
    // We could combine this field with `disruption_policy` at the typesystem
    // level to make it clear that `disruption_policy` is only applicable for
    // certain `sled_reboot_policy` values, but that's less ergonomic from an
    // omdb perspective: in most cases we expect to be adjusting only one or the
    // other of these, and if we want to temporarily disable evacuation, we'll
    // almost certainly want to preserve the existing `disruption_policy`
    // whenever we reenable it.
    pub sled_reboot_policy: PlannerSledRebootPolicy,

    /// Disruption policy applied to sleds being evacuated.
    pub disruption_policy: ReconfiguratorDisruptionPolicy,
}

impl PlannerConfig {
    pub fn display(&self) -> PlannerConfigDisplay<'_> {
        PlannerConfigDisplay { config: self }
    }
}

pub struct PlannerConfigDisplay<'a> {
    config: &'a PlannerConfig,
}

impl<'a> fmt::Display for PlannerConfigDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            config: PlannerConfig { disruption_policy, sled_reboot_policy },
        } = self;
        writeln!(f, "    disruption policy : {}", disruption_policy)?;
        writeln!(f, "    sled reboot policy: {}", sled_reboot_policy)?;
        Ok(())
    }
}

impl<'a> PlannerConfigDiff<'a> {
    pub fn display(&self) -> PlannerConfigDiffDisplay<'a, '_> {
        PlannerConfigDiffDisplay { diff: self }
    }
}

pub struct PlannerConfigDiffDisplay<'a, 'b> {
    diff: &'b PlannerConfigDiff<'a>,
}

impl fmt::Display for PlannerConfigDiffDisplay<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let PlannerConfigDiff { disruption_policy, sled_reboot_policy } =
            self.diff;

        let list = KvList::new(
            None,
            vec![
                diff_row!(disruption_policy, "disruption policy"),
                diff_row!(sled_reboot_policy, "sled reboot policy"),
            ],
        );
        // No need for writeln! here because KvList adds its own newlines.
        write!(f, "{list}")?;

        Ok(())
    }
}
