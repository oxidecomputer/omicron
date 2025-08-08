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
pub struct ReconfiguratorChickenSwitchesParam {
    pub version: u32,
    pub switches: ReconfiguratorChickenSwitches,
}

impl Default for ReconfiguratorChickenSwitchesParam {
    fn default() -> Self {
        Self {
            // The first supported version is 1.
            version: 1,
            switches: ReconfiguratorChickenSwitches::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReconfiguratorChickenSwitchesView {
    pub version: u32,
    pub switches: ReconfiguratorChickenSwitches,
    pub time_modified: DateTime<Utc>,
}

impl ReconfiguratorChickenSwitchesView {
    pub fn display(&self) -> ReconfiguratorChickenSwitchesViewDisplay<'_> {
        ReconfiguratorChickenSwitchesViewDisplay { view: self }
    }
}

impl Default for ReconfiguratorChickenSwitchesView {
    fn default() -> Self {
        // Use the default values from `ReconfiguratorChickenSwitchesParam`.
        let ReconfiguratorChickenSwitchesParam { version, switches } =
            ReconfiguratorChickenSwitchesParam::default();
        Self {
            version,
            switches,
            time_modified: Utc.with_ymd_and_hms(1970, 1, 1, 0, 1, 1).unwrap(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReconfiguratorChickenSwitchesViewDisplay<'a> {
    view: &'a ReconfiguratorChickenSwitchesView,
}

impl fmt::Display for ReconfiguratorChickenSwitchesViewDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            view:
                ReconfiguratorChickenSwitchesView {
                    version,
                    switches,
                    time_modified,
                },
        } = self;
        writeln!(f, "version: {version}")?;
        writeln!(
            f,
            "modified time: {}",
            humantime::format_rfc3339_millis((*time_modified).into())
        )?;
        // No need for a newline here because .display() adds its own newline at
        // the end.
        write!(f, "{}", switches.display())?;

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
pub struct ReconfiguratorChickenSwitches {
    pub planner_enabled: bool,
    pub planner_switches: PlannerChickenSwitches,
}

impl ReconfiguratorChickenSwitches {
    pub fn display(&self) -> ReconfiguratorChickenSwitchesDisplay<'_> {
        ReconfiguratorChickenSwitchesDisplay { switches: self }
    }
}

// Allow the clippy::derivable_impls lint: spell out the default values for
// these chicken switches for clarity.
#[expect(clippy::derivable_impls)]
impl Default for ReconfiguratorChickenSwitches {
    fn default() -> Self {
        Self {
            planner_enabled: false,
            planner_switches: PlannerChickenSwitches::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReconfiguratorChickenSwitchesDisplay<'a> {
    switches: &'a ReconfiguratorChickenSwitches,
}

impl fmt::Display for ReconfiguratorChickenSwitchesDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            switches:
                ReconfiguratorChickenSwitches { planner_enabled, planner_switches },
        } = self;
        writeln!(f, "planner enabled: {}", planner_enabled)?;
        writeln!(f, "planner switches:")?;
        // planner_switches does its own indentation, so it's not necessary to
        // use IndentWriter here -- and it adds its own newlines so we don't
        // need to add any more.
        write!(f, "{}", planner_switches.display())?;

        Ok(())
    }
}

impl<'a> ReconfiguratorChickenSwitchesDiff<'a> {
    pub fn display(&self) -> ReconfiguratorChickenSwitchesDiffDisplay<'a, '_> {
        ReconfiguratorChickenSwitchesDiffDisplay { diff: self }
    }
}

pub struct ReconfiguratorChickenSwitchesDiffDisplay<'a, 'b> {
    diff: &'b ReconfiguratorChickenSwitchesDiff<'a>,
}

impl fmt::Display for ReconfiguratorChickenSwitchesDiffDisplay<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ReconfiguratorChickenSwitchesDiff {
            planner_enabled,
            planner_switches,
        } = self.diff;

        let list = KvList::new(
            None,
            vec![diff_row!(planner_enabled, "planner enabled")],
        );
        // No need for writeln! here because KvList adds its own newlines.
        write!(f, "{list}")?;

        let mut indented = IndentWriter::new("    ", f);
        writeln!(indented, "planner switches:")?;
        write!(indented, "{}", planner_switches.display())?;

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
pub struct PlannerChickenSwitches {
    /// Whether to add zones even if a mupdate override is present.
    ///
    /// Once Nexus-driven update is active on a customer system, we must not add
    /// new zones while the system is recovering from a MUPdate. But that would
    /// require customers to upload a TUF repo before adding a new sled, even
    /// though Nexus-driven update is not active (as of r16).
    ///
    /// This switch, which is currently on by default, allows us to add zones
    /// even if we've detected a recent MUPdate on the system. We will want to
    /// turn it off as part of enabling Nexus-driven update.
    pub add_zones_with_mupdate_override: bool,
}

impl PlannerChickenSwitches {
    /// Returns the default value for a `SystemDescription`, e.g. in
    /// reconfigurator-cli.
    pub fn default_for_system_description() -> Self {
        // In reconfigurator-cli we set this to false to ensure tests run
        // against the desired configuration for r17.
        Self { add_zones_with_mupdate_override: false }
    }

    pub fn display(&self) -> PlannerChickenSwitchesDisplay<'_> {
        PlannerChickenSwitchesDisplay { switches: self }
    }
}

impl Default for PlannerChickenSwitches {
    fn default() -> Self {
        // On customer systems for now, we don't block zone additions on mupdate
        // overrides being present.
        Self { add_zones_with_mupdate_override: true }
    }
}

impl slog::KV for PlannerChickenSwitches {
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

pub struct PlannerChickenSwitchesDisplay<'a> {
    switches: &'a PlannerChickenSwitches,
}

impl<'a> fmt::Display for PlannerChickenSwitchesDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            switches: PlannerChickenSwitches { add_zones_with_mupdate_override },
        } = self;
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

impl<'a> PlannerChickenSwitchesDiff<'a> {
    pub fn display<'b>(&'b self) -> PlannerChickenSwitchesDiffDisplay<'a, 'b> {
        PlannerChickenSwitchesDiffDisplay { diff: self }
    }
}

#[derive(Clone, Debug)]
pub struct PlannerChickenSwitchesDiffDisplay<'a, 'b> {
    diff: &'b PlannerChickenSwitchesDiff<'a>,
}

impl fmt::Display for PlannerChickenSwitchesDiffDisplay<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let PlannerChickenSwitchesDiff { add_zones_with_mupdate_override } =
            self.diff;

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
