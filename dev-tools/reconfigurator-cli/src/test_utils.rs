// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! helpers for writing Rust tests that use reconfigurator-cli to drive a
//! simulated system

use crate::LogCapture;
use crate::ReconfiguratorSim;
use crate::TopLevelArgs;
use crate::process_command;
use anyhow::Context;
use anyhow::bail;
use clap::CommandFactory;
use clap::FromArgMatches;
use nexus_reconfigurator_simulation::BlueprintId;
use nexus_reconfigurator_simulation::errors::KeyError;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintDiffSummary;
use nexus_types::deployment::PlanningInput;
use slog::Logger;

pub struct ReconfiguratorCliTestState {
    sim: ReconfiguratorSim,
    noop_logcapture: LogCapture,
}

impl ReconfiguratorCliTestState {
    /// Construct a new `reconfigurator-cli` simulation instance, wrapped in a
    /// `ReconfiguratorCliTestState` for use by Rust tests.
    pub fn new(test_name: &str, log: &Logger) -> Self {
        let noop_logcapture = LogCapture::new_noop();
        let sim = ReconfiguratorSim::new(
            log.new(slog::o!("component" => "ReconfiguratorSim")),
            Some(test_name.to_string()),
        );
        Self { sim, noop_logcapture }
    }

    /// Run a series of `reconfigurator-cli` commands, returning an error on the
    /// first that fails (or `Ok(())` if all succeed).
    pub fn run(&mut self, commands: &[&str]) -> anyhow::Result<()> {
        for command in commands {
            let cmd = TopLevelArgs::command()
                .multicall(true)
                .try_get_matches_from(command.split_whitespace())
                .and_then(|matches| TopLevelArgs::from_arg_matches(&matches))
                .with_context(|| {
                    format!("failed to parse command {command}")
                })?;
            if let Some(output) =
                process_command(&mut self.sim, cmd, &self.noop_logcapture)?
            {
                println!("{output}");
            }
        }

        Ok(())
    }

    /// Get the specified blueprint.
    pub fn blueprint(&self, id: BlueprintId) -> Result<&Blueprint, KeyError> {
        let state = self.sim.current_state();
        state.system().resolve_and_get_blueprint(id)
    }

    /// Compute the diff between two blueprints.
    pub fn blueprint_diff(
        &self,
        from_id: BlueprintId,
        to_id: BlueprintId,
    ) -> Result<BlueprintDiffSummary<'_>, KeyError> {
        let from = self.blueprint(from_id)?;
        let to = self.blueprint(to_id)?;
        Ok(to.diff_since_blueprint(from))
    }

    /// Compute the diff from a blueprint's parent to itself.
    pub fn blueprint_diff_parent(
        &self,
        id: BlueprintId,
    ) -> anyhow::Result<BlueprintDiffSummary<'_>> {
        let blueprint = self.blueprint(id)?;
        let Some(parent_id) = blueprint.parent_blueprint_id else {
            bail!("blueprint {} does not have a parent", blueprint.id);
        };
        let parent = self.blueprint(BlueprintId::Id(parent_id))?;
        Ok(blueprint.diff_since_blueprint(parent))
    }

    /// Assemble the current planning input, using `parent_blueprint` to derive
    /// information that typically comes from a live system (e.g., the set of
    /// active Nexus instances).
    pub fn planning_input(
        &self,
        parent_blueprint: BlueprintId,
    ) -> anyhow::Result<PlanningInput> {
        let parent_blueprint = self
            .blueprint(parent_blueprint)
            .context("invalid parent blueprint")?;
        self.sim.planning_input(parent_blueprint)
    }
}
