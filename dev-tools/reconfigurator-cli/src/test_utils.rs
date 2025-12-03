// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! helpers for writing Rust tests that use reconfigurator-cli to drive a
//! simulated system

use std::sync::Arc;

use crate::BlueprintIdOpt;
use crate::BlueprintPlanArgs;
use crate::CollectionIdOpt;
use crate::ReconfiguratorSim;
use crate::cmd_blueprint_plan;
use anyhow::Context;
use nexus_inventory::now_db_precision;
use nexus_reconfigurator_blippy::Blippy;
use nexus_reconfigurator_blippy::BlippyReportSortKey;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
use nexus_reconfigurator_planning::system::SledBuilder;
use nexus_reconfigurator_simulation::BlueprintId;
use nexus_reconfigurator_simulation::SimStateBuilder;
use nexus_reconfigurator_simulation::errors::KeyError;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintSource;
use nexus_types::deployment::PlanningInput;
use nexus_types::external_api::views::SledPolicy;
use omicron_uuid_kinds::SledUuid;
use slog::Logger;

#[derive(Debug, Clone)]
pub struct ReconfiguratorCliTestState {
    sim: ReconfiguratorSim,
}

impl ReconfiguratorCliTestState {
    /// Construct a new `reconfigurator-cli` simulation instance, wrapped in a
    /// `ReconfiguratorCliTestState` for use by Rust tests.
    pub fn new(test_name: &str, log: &Logger) -> Self {
        let sim = ReconfiguratorSim::new(
            log.new(slog::o!("component" => "ReconfiguratorSim")),
            Some(test_name.to_string()),
        );
        Self { sim }
    }

    /// Load the default example system.
    pub fn load_example(&mut self) -> anyhow::Result<()> {
        self.load_example_customized(|builder| Ok(builder))
    }

    /// Load an example system.
    ///
    /// `f` provides an opportunity to customize the system.
    pub fn load_example_customized<F>(&mut self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(ExampleSystemBuilder) -> anyhow::Result<ExampleSystemBuilder>,
    {
        if let Some(output) = self.sim.load_example(None, f)? {
            println!("{output}");
        }

        Ok(())
    }

    /// Get the specified blueprint.
    pub fn blueprint(&self, id: BlueprintId) -> Result<&Blueprint, KeyError> {
        let state = self.sim.current_state();
        state.system().resolve_and_get_blueprint(id)
    }

    /// Run the blueprint planner, using the latest blueprint and inventory
    /// collection as input.
    ///
    /// Returns the new blueprint.
    ///
    /// # Errors
    ///
    /// Fails if planning fails. (This is unusual.)
    ///
    /// # Panics
    ///
    /// Panics if planning succeeds but the planner emits a blueprint that has
    /// one or more [`Blippy`] notes. We always expect the planner to emit
    /// "blippy clean" blueprints, and since this is used throughout the
    /// planner's tests, we want to assert that any blueprint we plan is indeed
    /// blippy clean.
    pub fn run_planner(&mut self) -> anyhow::Result<Blueprint> {
        if let Some(output) = cmd_blueprint_plan(
            &mut self.sim,
            BlueprintPlanArgs {
                parent_blueprint_id: BlueprintIdOpt::Latest,
                collection_id: Some(CollectionIdOpt::Latest),
            },
        )? {
            println!("{output}");
        }

        Ok(self.assert_latest_blueprint_is_blippy_clean())
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

    /// Assert that the latest blueprint and current planning input are "blippy
    /// clean" (i.e., have no [`Blippy`] notes).
    ///
    /// Returns the latest blueprint.
    ///
    /// # Panics
    ///
    /// Panics if the latest blueprint and current planning input have any
    /// blippy notes.
    pub fn assert_latest_blueprint_is_blippy_clean(&self) -> Blueprint {
        let blueprint = self
            .blueprint(BlueprintId::Latest)
            .expect("always have a latest blueprint");
        let planning_input = self
            .planning_input(BlueprintId::Latest)
            .expect("always have a latest blueprint");

        let blippy_report = Blippy::new(blueprint, &planning_input)
            .into_report(BlippyReportSortKey::Kind);
        if !blippy_report.notes().is_empty() {
            eprintln!("{}", blueprint.display());
            eprintln!("---");
            eprintln!("{}", blippy_report.display());
            panic!("expected blippy report for blueprint to have no notes");
        }

        blueprint.clone()
    }

    /// Change the internal simulator state.
    pub fn change_state<F, T>(
        &mut self,
        description: &str,
        f: F,
    ) -> anyhow::Result<T>
    where
        F: FnOnce(&mut SimStateBuilder) -> anyhow::Result<T>,
    {
        let mut state = self.sim.current_state().to_mut();
        let ret = f(&mut state)?;
        self.sim.commit_and_bump(description.to_string(), state);
        Ok(ret)
    }

    /// State change helper: generate a new inventory collection from the
    /// current simulator state.
    pub fn generate_inventory(
        &mut self,
        description: &str,
    ) -> anyhow::Result<()> {
        self.change_state(description, |state| {
            let inventory = state.to_collection_builder()?.build();
            state.system_mut().add_collection(inventory)?;
            Ok(())
        })
    }

    /// State change helper: edit the latest blueprint, inserting a new latest
    /// blueprint.
    pub fn blueprint_edit_latest<F>(
        &mut self,
        description: &str,
        f: F,
    ) -> anyhow::Result<Arc<Blueprint>>
    where
        F: FnOnce(&mut BlueprintBuilder<'_>) -> anyhow::Result<()>,
    {
        let log = self.sim.log.clone();
        self.change_state(description, |state| {
            let rng = state.rng_mut().next_planner_rng();
            let system = state.system_mut();
            let blueprint = system
                .get_blueprint(
                    &system.resolve_blueprint_id(BlueprintId::Latest),
                )
                .expect("always have a latest blueprint");
            let mut builder = BlueprintBuilder::new_based_on(
                &log,
                blueprint,
                "ReconfiguratorCliTestState",
                rng,
            )?;
            f(&mut builder)?;
            let blueprint = Arc::new(builder.build(BlueprintSource::Test));
            system.add_blueprint(Arc::clone(&blueprint))?;
            Ok(blueprint)
        })
    }

    /// State change helper: create a new latest blueprint that is a clone of
    /// the current latest blueprint (but with a its ID and parent ID updated),
    /// then pass it to `f` which is allowed to edit it in abnormal ways that
    /// cannot be done via `BlueprintBuilder`.
    pub fn blueprint_edit_latest_low_level<F>(
        &mut self,
        description: &str,
        f: F,
    ) -> anyhow::Result<Arc<Blueprint>>
    where
        F: FnOnce(&mut Blueprint) -> anyhow::Result<()>,
    {
        self.change_state(description, |state| {
            let mut rng = state.rng_mut().next_planner_rng();
            let system = state.system_mut();
            let parent_blueprint = system
                .get_blueprint(
                    &system.resolve_blueprint_id(BlueprintId::Latest),
                )
                .expect("always have a latest blueprint");
            let mut blueprint = parent_blueprint.clone();

            // Update metadata fields to make this a new blueprint.
            blueprint.id = rng.next_blueprint();
            blueprint.parent_blueprint_id = Some(parent_blueprint.id);
            blueprint.time_created = now_db_precision();

            // Perform whatever modifications the caller wants.
            f(&mut blueprint)?;

            let blueprint = Arc::new(blueprint);
            system.add_blueprint(Arc::clone(&blueprint))?;

            Ok(blueprint)
        })
    }

    /// State change helper: add a new sled, returning its ID.
    pub fn sled_add(&mut self, description: &str) -> anyhow::Result<SledUuid> {
        self.sled_add_customized(description, |sled| sled)
    }

    /// State change helper: add a new sled, returning its ID.
    ///
    /// `f` provides an opportunity to customize the sled.
    pub fn sled_add_customized<F>(
        &mut self,
        description: &str,
        f: F,
    ) -> anyhow::Result<SledUuid>
    where
        F: FnOnce(SledBuilder) -> SledBuilder,
    {
        self.change_state(description, |state| {
            let sled_id = state.rng_mut().next_sled_id();
            let new_sled = f(SledBuilder::new().id(sled_id));
            state.system_mut().description_mut().sled(new_sled)?;
            Ok(sled_id)
        })
    }

    /// State change helper: expunge a sled.
    pub fn sled_expunge(
        &mut self,
        description: &str,
        sled_id: SledUuid,
    ) -> anyhow::Result<()> {
        self.change_state(description, |state| {
            state
                .system_mut()
                .description_mut()
                .sled_set_policy(sled_id, SledPolicy::Expunged)?;
            Ok(())
        })
    }

    /// State change helper: for each active sled described by `blueprint`,
    /// update the simulate sled's config to match.
    ///
    /// This emulates blueprint execution sending new configs to all sleds.
    pub fn deploy_configs_to_active_sleds(
        &mut self,
        description: &str,
        blueprint: &Blueprint,
    ) -> anyhow::Result<()> {
        self.change_state(description, |state| {
            let description = state.system_mut().description_mut();
            for (sled_id, config) in blueprint.active_sled_configs() {
                let config = config.clone().into_in_service_sled_config();
                description.sled_set_omicron_config(sled_id, config)?;
            }
            Ok(())
        })
    }
}
