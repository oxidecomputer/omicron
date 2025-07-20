// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration of the deployment system

use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintTargetSet;
use nexus_types::deployment::PlannerChickenSwitches;
use nexus_types::deployment::PlanningInput;
use nexus_types::internal_api::views::UpdateStatus;
use nexus_types::inventory::Collection;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use slog_error_chain::InlineErrorChain;
use uuid::Uuid;

/// Common structure for collecting information that the planner needs
struct PlanningContext {
    planning_input: PlanningInput,
    creator: String,
    inventory: Option<Collection>,
}

impl super::Nexus {
    pub async fn blueprint_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<BlueprintMetadata> {
        self.db_datastore.blueprints_list(opctx, pagparams).await
    }

    pub async fn blueprint_view(
        &self,
        opctx: &OpContext,
        blueprint_id: Uuid,
    ) -> LookupResult<Blueprint> {
        let blueprint = authz::Blueprint::new(
            authz::FLEET,
            blueprint_id,
            LookupType::ById(blueprint_id),
        );
        self.db_datastore.blueprint_read(opctx, &blueprint).await
    }

    pub async fn blueprint_delete(
        &self,
        opctx: &OpContext,
        blueprint_id: Uuid,
    ) -> DeleteResult {
        let blueprint = authz::Blueprint::new(
            authz::FLEET,
            blueprint_id,
            LookupType::ById(blueprint_id),
        );
        self.db_datastore.blueprint_delete(opctx, &blueprint).await
    }

    pub async fn blueprint_target_view(
        &self,
        opctx: &OpContext,
    ) -> Result<BlueprintTarget, Error> {
        self.db_datastore.blueprint_target_get_current(opctx).await
    }

    pub async fn blueprint_target_set(
        &self,
        opctx: &OpContext,
        params: BlueprintTargetSet,
    ) -> Result<BlueprintTarget, Error> {
        let new_target = BlueprintTarget {
            target_id: params.target_id,
            enabled: params.enabled,
            time_made_target: chrono::Utc::now(),
        };

        self.db_datastore
            .blueprint_target_set_current(opctx, new_target)
            .await?;

        // We have a new target: trigger the background task to load this
        // blueprint.
        self.background_tasks
            .activate(&self.background_tasks.task_blueprint_loader);

        Ok(new_target)
    }

    pub async fn blueprint_target_set_enabled(
        &self,
        opctx: &OpContext,
        params: BlueprintTargetSet,
    ) -> Result<BlueprintTarget, Error> {
        let new_target = BlueprintTarget {
            target_id: params.target_id,
            enabled: params.enabled,
            time_made_target: chrono::Utc::now(),
        };

        self.db_datastore
            .blueprint_target_set_current_enabled(opctx, new_target)
            .await?;

        // We don't know whether this actually changed the enabled bit; activate
        // the background task to load this blueprint which does know.
        self.background_tasks
            .activate(&self.background_tasks.task_blueprint_loader);

        Ok(new_target)
    }

    async fn blueprint_planning_context(
        &self,
        opctx: &OpContext,
    ) -> Result<PlanningContext, Error> {
        let creator = self.id.to_string();
        let datastore = self.datastore();
        // Load up the chicken switches from the db directly (rather than from,
        // say, the background task) to ensure we get the latest state.
        let chicken_switches = self
            .db_datastore
            .reconfigurator_chicken_switches_get_latest(opctx)
            .await?
            .map_or_else(PlannerChickenSwitches::default, |switches| {
                switches.planner_switches
            });

        let planning_input =
            PlanningInputFromDb::assemble(opctx, datastore, chicken_switches)
                .await?;

        // The choice of which inventory collection to use here is not
        // necessarily trivial.  Inventory collections may be incomplete due to
        // transient (or even persistent) errors.  It's not yet clear what
        // general criteria we'll want to use in picking a collection here.  But
        // as of this writing, this is only used for one specific case, which is
        // to implement a gate that prevents the planner from provisioning
        // non-NTP zones on a sled unless we know there's an NTP zone already on
        // that sled.  For that purpose, it's okay if this collection is
        // incomplete due to a transient error -- that would just prevent
        // forward progress in the planner until the next time we try this.
        // (Critically, it won't cause the planner to do anything wrong.)
        let inventory = datastore
            .inventory_get_latest_collection(opctx)
            .await
            .internal_context(
                "fetching latest inventory collection for blueprint planner",
            )?;

        Ok(PlanningContext { planning_input, creator, inventory })
    }

    async fn blueprint_add(
        &self,
        opctx: &OpContext,
        blueprint: &Blueprint,
    ) -> Result<(), Error> {
        self.db_datastore.blueprint_insert(opctx, blueprint).await
    }

    pub async fn blueprint_create_regenerate(
        &self,
        opctx: &OpContext,
    ) -> CreateResult<Blueprint> {
        let (_, parent_blueprint) =
            self.db_datastore.blueprint_target_get_current_full(opctx).await?;

        let planning_context = self.blueprint_planning_context(opctx).await?;
        let inventory = planning_context.inventory.ok_or_else(|| {
            Error::internal_error("no recent inventory collection found")
        })?;
        let planner = Planner::new_based_on(
            opctx.log.clone(),
            &parent_blueprint,
            &planning_context.planning_input,
            &planning_context.creator,
            &inventory,
        )
        .map_err(|error| {
            Error::internal_error(&format!(
                "error creating blueprint planner: {error:#}",
            ))
        })?;
        let blueprint = planner.plan().map_err(|error| {
            Error::internal_error(&format!(
                "error generating blueprint: {}",
                InlineErrorChain::new(&error)
            ))
        })?;

        self.blueprint_add(&opctx, &blueprint).await?;
        Ok(blueprint)
    }

    pub async fn blueprint_import(
        &self,
        opctx: &OpContext,
        blueprint: Blueprint,
    ) -> Result<(), Error> {
        let _ = self.blueprint_add(&opctx, &blueprint).await?;
        Ok(())
    }

    pub async fn update_status(
        &self,
        opctx: &OpContext,
    ) -> Result<UpdateStatus, Error> {
        let planning_context = self.blueprint_planning_context(opctx).await?;
        let inventory = planning_context.inventory.ok_or_else(|| {
            Error::internal_error("no recent inventory collection found")
        })?;
        let new = planning_context.planning_input.tuf_repo().description();
        let old = planning_context.planning_input.old_repo().description();
        let status = UpdateStatus::new(old, new, &inventory);

        Ok(status)
    }
}
