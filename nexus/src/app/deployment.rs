// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration of the deployment system

use nexus_db_queries::context::OpContext;
use nexus_deployment::blueprint_builder::BlueprintBuilder;
use nexus_deployment::blueprint_builder::SledInfo;
use nexus_deployment::planner::Planner;
use nexus_types::deployment::params;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::inventory::Collection;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use uuid::Uuid;

// XXX-dap temporary in-memory store of blueprints, for testing.  This will move
// to the database.
pub struct Blueprints {
    all_blueprints: BTreeMap<Uuid, Blueprint>,
    target: BlueprintTarget,
}

impl Blueprints {
    pub fn new() -> Blueprints {
        Blueprints {
            all_blueprints: BTreeMap::new(),
            target: BlueprintTarget {
                target_id: None,
                enabled: false,
                time_set: chrono::Utc::now(),
            },
        }
    }
}

impl super::Nexus {
    pub async fn blueprint_list(
        &self,
        _opctx: &OpContext,
        _pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Blueprint> {
        // XXX-dap authz check
        // Since this is just temporary until we have a database impl, ignore
        // pagination.
        Ok(self
            .blueprints
            .lock()
            .unwrap()
            .all_blueprints
            .values()
            .cloned()
            .collect())
    }

    pub async fn blueprint_view(
        &self,
        _opctx: &OpContext,
        blueprint_id: Uuid,
    ) -> LookupResult<Blueprint> {
        // XXX-dap replace with an authz resource + lookup resource etc.
        self.blueprints
            .lock()
            .unwrap()
            .all_blueprints
            .get(&blueprint_id)
            .cloned()
            .ok_or_else(|| {
                Error::not_found_by_id(ResourceType::Blueprint, &blueprint_id)
            })
    }

    pub async fn blueprint_delete(
        &self,
        _opctx: &OpContext,
        blueprint_id: Uuid,
    ) -> DeleteResult {
        // XXX-dap authz check
        let mut blueprints = self.blueprints.lock().unwrap();
        if let Some(target_id) = blueprints.target.target_id {
            if target_id == blueprint_id {
                // XXX-dap
                return Err(Error::conflict(format!(
                    "blueprint {} is the current target and cannot be deleted",
                    blueprint_id
                )));
            }
        }

        if blueprints.all_blueprints.remove(&blueprint_id).is_none() {
            return Err(Error::not_found_by_id(
                ResourceType::Blueprint,
                &blueprint_id,
            ));
        }

        Ok(())
    }

    pub async fn blueprint_target_view(
        &self,
        _opctx: &OpContext,
    ) -> Result<BlueprintTarget, Error> {
        // XXX-dap authz check
        let blueprints = self.blueprints.lock().unwrap();
        Ok(blueprints.target.clone())
    }

    pub async fn blueprint_target_set(
        &self,
        _opctx: &OpContext,
        params: params::BlueprintTarget,
    ) -> Result<BlueprintTarget, Error> {
        // XXX-dap authz check
        let new_target_id = params.target_id;
        let enabled = params.enabled;
        let mut blueprints = self.blueprints.lock().unwrap();
        if let Some(blueprint) = blueprints.all_blueprints.get(&new_target_id) {
            if blueprint.parent_blueprint_id != blueprints.target.target_id {
                return Err(Error::conflict(&format!(
                    "blueprint {:?}: parent is {:?}, which is not the current \
                    target {:?}",
                    new_target_id,
                    blueprint
                        .parent_blueprint_id
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| String::from("<none>")),
                    blueprints
                        .target
                        .target_id
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| String::from("<none>")),
                )));
            }
            blueprints.target = BlueprintTarget {
                target_id: Some(new_target_id),
                enabled,
                time_set: chrono::Utc::now(),
            };
            // XXX-dap this is the point where we'd poke a background task
            Ok(blueprints.target.clone())
        } else {
            Err(Error::not_found_by_id(ResourceType::Blueprint, &new_target_id))
        }
    }

    async fn blueprint_planning_context(
        &self,
        opctx: &OpContext,
    ) -> Result<PlanningContext, Error> {
        todo!(); // XXX-dap this is where we fetch collection, sleds, etc.
    }

    async fn blueprint_add(
        &self,
        opctx: &OpContext,
        blueprint: Blueprint,
    ) -> Result<(), Error> {
        // XXX-dap authz check
        let mut blueprints = self.blueprints.lock().unwrap();
        assert!(blueprints
            .all_blueprints
            .insert(blueprint.id, blueprint)
            .is_none());
        Ok(())
    }

    pub async fn blueprint_create_current(
        &self,
        opctx: &OpContext,
    ) -> CreateResult<Blueprint> {
        let planning_context = self.blueprint_planning_context(opctx).await?;
        let blueprint = BlueprintBuilder::build_initial_from_collection(
            &planning_context.collection,
            &planning_context.sleds,
            &planning_context.creator,
            "initial blueprint",
        )
        .map_err(|error| {
            Error::internal_error(&format!(
                "error generating initial blueprint from current state: {}",
                InlineErrorChain::new(&error)
            ))
        })?;

        self.blueprint_add(&opctx, blueprint.clone()).await?;
        Ok(blueprint)
    }

    pub async fn blueprint_create_regenerate(
        &self,
        opctx: &OpContext,
    ) -> CreateResult<Blueprint> {
        let parent_blueprint = {
            let blueprints = self.blueprints.lock().unwrap();
            let Some(target_id) = blueprints.target.target_id else {
                return Err(Error::conflict(&format!(
                    "cannot add sled before initial blueprint is created"
                )));
            };
            blueprints
                .all_blueprints
                .get(&target_id)
                .cloned()
                .expect("expected target_id to correspond to a blueprint")
        };
        let planning_context = self.blueprint_planning_context(opctx).await?;
        let planner = Planner::new_based_on(
            opctx.log.clone(),
            &parent_blueprint,
            &planning_context.collection,
            &planning_context.sleds,
            &planning_context.creator,
            // XXX-dap this "reason" was intended for the case where we know why
            // we're doing this.  Right now such a case doesn't exist.
            "on-demand regenerate",
        );
        let blueprint = planner.plan().map_err(|error| {
            Error::internal_error(&format!(
                "error generating blueprint: {}",
                InlineErrorChain::new(&error)
            ))
        })?;

        self.blueprint_add(&opctx, blueprint.clone()).await?;
        Ok(blueprint)
    }
}

struct PlanningContext {
    collection: Collection,
    sleds: BTreeMap<Uuid, SledInfo>,
    creator: String,
}
