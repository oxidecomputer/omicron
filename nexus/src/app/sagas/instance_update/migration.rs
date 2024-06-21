// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::db::model::Instance;
use crate::app::db::model::Migration;
use crate::app::sagas::declare_saga_actions;
use nexus_db_queries::{authn, authz};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use serde::{Deserialize, Serialize};
use steno::ActionError;

// instance update (migration) subsaga: actions

// This subsaga is responsible for handling an instance update where either side
// of an active migration reports that the migration has failed or completed.
declare_saga_actions! {
    instance_update_migration;

    // Clear migration IDs and write back the instance
    CLEAR_MIGRATION_IDS -> "clear_migration_ids" {
        + sium_clear_migration_ids
    }
}

/// Parameters to the instance update (migration) sub-saga.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct Params {
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub(super) serialized_authn: authn::saga::Serialized,

    pub(super) authz_instance: authz::Instance,

    pub(super) instance: Instance,

    pub(super) migration: Migration,
}

#[derive(Debug)]
pub(super) struct SagaMigrationUpdate;
impl NexusSaga for SagaMigrationUpdate {
    const NAME: &'static str = "instance-update-migration";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        instance_update_migration_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        todo!("eliza: draw the rest of the saga...")
    }
}

async fn sium_clear_migration_ids(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Params {
        ref serialized_authn, ref authz_instance, ref migration, ..
    } = sagactx.saga_params()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let osagactx = sagactx.user_data();
    let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());

    let src_propolis_id =
        PropolisUuid::from_untyped_uuid(migration.source_propolis_id);
    let target_propolis_id =
        PropolisUuid::from_untyped_uuid(migration.target_propolis_id);

    slog::info!(
        osagactx.log(),
        "instance update (migration): clearing migration IDs";
        "instance_id" => %instance_id,
        "migration_id" => %migration.id,
        "src_propolis_id" => %src_propolis_id,
        "target_propolis_id" => %target_propolis_id,
        "migration_failed" => migration.either_side_failed(),
    );

    osagactx
        .datastore()
        .instance_unset_migration_ids(
            &opctx,
            instance_id,
            migration.id,
            target_propolis_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}
