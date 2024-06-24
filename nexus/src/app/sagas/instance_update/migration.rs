// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::db::model::Generation;
use crate::app::db::model::Instance;
use crate::app::db::model::Migration;
use crate::app::db::model::MigrationState;
use crate::app::sagas::declare_saga_actions;
use chrono::Utc;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::{authn, authz};
use omicron_common::api::external::Error;
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

    // Update the instance record to reflect the migration event. If the
    // migration has completed on the target VMM, or if the migration has
    // failed, this will clear the migration IDs, allowing the instance to
    // migrate again. If the migration has completed on either VMM, the target
    // VMM becomes the active VMM.
    UPDATE_INSTANCE_RECORD -> "update_instance_record" {
        + sium_update_instance_record
    }

    // Update network configuration to point to the new active VMM.
    UPDATE_NETWORK_CONFIG -> "update_network_config" {
        + sium_update_network_config
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
        _: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(update_instance_record_action());
        builder.append(update_network_config_action());

        Ok(builder.build()?)
    }
}

async fn sium_update_instance_record(
    sagactx: NexusActionContext,
) -> Result<PropolisUuid, ActionError> {
    let Params { ref authz_instance, ref migration, ref instance, .. } =
        sagactx.saga_params()?;

    let osagactx = sagactx.user_data();
    let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());

    let mut new_runtime = instance.runtime().clone();
    new_runtime.gen = Generation(new_runtime.gen.next());
    new_runtime.time_updated = Utc::now();

    // Determine how to update the instance record to reflect the current
    // migration state.
    let failed = migration.either_side_failed();
    // If the migration has failed, or if the target reports that the migration
    // has completed, clear the instance record's migration IDs so that a new
    // migration can begin.
    if failed || migration.target_state == MigrationState::COMPLETED {
        info!(
            osagactx.log(),
            "instance update (migration {}): clearing migration IDs",
            if failed { "failed" } else { "target_completed" };
            "instance_id" => %instance_id,
            "migration_id" => %migration.id,
            "src_propolis_id" => %migration.source_propolis_id,
            "target_propolis_id" => %migration.target_propolis_id,
            "instance_update" => %"migration",
        );
        new_runtime.migration_id = None;
        new_runtime.dst_propolis_id = None;
    }

    // If either side reports that the migration has completed, move the target
    // Propolis ID to the active position.
    let new_propolis_id = if !failed && migration.either_side_completed() {
        info!(
            osagactx.log(),
            "instance update (migration completed): setting active VMM ID to target";
            "instance_id" => %instance_id,
            "migration_id" => %migration.id,
            "src_propolis_id" => %migration.source_propolis_id,
            "target_propolis_id" => %migration.target_propolis_id,
            "instance_update" => %"migration",
        );
        new_runtime.propolis_id = Some(migration.target_propolis_id);
        migration.target_propolis_id
    } else {
        migration.source_propolis_id
    };

    osagactx
        .datastore()
        .instance_update_runtime(&instance_id, &new_runtime)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(PropolisUuid::from_untyped_uuid(new_propolis_id))
}

async fn sium_update_network_config(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Params { ref serialized_authn, ref authz_instance, migration, .. } =
        sagactx.saga_params()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let osagactx = sagactx.user_data();
    let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());

    // Either the instance moved from one sled to another, or it attempted
    // to migrate and failed. Ensure the correct networking configuration
    // exists for its current home.
    //
    // TODO(#3107) This is necessary even if the instance didn't move,
    // because registering a migration target on a sled creates OPTE ports
    // for its VNICs, and that creates new V2P mappings on that sled that
    // place the relevant virtual IPs on the local sled. Once OPTE stops
    // creating these mappings, this path only needs to be taken if an
    // instance has changed sleds.

    // Look up the ID of the sled that the instance now resides on, so that we
    // can look up its address.
    let active_propolis_id =
        sagactx.lookup::<PropolisUuid>("update_instance_record")?;
    let new_sled_id = match osagactx
        .datastore()
        .vmm_fetch(&opctx, authz_instance, &active_propolis_id)
        .await
    {
        Ok(vmm) => vmm.sled_id,

        // A VMM in the active position should never be destroyed. If the
        // sled sending this message is the owner of the instance's last
        // active VMM and is destroying it, it should also have retired that
        // VMM.
        Err(Error::ObjectNotFound { .. }) => {
            error!(osagactx.log(), "instance's active vmm unexpectedly not found";
                    "instance_id" => %instance_id,
                    "propolis_id" => %active_propolis_id);

            return Ok(());
        }
        Err(e) => return Err(ActionError::action_failed(e)),
    };

    info!(
        osagactx.log(),
        "instance update (migration): ensuring updated instance network config";
        "instance_id" => %instance_id,
        "migration_id" => %migration.id,
        "src_propolis_id" => %migration.source_propolis_id,
        "target_propolis_id" => %migration.target_propolis_id,
        "active_propolis_id" => %active_propolis_id,
        "sled_id" => %new_sled_id,
        "migration_failed" => migration.either_side_failed(),
    );

    if let Err(e) = osagactx.nexus().v2p_notification_tx.send(()) {
        error!(
            osagactx.log(),
            "error notifying background task of v2p change";
            "error" => ?e
        )
    };

    let (.., sled) = LookupPath::new(&opctx, osagactx.datastore())
        .sled_id(new_sled_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_ensure_dpd_config(&opctx, instance_id, &sled.address(), None)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}
