// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    instance_common::allocate_vmm_ipv6, NexusActionContext, NexusSaga,
    SagaInitError, ACTION_GENERATE_ID,
};
use crate::app::instance::InstanceStateChangeError;
use crate::app::sagas::declare_saga_actions;
use chrono::Utc;
use nexus_db_model::Generation;
use nexus_db_queries::db::{
    datastore::InstanceAndVmms, identity::Resource, lookup::LookupPath,
};
use nexus_db_queries::{authn, authz, db};
use omicron_common::api::external::{Error, InstanceState};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Parameters to the instance update saga.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub authz_instance: authz::Instance,

    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
}

declare_saga_actions! {
    instance_update;

    // Read the target Instance from CRDB and join with its active VMM and
    // migration target VMM records if they exist, and then acquire the
    // "instance updater" lock with this saga's ID if no other saga is currently
    // updating the instance.
    LOOKUP_AND_LOCK_INSTANCE -> "instance_and_vmms" {
        + siu_lookup_and_lock_instance
        - siu_lookup_and_lock_instance_undo
    }

    //
}

const SAGA_INSTANCE_LOCK_ID: &str = "saga_instance_lock_id";

async fn siu_lookup_and_lock_instance(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let Params { ref authz_instance, ref serialized_authn, .. } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let lookup = osagactx
        .datastore()
        .instance_fetch_with_vmms(&opctx, authz_instance)
        .await?;

    // try to lock

    let lock_id = sagactx.lookup::<Uuid>(SAGA_INSTANCE_LOCK_ID)?;
    let lock = osagactx
        .datastore()
        .instance_updater_try_lock(
            &opctx,
            &authz_instance,
            &lookup.instance.runtime_state.updater_gen,
            &lock_id,
        )
        .await?;

    Ok(lookup)
}

async fn siu_lookup_and_lock_instance_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let Params { ref authz_instance, ref serialized_authn, .. } =
        sagactx.saga_params::<Params>()?;
    let lock_id = sagactx.lookup::<Uuid>(SAGA_INSTANCE_LOCK_ID)?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    osagactx.datastore().instance_updater_unlock(
        &opctx,
        &authz_instance,
        &lock_id,
    )?;
    Ok(())
}
