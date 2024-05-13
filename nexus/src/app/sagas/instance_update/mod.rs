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

pub mod destroyed;

/// Parameters to the instance update saga.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub authz_instance: authz::Instance,

    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,

    pub start_state: InstanceAndVmms,
}

const SAGA_INSTANCE_LOCK_ID: &str = "saga_instance_lock_id";

async fn siu_lock_instance(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let Params {
        ref authz_instance,
        ref serialized_authn,
        ref start_state,
        ..
    } = sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    // try to lock

    let lock_id = sagactx.lookup::<Uuid>(SAGA_INSTANCE_LOCK_ID)?;
    let lock = osagactx
        .datastore()
        .instance_updater_try_lock(
            &opctx,
            &authz_instance,
            start_state.instance.runtime_state.updater_gen,
            &lock_id,
        )
        .await?;

    Ok(())
}

async fn siu_unlock_instance(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let Params { ref authz_instance, ref serialized_authn, .. } =
        sagactx.saga_params::<Params>()?;
    let lock_id = sagactx.lookup::<Uuid>(SAGA_INSTANCE_LOCK_ID)?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    osagactx
        .datastore()
        .instance_updater_unlock(&opctx, &authz_instance, &lock_id)
        .await?;
    Ok(())
}
