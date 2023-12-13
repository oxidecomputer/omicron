// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    common_storage::{
        call_pantry_attach_for_disk, call_pantry_detach_for_disk,
        delete_crucible_regions, ensure_all_datasets_and_regions,
        get_pantry_address,
    },
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz, db};
use crate::external_api::params;
use nexus_db_queries::db::identity::{Asset, Resource};
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::Error;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::{CrucibleOpts, VolumeConstructionRequest};
use std::convert::TryFrom;
use std::net::SocketAddrV6;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// rough sequence of evts:
// - take temp ownership of instance while interacting w/ sled agent
//  -> mark instance migration id as Some(0) if None
// - Detach EIP from instance, hang onto its ID.
// - Withdraw routes
//  -> ensure_dpd... (?) Do we actually need to?
//  -> must precede OPTE: host may change its sending
//     behaviour prematurely
// - Deregister addr in OPTE
//  -> Put addr in sled-agent endpoint
// - Delete EIP iff. Ephemeral
//   -> why so late? Risk that we can't recover our IP in an unwind.
// - free up migration_id of instance.
//  -> mark instance migration id as None

declare_saga_actions! {
    instance_ip_detach;
    STAGE1 -> "result1" {
        + do_fn
        - undo_fn
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub authz_instance: authz::Instance,
    pub instance: db::model::Instance,
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
}

#[derive(Debug)]
pub struct SagaInstanceIpDetach;
impl NexusSaga for SagaInstanceIpDetach {
    const NAME: &'static str = "external-ip-detach";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        instance_ip_detach_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(stage1_action());
        Ok(builder.build()?)
    }
}

async fn do_fn(sagactx: NexusActionContext) -> Result<(), ActionError> {
    todo!()
}

async fn undo_fn(sagactx: NexusActionContext) -> Result<(), ActionError> {
    todo!()
}

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        app::saga::create_saga_dag, app::sagas::disk_create::Params,
        app::sagas::disk_create::SagaDiskCreate, external_api::params,
    };
    use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
    use diesel::{
        ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::{authn::saga::Serialized, db::datastore::DataStore};
    use nexus_test_utils::resource_helpers::create_ip_pool;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Name;
    use omicron_sled_agent::sim::SledAgent;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }
}
