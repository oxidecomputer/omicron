// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::sagas::declare_saga_actions;
use crate::db::model::Instance;
use nexus_db_queries::db::datastore::Disk;
use nexus_db_queries::{authn, authz};
use nexus_types::saga::saga_action_failed;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{GenericUuid, PropolisUuid};
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

declare_saga_actions! {
    instance_disk_attach;

    CREATE_ATTACH_DISK_RECORD -> "instance_disk" {
        + sida_begin_attach_disk
        - sida_begin_attach_disk_undo
    }

    ATTACH_DISK -> "disk" {
        + sida_attach_disk
        - sida_attach_disk_undo
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub authz_instance: authz::Instance,
    pub authz_disk: authz::Disk,
}

async fn sida_begin_attach_disk(
    sagactx: NexusActionContext,
) -> Result<(Instance, Disk), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    warn!(log, "AAAAAAAA");

    let result = datastore
        .instance_attach_disk(
            &opctx,
            &params.authz_instance,
            &params.authz_disk,
            12,
        )
        .await
        .map_err(saga_action_failed)?;

    Ok(result)
}

async fn sida_begin_attach_disk_undo(
    _sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    Ok(())
}

async fn sida_attach_disk(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let inst_and_vmm = datastore
        .instance_fetch_with_vmm(&opctx, &params.authz_instance)
        .await
        .map_err(saga_action_failed)?;

    let (vmm_id, sled_id) = match inst_and_vmm
        .vmm()
        .as_ref()
        .map(|vmm| (PropolisUuid::from_untyped_uuid(vmm.id), vmm.sled_id()))
    {
        Some(a) => a,
        None => {
            return Err(saga_action_failed(Error::invalid_request(
                "AAAAAAAAAAAAAAAAAAA",
            )));
        }
    };

    let (_, disk) = sagactx.lookup::<(Instance, Disk)>("instance_disk")?;

    osagactx
        .nexus()
        .request_disk_attach(&opctx, &vmm_id, &sled_id, disk.id())
        .await
        .map_err(saga_action_failed)?;
    Ok(())
}

async fn sida_attach_disk_undo(
    _sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    Ok(())
}

#[derive(Debug)]
pub struct SagaInstanceDiskAttach;
impl NexusSaga for SagaInstanceDiskAttach {
    const NAME: &'static str = "disk-attach";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        instance_disk_attach_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(create_attach_disk_record_action());
        builder.append(attach_disk_action());
        Ok(builder.build()?)
    }
}
